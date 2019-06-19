######################################################################################################################
#  Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance        #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://aws.amazon.com/asl/                                                                                    #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

import os
from datetime import datetime, timedelta

import boto3
import dateutil.parser

import configuration
import handlers.task_tracking_table
import pytz
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from configuration.task_configuration import TaskConfiguration
from main import lambda_handler
from scheduling.cron_expression import CronExpression
from util import safe_dict, safe_json
from util.logger import Logger

NAME_ATTR = "Name"

LAST_SCHEDULER_RUN_KEY = "last-scheduler-run"

INFO_CONFIG_RUN = "Running scheduler for configuration update of task \"{}\""
INFO_CURRENT_SCHEDULING_DT = "Current datetime used for scheduling is {}"
INFO_LAST_SAVED = "Last saved scheduler execution was at {}"
INFO_NO_TASKS_STARTED = "Number of enabled tasks in configuration is {}, no tasks were started"
INFO_RESULT = "Handling cloudwatch event took {:>.3f} seconds"
INFO_SCHEDULED_TASK = "Scheduling task \"{}\" for time {} in timezone {}\nTask definition is {}"
INFO_STARTED_TASKS = "Number of enabled tasks is {}, started tasks {}"
INFO_NO_NEXT_WITHIN = "No executions for task {} scheduled within the next 24 hours"
INFO_TASK_SCHEDULER_ALREADY_RAN = "Scheduler already executed for this minute"
INFO_NEXT_EXECUTION = "Next execution for task \"{}\" within the next 24 hours will be at {} ({})"
INFO_NEXT_EXECUTED_TASK = "The first task that wil be executed within 24 hours is \"{}\" at {}"
INFO_NEXT_ONE_MINUTE = "Next schedule event will be in one minute"
INFO_NEXT_EVENT = "Next schedule event will be at {}"
INFO_NO_TASKS_SCHEDULED = "There are no tasks scheduled within the next 24 hours"
INFO_LAMBDA = "Invoked lambda function, started_tasks is {}, payload is {}"


LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class ScheduleHandler:
    """
    Class that handles time based events from CloudWatch rules
    """

    def __init__(self, event, context):
        """
        Initializes the instance.
        :param event: event to handle
        :param context: CLambda context
        """
        self._context = context
        self._event = event
        self._table = None

        # Setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = Logger(logstream=logstream, buffersize=20, context=context)
        self.configuration_update = ScheduleHandler.is_config_update(self._event)
        if self.configuration_update:
            if "OldImage" in self._event["Records"][0]["dynamodb"]:
                self.updated_task = self._event["Records"][0]["dynamodb"]["OldImage"][configuration.CONFIG_TASK_NAME]["S"]
            else:
                self.updated_task = self._event["Records"][0]["dynamodb"]["NewImage"][configuration.CONFIG_TASK_NAME]["S"]

    @staticmethod
    def is_handling_request(event):
        """
        Tests if event is handled by instance of this handler.
        :param event: Tested event
        :return: True if the event is a cloudwatch rule event for scheduling or configuration update
        """
        source = event.get(handlers.HANDLER_EVENT_SOURCE, "")

        if source == "aws.events":
            resources = event.get("resources", [])
            if len(resources) == 1 and resources[0].partition("/")[2].startswith(
                            os.getenv(handlers.ENV_STACK_NAME) + "-SchedulerRule-"):
                return True
            return False

        return ScheduleHandler.is_config_update(event)

    @staticmethod
    def is_config_update(event):
        if "Records" not in event:
            return False

        source_arn = event["Records"][0]["eventSourceARN"]
        table_name = source_arn.split("/")[1]
        return table_name == os.getenv(configuration.ENV_CONFIG_TABLE)

    @property
    def _last_run_table(self):
        """
        Returns table to store last execution time for this handler.
        :return: table to store last execution time for this handler
        """
        if self._table is None:
            self._table = boto3.resource('dynamodb').Table(os.environ[handlers.ENV_LAST_RUN_TABLE])
            add_retry_methods_to_resource(self._table, ["get_item", "update_item"])
        return self._table

    def _get_last_run(self):
        """
        Returns the last UTC datetime this ops automator handler was executed.
        :return: Last datetime this handler was executed in timezone UTC
        """
        # get from table
        resp = self._last_run_table.get_item_with_retries(
            Key={
                NAME_ATTR: LAST_SCHEDULER_RUN_KEY
            }, ConsistentRead=True)

        # test if item was in table
        if "Item" in resp:
            return dateutil.parser.parse(resp["Item"]["value"])
        else:
            # default for first call is current datetime minus one minute
            return datetime.now(tz=pytz.timezone("UCT")).replace(second=0, microsecond=0) - timedelta(minutes=1)

    def _set_last_run(self):
        """
        Stores and returns the current datetime in UTC as the last execution time of this handler.
        :return: Stored last execution time in UTC timezone
        """
        dt = datetime.now(tz=pytz.timezone("UCT")).replace(second=0, microsecond=0)
        self._last_run_table.update_item(
            Key={NAME_ATTR: LAST_SCHEDULER_RUN_KEY},
            AttributeUpdates={
                "value": {"Action": "PUT", "Value": dt.isoformat()}
            })
        return dt

    def handle_request(self):
        """
        Handles the cloudwatch rule timer event
        :return: Started tasks, if any, information
        """

        try:
            started_tasks = []

            start = datetime.now()

            last_run_dt = self._get_last_run()
            self._logger.info("Handler {}", self.__class__.__name__)
            self._logger.info(INFO_LAST_SAVED, str(last_run_dt))

            if self.configuration_update:
                self._logger.info(INFO_CONFIG_RUN, self.updated_task)

            # test if we already executed in this minute
            current_dt = self._set_last_run()
            already_ran_this_minute = last_run_dt == current_dt

            if already_ran_this_minute and not self.configuration_update:
                self._logger.info(INFO_TASK_SCHEDULER_ALREADY_RAN)
            else:

                self._logger.info(INFO_CURRENT_SCHEDULING_DT, current_dt)

                task = None
                enabled_tasks = 0

                next_executed_task = None
                utc = pytz.timezone("UTC")

                try:
                    for task in [t for t in TaskConfiguration(context=self._context, logger=self._logger).get_tasks() if
                                 t.get(handlers.TASK_INTERVAL) is not None
                                 and t.get(handlers.TASK_ENABLED, True)]:

                        enabled_tasks += 1

                        self._logger.debug_enabled = task[handlers.TASK_DEBUG]

                        task_name = task[handlers.TASK_NAME]

                        # timezone for specific task
                        task_timezone = pytz.timezone(task[handlers.TASK_TIMEZONE])

                        # create cron expression to test if task needs te be executed
                        task_cron_expression = CronExpression(expression=task[handlers.TASK_INTERVAL])

                        localized_last_run = last_run_dt.astimezone(task_timezone)
                        localized_current_dt = current_dt.astimezone(task_timezone)

                        next_execution = task_cron_expression.first_within_next(timedelta(hours=24), localized_current_dt)
                        next_execution_utc = next_execution.astimezone(utc) if next_execution else None

                        if next_execution_utc is not None:
                            if next_executed_task is None or next_execution_utc < next_executed_task[0]:
                                next_executed_task = (next_execution_utc, task)

                        if already_ran_this_minute:
                            continue

                        # test if task needs te be executed since last run of ops automator
                        execute_dt_since_last = task_cron_expression.last_since(localized_last_run, localized_current_dt)

                        if execute_dt_since_last is None:
                            if next_execution is not None:
                                next_execution = next_execution.astimezone(task_timezone)
                                self._logger.info(INFO_NEXT_EXECUTION, task_name, next_execution.isoformat(), task_timezone)
                            else:
                                self._logger.info(INFO_NO_NEXT_WITHIN, task_name)
                            continue

                        started_tasks.append(task_name)

                        self._logger.debug(INFO_SCHEDULED_TASK, task_name, execute_dt_since_last, task_timezone,
                                           str(safe_json(task, indent=2)))

                        # create an event for lambda function that starts execution by selecting for resources for this task
                        self._execute_task(task, execute_dt_since_last)

                    if started_tasks:
                        self._logger.info(INFO_STARTED_TASKS, enabled_tasks, ",".join(started_tasks))
                    else:
                        self._logger.info(INFO_NO_TASKS_STARTED, enabled_tasks)

                    self._set_next_schedule_event(current_dt, next_executed_task)

                    running_time = float((datetime.now() - start).total_seconds())

                    self._logger.info(INFO_RESULT, running_time)

                    return safe_dict({
                        "datetime": datetime.now().isoformat(),
                        "running-time": running_time,
                        "event-datetime": current_dt.isoformat(),
                        "enabled_tasks": enabled_tasks,
                        "started-tasks": started_tasks
                    })

                except ValueError as ex:
                    self._logger.error("{}\n{}".format(ex, safe_json(task, indent=2)))

        finally:
            self._logger.flush()

    def _set_next_schedule_event(self, scheduler_dt, next_executed_task):
        """
        Sets the cron expression of the scheduler event rule in cloudwatch depending on next executed task
        :param scheduler_dt: dt used for this scheduler run
        :param next_executed_task: Next task to execute
        :return: 
        """
        if next_executed_task is not None:

            utc = pytz.timezone("UTC")

            timestr = "{} ({})".format(next_executed_task[0].isoformat(), utc)
            next_task_tz = pytz.timezone(next_executed_task[1][handlers.TASK_TIMEZONE])
            if next_task_tz != utc:
                timestr += ", {} ({})".format(next_executed_task[0].astimezone(next_task_tz), next_task_tz)
            self._logger.info(INFO_NEXT_EXECUTED_TASK, next_executed_task[1][handlers.TASK_NAME], timestr)

            if next_executed_task[0] > scheduler_dt + timedelta(minutes=5):
                next_event_time = handlers.set_event_for_time(next_executed_task[0])
                self._logger.info(INFO_NEXT_EVENT.format(next_event_time.isoformat()))
            else:
                handlers.set_scheduler_rule_every_minute()
                self._logger.info(INFO_NEXT_ONE_MINUTE)
        else:
            self._logger.info(INFO_NO_TASKS_SCHEDULED)
            next_event_time = handlers.set_event_for_time(scheduler_dt)
            self._logger.info(INFO_NEXT_EVENT.format(next_event_time.isoformat()))

    def _execute_task(self, task, dt=None):
        """
        Execute a task by starting a lambda function that selects the resources for that action
        :param task: Task started
        :param dt: Task start datetime
        :return: 
        """
        event = {
            handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
            handlers.HANDLER_EVENT_TASK: task,
            handlers.HANDLER_EVENT_SOURCE: "aws:events",
            handlers.HANDLER_EVENT_TASK_DT: dt.isoformat() if dt is not None else datetime.utcnow().isoformat()
        }
        if self._context is not None:
            # start lambda function to scan for task resources
            payload = str.encode(safe_json(event))
            client = get_client_with_retries("lambda", ["invoke"], context=self._context)
            resp = client.invoke_with_retries(FunctionName=self._context.function_name,
                                              Qualifier=self._context.function_version,
                                              InvocationType="Event", LogType="None", Payload=payload)
            self._logger.info(INFO_LAMBDA, resp["StatusCode"], payload)
        else:
            # or if not running in lambda environment pass event to main task handler
            lambda_handler(event, None)
