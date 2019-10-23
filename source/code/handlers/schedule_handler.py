###################################################################################################################### 
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           # 
#                                                                                                                    # 
#  Licensed under the Apache License Version 2.0 (the "License"). You may not use this file except in compliance     # 
#  with the License. A copy of the License is located at                                                             # 
#                                                                                                                    # 
#      http://www.apache.org/licenses/                                                                               # 
#                                                                                                                    # 
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES # 
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    # 
#  and limitations under the License.                                                                                # 
######################################################################################################################
import os
import types
import uuid
from datetime import datetime, timedelta

import boto3
import dateutil.parser

import actions
import configuration
import handlers.task_tracking_table
import pytz
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from configuration.task_configuration import TaskConfiguration
from helpers import safe_dict, safe_json
from main import lambda_handler
from outputs.queued_logger import QueuedLogger
from scheduling.cron_expression import CronExpression

NAME_ATTR = "Name"

LAST_SCHEDULER_RUN_KEY = "last-scheduler-run"

INFO_CONFIG_RUN = "Running scheduler for configuration update of task \"{}\""
INFO_CURRENT_SCHEDULING_DT = "Current datetime used for scheduling is {}"
INFO_LAST_SAVED = "Last saved scheduler execution was at {}"
INFO_NO_TASKS_STARTED = "Number of enabled tasks triggered by cron expression in configuration is {}, no tasks were started"
INFO_RESULT = "Handling cloudwatch event took {:>.3f} seconds"
INFO_SCHEDULED_TASK = "Scheduling task \"{}\" for time {} in timezone {}\nTask definition is {}"
INFO_STARTED_TASKS = "Number of enabled tasks is {}, started tasks {}"
INFO_NO_NEXT_WITHIN = "No executions for task {} scheduled within the next 24 hours"
INFO_TASK_SCHEDULER_ALREADY_RAN = "Scheduler already executed for this minute"
INFO_NEXT_EXECUTION = "Next execution for task \"{}\" within the next 24 hours will be at {} ({})"
INFO_NEXT_EXECUTED_TASK = "The first task that wil be executed within 24 hours  is \"{}\" at {}"
INFO_NEXT_ONE_MINUTE = "Next schedule event will be in one minute"
INF_NEXT_EVENT = "Next schedule event will be at {}"
INFO_NO_TASKS_SCHEDULED = "There are no tasks scheduled within the next 24 hours"
INFO_RUNNING_AS_ECS_JOB = "Running selection of resources for task {} as ECS job"
INFO_RUNNING_LAMBDA = "Running selection of resources on lambda function {}"
INF_HANDLING_EXEC_REQUEST = "Handing execute request for task {}"

ERR_FAILED_START_LAMBDA_TASK = "Failed to start lambda task, {}"
ERR_SCHEDULE_HANDLER = "Schedule handler error {}\n{}"

DEBUG_LAMBDA = "Invoked lambda function, started_tasks is {}, payload is {}"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class ScheduleHandler(object):
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
        self._logger = QueuedLogger(logstream=logstream, buffersize=50, context=context)

        self.configuration_update = ScheduleHandler.is_config_update(self._event)
        if self.configuration_update:
            if "OldImage" in self._event["Records"][0]["dynamodb"]:
                self.updated_task = self._event["Records"][0]["dynamodb"]["OldImage"][configuration.CONFIG_TASK_NAME]["S"]
            else:
                self.updated_task = self._event["Records"][0]["dynamodb"]["NewImage"][configuration.CONFIG_TASK_NAME]["S"]

        self.execute_task_request = self.is_execute_event(self._event)
        self.executed_task_name = event.get(handlers.HANDLER_EVENT_TASK_NAME, "") if self.execute_task_request else None

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Tests if event is handled by instance of this handler.
        :param _:
        :param event: Tested event
        :return: True if the event is a cloudwatch rule event for scheduling or configuration update
        """
        source = event.get(handlers.HANDLER_EVENT_SOURCE, "")

        if source == "aws.events":
            resources = event.get("resources", [])
            if len(resources) == 1 and resources[0].partition("/")[2].lower() == os.getenv(handlers.ENV_OPS_AUTOMATOR_RULE).lower():
                return True
            return False

        return ScheduleHandler.is_config_update(event) or ScheduleHandler.is_execute_event(event)

    @staticmethod
    def is_config_update(event):
        if event.get("Records", [{}])[0].get("eventSource", "") != "aws:dynamodb":
            return False

        source_arn = event["Records"][0]["eventSourceARN"]
        table_name = source_arn.split("/")[1]
        return table_name == os.getenv(configuration.ENV_CONFIG_TABLE)

    @staticmethod
    def is_execute_event(event):
        return event.get(handlers.HANDLER_EVENT_ACTION, "") == handlers.HANDLER_EVENT_SCHEDULER_EXECUTE_TASK and event.get(
            handlers.HANDLER_EVENT_TASK_NAME, None) is not None

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
            return dateutil.parser.parse(resp["Item"]["value"]).replace(second=0, microsecond=0)
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

        start = datetime.now()

        try:
            task_config = TaskConfiguration(context=self._context, logger=self._logger)
            if not self.execute_task_request:
                result = self.handle_scheduler_tasks(task_config)
            else:
                result = self.handle_execute_task_request(task_config)

            running_time = float((datetime.now() - start).total_seconds())

            self._logger.info(INFO_RESULT, running_time)

            return result
        finally:
            self._logger.flush()

    def handle_scheduler_tasks(self, task_config):

        started_tasks = {}
        start = datetime.now()

        last_run_dt = self._get_last_run()

        self._logger.info(INFO_LAST_SAVED, last_run_dt.isoformat())

        if self.configuration_update:
            self._logger.info(INFO_CONFIG_RUN, self.updated_task)

        current_dt = self._set_last_run()
        already_ran_this_minute = last_run_dt == current_dt
        if already_ran_this_minute and not (self.configuration_update or self.execute_task_request):
            self._logger.info(INFO_TASK_SCHEDULER_ALREADY_RAN)

        else:

            self._logger.info(INFO_CURRENT_SCHEDULING_DT, current_dt)

            task = None
            enabled_tasks = 0

            next_executed_task = None
            utc = pytz.timezone("UTC")

            tasks = [t for t in task_config.get_tasks() if
                     t.get(handlers.TASK_INTERVAL) is not None
                     and t.get(handlers.TASK_ENABLED, True)]

            try:
                for task in tasks:

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
                    next_execution_utc = next_execution.astimezone(utc).replace(
                        microsecond=0) if next_execution is not None else None

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

                    self._logger.info(INFO_SCHEDULED_TASK, task_name, execute_dt_since_last, task_timezone,
                                      str(safe_json(task, indent=2)))

                    # create an event for lambda function that starts execution by selecting for resources for this task
                    task_group, sub_tasks = self._execute_task(task, execute_dt_since_last)

                    started_tasks[task_name] = {
                        "task-group": task_group,
                        "sub-tasks": sub_tasks
                    }

                if started_tasks:
                    self._logger.info(INFO_STARTED_TASKS, enabled_tasks, ",".join(started_tasks))
                else:
                    self._logger.info(INFO_NO_TASKS_STARTED, enabled_tasks)

                self._set_next_schedule_event(current_dt, next_executed_task)

                running_time = float((datetime.now() - start).total_seconds())

                return safe_dict({
                    "datetime": datetime.now().isoformat(),
                    "running-time": running_time,
                    "event-datetime": current_dt.isoformat(),
                    "enabled_tasks": enabled_tasks,
                    "started-tasks": started_tasks
                })

            except ValueError as ex:
                self._logger.error(ERR_SCHEDULE_HANDLER, ex, safe_json(task, indent=2))

    def handle_execute_task_request(self, task_config):

        self._logger.info(INF_HANDLING_EXEC_REQUEST, self.executed_task_name)

        task_to_execute = task_config.get_task(name=self.executed_task_name)
        if task_to_execute is None:
            raise ValueError("Task with name {} does not exists for stack {}".format(self.executed_task_name,
                                                                                     os.getenv(handlers.ENV_STACK_NAME)))

        if not task_to_execute.get(handlers.TASK_ENABLED):
            raise ValueError("Task with name {} is not enabled", self.executed_task_name)

        task_group, sub_tasks = self._execute_task(task_to_execute)
        return safe_dict({
            "datetime": datetime.now().isoformat(),
            "executed-task": self.executed_task_name,
            "task-group": task_group,
            "sub-tasks": sub_tasks
        })

    def _set_next_schedule_event(self, scheduler_dt, next_executed_task):
        """
        Sets the cron expression of the scheduler event rule in cloudwatch depending on next executed task
        :param scheduler_dt: dt used for this scheduler run
        :param next_executed_task: Next task to execute
        :return:
        """
        if next_executed_task is not None:

            utc = pytz.timezone("UTC")

            time_str = "{} ({})".format(next_executed_task[0].isoformat(), utc)
            next_task_tz = pytz.timezone(next_executed_task[1][handlers.TASK_TIMEZONE])
            if next_task_tz != utc:
                time_str += ", {} ({})".format(next_executed_task[0].astimezone(next_task_tz), next_task_tz)
            self._logger.info(INFO_NEXT_EXECUTED_TASK, next_executed_task[1][handlers.TASK_NAME], time_str)

            if next_executed_task[0] > scheduler_dt + timedelta(minutes=5):
                next_event_time = handlers.set_event_for_time(next_executed_task[0], task=next_executed_task[1],
                                                              logger=self._logger, context=self._context)
                self._logger.info(INF_NEXT_EVENT, next_event_time.isoformat())
            else:
                handlers.set_scheduler_rule_every_minute(task=next_executed_task[1])
                self._logger.info(INFO_NEXT_ONE_MINUTE)
        else:
            self._logger.info(INFO_NO_TASKS_SCHEDULED)
            next_event_time = handlers.set_event_for_time(scheduler_dt, context=self._context, logger=self._logger)
            self._logger.info(INF_NEXT_EVENT, next_event_time.isoformat())

    @staticmethod
    def task_account_region_sub_tasks(task):
        action_properties = actions.get_action_properties(task[handlers.TASK_ACTION])

        aggregation_level = action_properties[actions.ACTION_AGGREGATION]
        # property may be a lambda function, call the function with parameters of task as lambda parameters
        if types.FunctionType == type(aggregation_level):
            aggregation_level = aggregation_level(task.get("parameters", {}))

        if aggregation_level == actions.ACTION_AGGREGATION_TASK:
            yield {
                handlers.TASK_THIS_ACCOUNT: task[handlers.TASK_THIS_ACCOUNT],
                handlers.TASK_ACCOUNTS: task[handlers.TASK_ACCOUNTS],
                handlers.TASK_REGIONS: task[handlers.TASK_REGIONS]
            }
        else:
            if task[handlers.TASK_THIS_ACCOUNT]:
                if aggregation_level == actions.ACTION_AGGREGATION_ACCOUNT:
                    yield {
                        handlers.TASK_THIS_ACCOUNT: True,
                        handlers.TASK_ACCOUNTS: [],
                        handlers.TASK_REGIONS: task[handlers.TASK_REGIONS]
                    }
                else:
                    for region in task.get(handlers.TASK_REGIONS, [None]):
                        yield {
                            handlers.TASK_THIS_ACCOUNT: True,
                            handlers.TASK_ACCOUNTS: [],
                            handlers.TASK_REGIONS: [region]
                        }

            for account in task.get(handlers.TASK_ACCOUNTS, []):
                if aggregation_level == actions.ACTION_AGGREGATION_ACCOUNT:
                    yield {
                        handlers.TASK_THIS_ACCOUNT: False,
                        handlers.TASK_ACCOUNTS: [account],
                        handlers.TASK_REGIONS: task[handlers.TASK_REGIONS]
                    }
                else:
                    for region in task.get(handlers.TASK_REGIONS, [None]):
                        yield {
                            handlers.TASK_THIS_ACCOUNT: False,
                            handlers.TASK_ACCOUNTS: [account],
                            handlers.TASK_REGIONS: [region]
                        }

    def _execute_task(self, task, dt=None, task_group=None):
        """
        Execute a task by starting a lambda function that selects the resources for that action
        :param task: Task started
        :param dt: Task start datetime
        :return:
        """

        debug_state = self._logger.debug_enabled
        self._logger.debug_enabled = task.get(handlers.TASK_DEBUG, False)
        if task_group is None:
            task_group = str(uuid.uuid4())
        try:

            event = {
                handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                handlers.HANDLER_EVENT_TASK: task,
                handlers.HANDLER_EVENT_SOURCE: "scheduler-handler",
                handlers.HANDLER_EVENT_TASK_DT: dt.isoformat() if dt is not None else datetime.utcnow().isoformat(),
                handlers.HANDLER_EVENT_TASK_GROUP: task_group
            }

            sub_tasks = list(ScheduleHandler.task_account_region_sub_tasks(task))
            for sub_task in sub_tasks:

                event[handlers.HANDLER_EVENT_SUB_TASK] = sub_task

                if not handlers.running_local(self._context):

                    if task[handlers.TASK_SELECT_SIZE] != actions.ACTION_USE_ECS:
                        # start lambda function to scan for task resources
                        payload = str.encode(safe_json(event))
                        client = get_client_with_retries("lambda", ["invoke"], context=self._context)

                        function_name = "{}-{}-{}".format(os.getenv(handlers.ENV_STACK_NAME),
                                                          os.getenv(handlers.ENV_LAMBDA_NAME),
                                                          task[handlers.TASK_SELECT_SIZE])

                        self._logger.info(INFO_RUNNING_LAMBDA, function_name)

                        try:
                            resp = client.invoke_with_retries(FunctionName=function_name,
                                                              InvocationType="Event", LogType="None", Payload=payload)

                            self._logger.debug(DEBUG_LAMBDA, resp["StatusCode"], payload)
                        except Exception as ex:
                            self._logger.error(ERR_FAILED_START_LAMBDA_TASK, str(ex))

                    else:
                        ecs_args = {
                            handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                            handlers.TASK_NAME: task[handlers.TASK_NAME],
                            handlers.HANDLER_EVENT_SUB_TASK: sub_task
                        }

                        ecs_memory = task.get(handlers.TASK_SELECT_ECS_MEMORY, None)

                        self._logger.info(INFO_RUNNING_AS_ECS_JOB, task[handlers.TASK_NAME])
                        handlers.run_as_ecs_job(ecs_args, ecs_memory_size=ecs_memory, context=self._context, logger=self._logger)

                else:
                    if task[handlers.TASK_SELECT_SIZE] == actions.ACTION_USE_ECS:
                        ecs_args = {
                            handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                            handlers.TASK_NAME: task[handlers.TASK_NAME],
                            handlers.HANDLER_EVENT_SUB_TASK: sub_task
                        }

                        ecs_memory = task.get(handlers.TASK_SELECT_ECS_MEMORY, None)

                        handlers.run_as_ecs_job(ecs_args, ecs_memory_size=ecs_memory, logger=self._logger)
                    else:
                        # or if not running in lambda environment pass event to main task handler
                        lambda_handler(event, self._context)

            return task_group, sub_tasks

        finally:
            self._logger.debug_enabled = debug_state
