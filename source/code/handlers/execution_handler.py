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
import json
import os
import threading
import time
from datetime import datetime, timedelta

import actions
import handlers
import services
from boto_retry import get_client_with_retries
from handlers.task_tracking_table import TaskTrackingTable
from helpers import safe_dict, safe_json, full_stack
from metrics.anonymous_metrics import send_metrics_data, allow_send_metrics
from outputs import raise_exception
from outputs.queued_logger import QueuedLogger

WARN_ADJUST_LAMBDA_MEMORY_SETTINGS_COMPLETION = "Adjust completion memory settings for task {}"

WARN_COMPLETION_CHECK_TIMEOUT = "Completion checking not completed after {} seconds"

REMAINING_COMPLETION_CHECK = 15

EXECUTE_TIME_REMAINING = 20

ERR_EXECUTION_NOT_COMPLETED = "Execution not completed after {} seconds"
ERR_BUILDING_SUBJECT_FOR_LOG_STREAM = "Error building log subject for action class {}, {}"
ERR_EXECUTING_ACTION = "Error running executing logic for action: {}"
ERR_EXECUTING_COMPLETION_CHECK = "Error running task completion check method : {}"
ERR_EXECUTION_TASK = "Error execution of {} for task {}\n {}{}"
ERR_INVALID_ACTION = "Action {} is not a valid action for the execution handler"
WARN_METRICS_DATA = "Error processing or sending metrics data ({})"
ERR_READING_S3_RESOURCES = "Error reading action resources from bucket {}, key {} for task {}, {}"
ERR_TASK_TIMEOUT = "Timeout waiting for completion of task after {}."
ERR_TIMEOUT = "Adjust execution memory settings for task {} or check boto retries"

INF_ACTION = "Executing action {} ({}) for task {} with parameters\n{}"
INF_ACTION_NOT_COMPLETED = "Action not completed after {}, waiting for next completion check"
INF_ACTION_RESULT = "Action completed in {} seconds, result is {}"
INF_FINISH_EXEC = "=== Finished execution of step {} for task with id {} ==="
INF_LAMBDA_MEMORY = "Memory limit for lambda {} executing the action is {}MB"
INF_RULE_ENABLED = "Enabling CloudWatch Events Rule \"{}\""
INF_SIMULATION_MODE_NO_RULE_ENABLED = "Completion handling not enabled as handler is running in simulation mode"
INF_START_EXEC = "=== Start step {} for task with id {} ==="
INF_STARTED_AND_WAITING_FOR_COMPLETION = "Action started with result \n{}\n Task is waiting for completion"
INF_TASK_COMPLETED = "Action completion check result is {}\n Task completed after {}"
INF_SENDING_METRICS_DATA = "Sending metrics data is {}"

LOG_STREAM = "{}-{}-{}-{}"


class ExecutionHandler(object):
    """
    Class to handle event to execute an action on a resource.
    """

    def __init__(self, event, context):
        """
        Initializes handler.
        :param event: Event to handle
        :param context: Context if run within Lambda environment
        """
        self._context = context
        self._event = event

        self.action_id = self._event[handlers.TASK_TR_ID]
        self.task = self._event[handlers.TASK_TR_NAME]
        self.task_timezone = self._event.get(handlers.TASK_TR_TIMEZONE, None)
        self.has_completion = self._event[handlers.TASK_TR_HAS_COMPLETION]
        self.action_parameters = self._event.get(handlers.TASK_TR_PARAMETERS, {})
        self.dryrun = self._event.get(handlers.TASK_TR_DRYRUN)
        self.interval = self._event.get(handlers.TASK_TR_INTERVAL,None)
        self.metrics = self._event.get(handlers.TASK_TR_METRICS, False)
        self.debug = self._event.get(handlers.TASK_TR_DEBUG)
        self.started_at = int(self._event.get(handlers.TASK_TR_STARTED_TS, 0))
        self.start_result = self._event.get(handlers.TASK_TR_START_RESULT, None)
        self.session = services.get_session(self._event.get(handlers.TASK_TR_ASSUMED_ROLE))
        self.stack_name = os.getenv(handlers.ENV_STACK_NAME)
        self.stack_id = os.getenv(handlers.ENV_STACK_ID)
        self.action = event[handlers.TASK_TR_ACTION]
        self.tagfilter = event.get(handlers.TASK_TR_TAGFILTER, "")
        self.action_properties = actions.get_action_properties(self.action)
        self.action_class = actions.get_action_class(self.action)
        self._stack_resources = None
        self.timeout = int(self._event[handlers.TASK_TR_TIMEOUT]) * 60 if self._event.get(handlers.TASK_TR_TIMEOUT, None) not in [
            None, "None"] else 0
        self.execution_log_stream = self._event.get(handlers.TASK_TR_EXECUTION_LOGSTREAM)
        self.assumed_role = self._event.get(handlers.TASK_TR_ASSUMED_ROLE, None)
        self.events = self._event.get(handlers.TASK_TR_EVENTS, {})
        if isinstance(self.events, str):
            self.events = json.loads(self._event.get(handlers.TASK_TR_EVENTS, "{}").replace("u'", '"').replace("'", '"'))

        self._action_resources = None
        self._s3_client = None
        self._action_instance = None
        self._action_class = None
        self._action_arguments = None
        self._timer = None
        self._timeout_event = None

        self.__logger = None
        self.__action_tracking = None

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Tests if event is handled by this handler.
        :param _:
        :param event: Tested event
        :return: True if the event is handled by this handler
        """
        return event.get(handlers.HANDLER_EVENT_ACTION, "") in [handlers.HANDLER_ACTION_EXECUTE,
                                                                handlers.HANDLER_ACTION_TEST_COMPLETION]

    @property
    def _logger(self):
        if self.__logger is None:
            # setup logging
            if self.execution_log_stream is None:

                if callable(getattr(self._action_class, "action_logging_subject", None)):
                    # noinspection PyBroadException
                    try:
                        action_subject = self._action_class.action_logging_subject(self._action_arguments,
                                                                                   self.action_parameters)
                        self.execution_log_stream = "{}-{}".format(self._event[handlers.TASK_TR_NAME], action_subject)
                    except Exception as ex:
                        print((ERR_BUILDING_SUBJECT_FOR_LOG_STREAM, str(self._action_class), ex))
                        action_subject = "unknown-"
                        self.execution_log_stream = LOG_STREAM.format(self._event[handlers.TASK_TR_NAME], action_subject,
                                                                      actions.log_stream_datetime(),
                                                                      self._action_arguments.get(handlers.TASK_TR_ID,"None"))
            else:
                self.execution_log_stream = self.execution_log_stream
            self.__logger = QueuedLogger(logstream=self.execution_log_stream,
                                         buffersize=50 if self.debug else 20,
                                         context=self._context,
                                         debug=self.debug)
        return self.__logger

    @property
    def _action_tracking(self):
        if self.__action_tracking is None:
            self.__action_tracking = TaskTrackingTable(self._context, logger=self._logger)
        return self.__action_tracking

    @property
    def s3_client(self):

        if self._s3_client is None:
            self._s3_client = get_client_with_retries("s3", ["get_object"])
        return self._s3_client

    @property
    def action_resources(self):
        if self._action_resources is None:

            if not self._event.get(handlers.TASK_TR_S3_RESOURCES, False):
                self._action_resources = handlers.get_item_resource_data(self._event, self._context)
            else:
                bucket = os.getenv(handlers.ENV_RESOURCE_BUCKET)
                key = self.action_id + ".json"
                try:
                    resp = self.s3_client.get_object_with_retries(Bucket=bucket, Key=key)
                    self._event[handlers.TASK_TR_RESOURCES] = resp["Body"].read().decode('utf-8')
                    self._action_resources = handlers.get_item_resource_data(self._event, self._context)
                except Exception as ex:
                    raise_exception(ERR_READING_S3_RESOURCES, bucket, key, self.action_id, ex)

        return self._action_resources

    @property
    def stack_resources(self):
        """
        Reads the action stack resources
        :return: Stack resources for the action
        """

        if self._stack_resources is None:

            self._stack_resources = {}
            # test if this action has additional stack resources
            resources = self.action_properties.get(actions.ACTION_STACK_RESOURCES, {})
            if resources:
                # name of the class
                class_name = self.action_properties[actions.ACTION_CLASS_NAME][0:-len("Action")]
                # actual resource names is name of class + name from class properties
                logical_resource_names = [class_name + resource_name for resource_name in resources]

                cfn = get_client_with_retries("cloudformation", ["list_stack_resources"], context=self._context)
                args = {"StackName": self.stack_id}
                while True:
                    # get the stack resources
                    cfn_resp = cfn.list_stack_resources_with_retries(**args)
                    for res in cfn_resp.get("StackResourceSummaries", []):

                        # actual name
                        logical_resource_id = res["LogicalResourceId"]
                        # test if this resource is an resource from the action properties
                        if logical_resource_id in logical_resource_names:
                            self._stack_resources[logical_resource_id[len(class_name):]] = {
                                i: res[i] for i in ["LogicalResourceId",
                                                    "PhysicalResourceId",
                                                    "ResourceType"]
                            }

                        # test if we've found the number of resources that we declared, in that case no need to read more
                        if len(list(self._stack_resources.keys())) == len(resources):
                            return self._stack_resources

                    # continuation if > 100 resources in stack
                    if "NextToken" in cfn_resp:
                        args["NextToken"] = cfn_resp["NextToken"]
                    else:
                        break
        return self._stack_resources

    def _handle_task_execution(self):

        def execute_timed_out():
            """
            Function is called when the handling of the request times out
            :return:
            """
            time_used = int(int(os.getenv(handlers.ENV_LAMBDA_TIMEOUT)) - self._context.get_remaining_time_in_millis() / 1000)
            self._logger.error(ERR_EXECUTION_NOT_COMPLETED, time_used)

            if self.action_properties.get(actions.ACTION_EXECUTE_SIZE, None) is not None:
                self._logger.error(ERR_TIMEOUT, self.task)

            self._timeout_event.set()
            self._logger.flush()
            self._timer.cancel()

        def handle_metrics(result):
            self._logger.info(INF_SENDING_METRICS_DATA, "enabled" if allow_send_metrics() else "disabled")
            if allow_send_metrics():
                try:
                    result_data = result if isinstance(result, dict) else json.loads(result)
                    if actions.METRICS_DATA in result_data:
                        send_metrics_data(metrics_data=result_data[actions.METRICS_DATA], logger=self._logger)
                except Exception as ex:
                    self._logger.warning(WARN_METRICS_DATA, str(ex))

        self._logger.info(INF_ACTION, self.action, self.action_id, self.task, safe_json(self.action_parameters, indent=3))
        if not handlers.running_local(self._context):
            self._logger.info(INF_LAMBDA_MEMORY, self._context.function_name, self._context.memory_limit_in_mb)

        self._logger.debug("Setting task state to {}", handlers.STATUS_STARTED)
        self._action_tracking.update_task(self.action_id, self.task, task_metrics=self.metrics, status=handlers.STATUS_STARTED)

        start = time.time()

        return_data = {
            "task": self.task,
            "action": self.action,
            "id": self.action_id,
            "dryrun": self.dryrun,
        }

        if self._context is not None:
            execution_time_left = (self._context.get_remaining_time_in_millis() / 1000.00) - EXECUTE_TIME_REMAINING
            self._timer = threading.Timer(execution_time_left, execute_timed_out)
            self._timer.start()

        try:
            self._logger.debug("Start executing task")
            action_result = self._action_instance.execute()
            if isinstance(action_result, str):
                action_result = json.loads(action_result)
        finally:
            if self._timer is not None:
                self._timer.cancel()
                if self._timeout_event.is_set():
                    raise Exception("Timeout execution action")

        if not self._action_instance.properties.get(actions.ACTION_INTERNAL, False):
            handle_metrics(action_result)

        execution_time = int(time.time() - start)
        self._logger.debug("Task needs{}completion", " no" if not self.has_completion else " ")
        if not self.has_completion or self.dryrun:

            self._logger.debug("Setting state of task to {} ", handlers.STATUS_COMPLETED)
            self._action_tracking.update_task(action_id=self.action_id, task=self.task, task_metrics=self.metrics,
                                              status=handlers.STATUS_COMPLETED,
                                              status_data={
                                                  handlers.TASK_TR_STARTED_TS: int(start),
                                                  handlers.TASK_TR_RESULT: action_result,
                                                  handlers.TASK_TR_EXECUTION_TIME: str(execution_time),
                                                  handlers.TASK_TR_EXECUTION_LOGSTREAM: self.execution_log_stream
                                              })
            # noinspection PyBroadException
            try:
                self._logger.info(INF_ACTION_RESULT, execution_time, safe_json(action_result, indent=3))
            except Exception:
                self._logger.info(INF_ACTION_RESULT, execution_time, str(action_result))

        else:
            # the action has a method for testing completion of the task, set the status to waiting and store the result
            # of the execution that started the action as start result that will be passed to the completion method together
            self._logger.debug("Setting state of task to {} ", handlers.STATUS_WAIT_FOR_COMPLETION)
            self._action_tracking.update_task(action_id=self.action_id,
                                              task=self.task,
                                              task_metrics=self.metrics,
                                              status=handlers.STATUS_WAIT_FOR_COMPLETION,
                                              status_data={
                                                  handlers.TASK_TR_LAST_WAIT_COMPLETION: datetime.now().isoformat(),
                                                  handlers.TASK_TR_STARTED_TS: int(start),
                                                  handlers.TASK_TR_START_RESULT: action_result,
                                                  handlers.TASK_TR_START_EXECUTION_TIME: str(execution_time),
                                                  handlers.TASK_TR_EXECUTION_LOGSTREAM: self.execution_log_stream
                                              })

            self._logger.info(INF_STARTED_AND_WAITING_FOR_COMPLETION, safe_json(action_result, indent=3))

            if not handlers.running_local(self._context):
                rule = handlers.enable_completion_cloudwatch_rule(self._context)
                self._logger.info(INF_RULE_ENABLED, rule)
            else:
                self._logger.info(INF_SIMULATION_MODE_NO_RULE_ENABLED)

        # no exception from action
        return_data.update({
            "result": handlers.STATUS_WAIT_FOR_COMPLETION if self.has_completion else handlers.STATUS_COMPLETED,
            "action-result": str(action_result),
            "datetime": datetime.now().isoformat(),
            "running-time": str(execution_time),
            "task-group": self._event[handlers.TASK_TR_GROUP],
            "task-id": self._event[handlers.TASK_TR_ID]
        })

        return safe_dict(return_data)

    def _handle_test_task_completion(self):

        def completion_timed_out():
            """
            Function is called when the handling of the request times out
            :return:
            """
            time_used = int(os.getenv(handlers.ENV_LAMBDA_TIMEOUT) - self._context.get_remaining_time_in_millis() / 1000)
            self._logger.warning(WARN_COMPLETION_CHECK_TIMEOUT, time_used)

            if self.action_properties.get(actions.ACTION_COMPLETION_SIZE, None) is not None:
                self._logger.warning(WARN_ADJUST_LAMBDA_MEMORY_SETTINGS_COMPLETION, time_used, self.task)

            self._timeout_event.set()

            self._logger.flush()
            if self._timer is not None:
                self._timer.cancel()

        execution_time = int(time.time()) - self.started_at
        execution_time_str = str(timedelta(seconds=execution_time))

        result_data = {
            "task": self.task,
            "action": self.action,
            "id": self.action_id,
            "datetime": datetime.now().isoformat(),
            "running-time": execution_time
        }

        if self._context is not None:
            execution_time_left = (self._context.get_remaining_time_in_millis() / 1000.00) - REMAINING_COMPLETION_CHECK
            self._timer = threading.Timer(execution_time_left, completion_timed_out)
            self._timer.start()

        try:
            # make one more check for completion before testing for timeout
            check_result = self._action_instance.is_completed(self.start_result)
        finally:
            if self._timer is not None:
                self._timer.cancel()
                if self._timeout_event.is_set():
                    raise Exception("Task completion check timed out")

        if check_result is not None:

            self._action_tracking.update_task(action_id=self.action_id,
                                              task=self.task,
                                              task_metrics=self.metrics,
                                              status=handlers.STATUS_COMPLETED,
                                              status_data={
                                                  handlers.TASK_TR_RESULT: check_result,
                                                  handlers.TASK_TR_EXECUTION_TIME: str(execution_time)
                                              })

            self._logger.info(INF_TASK_COMPLETED, safe_json(check_result, indent=3), execution_time_str)

            result_data.update({
                "result": handlers.STATUS_COMPLETED,
                "action-result": str(check_result)

            })

        elif execution_time > self.timeout:
            self._action_tracking.update_task(action_id=self.action_id,
                                              task=self.task,
                                              task_metrics=self.metrics,
                                              status=handlers.STATUS_TIMED_OUT,
                                              status_data={handlers.TASK_TR_EXECUTION_TIME: str(execution_time)
                                                           })

            self._logger.error(ERR_TASK_TIMEOUT, execution_time_str)

            result_data.update({
                "result": handlers.STATUS_TIMED_OUT
            })

            return result_data

        else:

            self._logger.info(INF_ACTION_NOT_COMPLETED, execution_time_str)
            result_data.update({
                "result": handlers.STATUS_WAIT_FOR_COMPLETION
            })

        return safe_dict(result_data)

    # noinspection PyDictCreation
    def handle_request(self):
        """
        Handles action execute requests, creates an instance of the required action class and executes the action on the
        resources passed in the event.
        :return:
        """

        # get class of the action, this class is needed by the _logger property
        self._action_class = actions.get_action_class(self.action)

        try:
            self._action_arguments = {
                actions.ACTION_PARAM_CONTEXT: self._context,
                actions.ACTION_PARAM_EVENT: self._event,
                actions.ACTION_PARAM_SESSION: self.session,
                actions.ACTION_PARAM_RESOURCES: self.action_resources,
                actions.ACTION_PARAM_INTERVAL: self.interval,
                actions.ACTION_PARAM_DEBUG: self.debug,
                actions.ACTION_PARAM_DRYRUN: self.dryrun,
                actions.ACTION_PARAM_TASK_ID: self.action_id,
                actions.ACTION_PARAM_TASK: self.task,
                actions.ACTION_PARAM_TASK_TIMEZONE: self.task_timezone,
                actions.ACTION_PARAM_STACK: self.stack_name,
                actions.ACTION_PARAM_STACK_ID: self.stack_id,
                actions.ACTION_PARAM_STACK_RESOURCES: self.stack_resources,
                actions.ACTION_PARAM_ASSUMED_ROLE: self.assumed_role,
                actions.ACTION_PARAM_STARTED_AT: self.started_at,
                actions.ACTION_PARAM_TAGFILTER: self.tagfilter,
                actions.ACTION_PARAM_TIMEOUT: self.timeout,
                actions.ACTION_PARAM_TAG_FILTER: self.tagfilter,
                actions.ACTION_PARAM_EVENTS: self.events}

            # called after initialization other arguments as it is using these to construct the logger
            self._action_arguments[actions.ACTION_PARAM_LOGGER] = self._logger

            if self._context is not None:
                self._timeout_event = threading.Event()
                self._action_arguments[actions.ACTION_PARAM_TIMEOUT_EVENT] = self._timeout_event

            # create the instance of the action class
            self._action_instance = self._action_class(self._action_arguments, self.action_parameters)

            self._logger.info(INF_START_EXEC, self._event[handlers.HANDLER_EVENT_ACTION], self.action_id)

            if self._event[handlers.HANDLER_EVENT_ACTION] == handlers.HANDLER_ACTION_EXECUTE:
                return self._handle_task_execution()

            elif self._event[handlers.HANDLER_EVENT_ACTION] == handlers.HANDLER_ACTION_TEST_COMPLETION:
                return self._handle_test_task_completion()

            raise Exception(
                ERR_INVALID_ACTION.format(self._event[handlers.HANDLER_EVENT_ACTION]))

        except Exception as ex:

            self._logger.error(ERR_EXECUTION_TASK, self._event[handlers.HANDLER_EVENT_ACTION], self.task, str(ex),
                               ("\n" + full_stack()) if self.debug else "")

            self._action_tracking.update_task(action_id=self.action_id,
                                              task=self.task,
                                              task_metrics=self.metrics,
                                              status=handlers.STATUS_FAILED,
                                              status_data={handlers.TASK_TR_ERROR: str(ex)})
        finally:
            self._logger.info(INF_FINISH_EXEC, self._event[handlers.HANDLER_EVENT_ACTION], self.action_id)
            self._logger.flush()
