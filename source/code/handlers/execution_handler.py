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

import json
import os
import traceback
from datetime import datetime, timedelta
from time import time

import actions
import handlers
import handlers.task_tracking_table as tracking
from boto_retry import get_client_with_retries
from handlers.task_tracking_table import TaskTrackingTable
from services.aws_service import AwsService
from util import safe_dict
from util.logger import Logger
from util.metrics import send_metrics_data, allow_send_metrics

ERR_EXECUTION_TASK = "Error execution of {} for task {}, ({})\n {}"
ERR_EXECUTING_ACTION = "Error running executing logic for action: {}"
ERR_EXECUTING_COMPLETION_CHECK = "Error running task completion check method : {}"
ERR_TASK_TIMEOUT = "Timeout waiting for completion of task after {}."
INFO_ACTION = "Executing action {} ({}) {}for task {} with parameters {}"
INFO_ACTION_NOT_COMPLETED = "Action not completed after {}, waiting for next completion check"
INFO_ACTION_RESULT = "Action completed in {:>.3f} seconds, result is {}"
INFO_CHECK_TASK_COMPLETION = "Checking completion for action \"{}\" for task \"{}\" ({}) with parameters {} " \
                             "and execution start result {}"
INFO_RULE_ENABLED = "Enabling CloudWatch Events Rule \"{}\""
INFO_STARTED_AND_WAITING_FOR_COMPLETION = "Action started with result {}\n Task is waiting for completion"
INFO_TASK_COMPLETED = "Action completion check result is {}\n Task completed after {}"
INFO_LAMBDA_MEMORY = "Memory limit for lambda {} executing the action is {}MB"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}-{}"


class ExecutionHandler:
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
        self._action_tracking = TaskTrackingTable(context)

        self.action_id = self._event[tracking.TASK_TR_ID]
        self.task = self._event[tracking.TASK_TR_NAME]
        self.action = self._event[tracking.TASK_TR_ACTION]
        self.test_completion_method = getattr(actions.get_action_class(self.action), handlers.COMPLETION_METHOD, None)
        self.action_parameters = json.loads(self._event.get(tracking.TASK_TR_PARAMETERS, "{}"))
        self.action_resources = json.loads(self._event.get(tracking.TASK_TR_RESOURCES, "{}"))
        self.dryrun = self._event.get(tracking.TASK_TR_DRYRUN)
        self.debug = self._event.get(tracking.TASK_TR_DEBUG)
        self.started_at = float(self._event.get(tracking.TASK_TR_STARTED_TS, 0))
        self.start_result = self._event.get(tracking.TASK_TR_START_RESULT, None)
        self.session = AwsService.get_session(self._event.get(tracking.TASK_TR_ASSUMED_ROLE))
        self.stack_name = os.getenv(handlers.ENV_STACK_NAME)
        self.stack_id = os.getenv(handlers.ENV_STACK_ID)
        self.action = event[tracking.TASK_TR_ACTION]
        self.action_properties = actions.get_action_properties(self.action)
        self.action_class = actions.get_action_class(self.action)
        self._stack_resources = None
        self.timeout = self._event.get(tracking.TASK_TR_TIMEOUT)
        self.execution_log_stream = self._event.get(tracking.TASK_TR_EXECUTION_LOGSTREAM)

        # setup logging
        if self.execution_log_stream is None:
            dt = datetime.utcnow()
            self.execution_log_stream = LOG_STREAM.format(self._event[tracking.TASK_TR_NAME], dt.year, dt.month, dt.day, dt.hour,
                                                          dt.minute, self.action_id)
        else:
            self.execution_log_stream = self.execution_log_stream

        debug = event[tracking.TASK_TR_DEBUG]

        self._logger = Logger(logstream=self.execution_log_stream, buffersize=40 if debug else 20, context=context, debug=debug)

    @staticmethod
    def is_handling_request(event):
        """
        Tests if event is handled by this handler.
        :param event: Tested event
        :return: True if the event is handled by this handler
        """
        return event.get(handlers.HANDLER_EVENT_ACTION, "") in [handlers.HANDLER_ACTION_EXECUTE,
                                                                handlers.HANDLER_ACTION_TEST_COMPLETION]

    @property
    def stack_resources(self):
        """
        Reads the action stack resources
        :return: Stack resources for the action
        """

        if self._stack_resources is None:

            self._stack_resources = {}
            # test if this action has additional stack resources
            resources = self.action_properties.get(actions.ACTION_PARAM_STACK_RESOURCES, {})
            if resources:
                # name of the class
                class_name = self.action_properties[actions.ACTION_CLASS_NAME]
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
                        if len(self._stack_resources.keys()) == len(resources):
                            return self._stack_resources

                    # continuation if > 100 resources in stack
                    if "NextToken" in cfn_resp:
                        args["NextToken"] = cfn_resp["NextToken"]
                    else:
                        break
        return self._stack_resources

    def _handle_task_execution(self, action_instance, args):

        def handle_metrics(result):
            self._logger.info("Sending metrics data is {}", "enabled" if allow_send_metrics() else "disabled")
            if allow_send_metrics():
                try:
                    result_data = result if isinstance(result, dict) else json.loads(result)
                    if actions.METRICS_DATA in result_data:
                        send_metrics_data(metrics=result_data[actions.METRICS_DATA], logger=self._logger)
                except Exception as ex:
                    self._logger.warning("Error processing or sending metrics data ({})", str(ex))

        self._logger.info(INFO_ACTION, self.action, self.action_id, "in dry-run mode " if self.dryrun else "", self.task,
                          json.dumps(self.action_parameters, indent=2))
        self._logger.info(INFO_LAMBDA_MEMORY, self._context.function_name, self._context.memory_limit_in_mb)

        self._action_tracking.update_action(self.action_id, status=tracking.STATUS_STARTED)

        start = time()

        return_data = {
            "task": self.task,
            "action": self.action,
            "id": self.action_id,
            "dryrun": self.dryrun,
        }

        action_result = action_instance.execute(args)

        if not action_instance.properties.get(actions.ACTION_INTERNAL, False):
            handle_metrics(action_result)

        execution_time = round(float((time() - start)), 3)

        if self.test_completion_method is None or self.dryrun:

            self._action_tracking.update_action(action_id=self.action_id,
                                                status=tracking.STATUS_COMPLETED,
                                                status_data={
                                                    tracking.TASK_TR_STARTED_TS: datetime.now().isoformat(),
                                                    tracking.TASK_TR_RESULT: str(action_result),
                                                    tracking.TASK_TR_EXECUTION_TIME: str(execution_time),
                                                    tracking.TASK_TR_EXECUTION_LOGSTREAM: self.execution_log_stream
                                                })
            self._logger.info(INFO_ACTION_RESULT, execution_time, str(action_result))

        else:
            # the action has a method for testing completion of the task, set the status to waiting and store the result
            # of the execution that started the action as start result that will be passed to the completion method together
            self._action_tracking.update_action(action_id=self.action_id,
                                                status=tracking.STATUS_WAIT_FOR_COMPLETION,
                                                status_data={
                                                    tracking.TASK_TR_LAST_WAIT_COMPLETION: datetime.now().isoformat(),
                                                    tracking.TASK_TR_STARTED_TS: int(start),
                                                    tracking.TASK_TR_START_RESULT: str(action_result),
                                                    tracking.TASK_TR_START_EXECUTION_TIME: str(execution_time),
                                                    tracking.TASK_TR_EXECUTION_LOGSTREAM: self.execution_log_stream
                                                })

            self._logger.info(INFO_STARTED_AND_WAITING_FOR_COMPLETION, str(action_result))
            if self._context is not None:
                rule = handlers.enable_completion_cloudwatch_rule(self._context)
                self._logger.info(INFO_RULE_ENABLED, rule)

        # no exception from action
        return_data.update({
            "result": tracking.STATUS_COMPLETED,
            "action-result": str(action_result),
            "datetime": datetime.now().isoformat(),
            "running-time": execution_time
        })

        return safe_dict(return_data)

    def _handle_test_task_completion(self, action_instance, arguments):

        self._logger.info(
            INFO_CHECK_TASK_COMPLETION, self.action, self.task, self.action_id, json.dumps(self.action_parameters, indent=2),
            self.start_result)

        execution_time = round(float((time() - self.started_at)), 3)

        execution_time_str = str(timedelta(seconds=execution_time))

        result_data = {
            "task": self.task,
            "action": self.action,
            "id": self.action_id,
            "datetime": datetime.now().isoformat(),
            "running-time": execution_time
        }

        # make one more check for completion before testing for timeout
        check_result = action_instance.is_completed(arguments, self.start_result)

        if check_result is not None:

            self._action_tracking.update_action(action_id=self.action_id,
                                                status=tracking.STATUS_COMPLETED,
                                                status_data={
                                                    tracking.TASK_TR_RESULT: str(check_result),
                                                    tracking.TASK_TR_EXECUTION_TIME: str(execution_time)
                                                })

            self._logger.info(INFO_TASK_COMPLETED, str(check_result),
                              execution_time_str)

            result_data.update({
                "result": tracking.STATUS_COMPLETED,
                "action-result": str(check_result)

            })

        elif execution_time > self.timeout:
            self._action_tracking.update_action(action_id=self.action_id,
                                                status=tracking.STATUS_TIMED_OUT,
                                                status_data={
                                                    tracking.TASK_TR_EXECUTION_TIME: str(execution_time)
                                                })

            self._logger.error(ERR_TASK_TIMEOUT, execution_time_str)

            result_data.update({
                "result": tracking.STATUS_TIMED_OUT
            })

            return result_data

        else:

            self._logger.info(INFO_ACTION_NOT_COMPLETED, execution_time_str)
            result_data.update({
                "result": tracking.STATUS_WAIT_FOR_COMPLETION
            })

        return safe_dict(result_data)

    def handle_request(self):
        """
        Handles action execute requests, creates an instance of the required action class and executes the action on the
        resources passed in the event.
        :return:
        """

        try:
            self._logger.info("Handler {}", self.__class__.__name__)

            args = {
                actions.ACTION_PARAM_CONTEXT: self._context,
                actions.ACTION_PARAM_EVENT: self._event,
                actions.ACTION_PARAM_SESSION: self.session,
                actions.ACTION_PARAM_LOGGER: self._logger,
                actions.ACTION_PARAM_RESOURCES: self.action_resources,
                actions.ACTION_PARAM_DEBUG: self.debug,
                actions.ACTION_PARAM_DRYRUN: self.dryrun,
                actions.ACTION_PARAM_ACTION_ID: self.action_id,
                actions.ACTION_PARAM_TASK: self.task,
                actions.ACTION_PARAM_STACK: self.stack_name,
                actions.ACTION_PARAM_STACK_ID: self.stack_id,
                actions.ACTION_PARAM_STACK_RESOURCES: self.stack_resources
            }
            args.update(self.action_parameters)

            action_instance = actions.create_action(self.action, args)

            if self._event[handlers.HANDLER_EVENT_ACTION] == handlers.HANDLER_ACTION_EXECUTE:
                return self._handle_task_execution(action_instance, args)
            elif self._event[handlers.HANDLER_EVENT_ACTION] == handlers.HANDLER_ACTION_TEST_COMPLETION:
                return self._handle_test_task_completion(action_instance, args)
        except Exception as ex:
            self._logger.error(ERR_EXECUTION_TASK, self._event[handlers.HANDLER_EVENT_ACTION], self.task, str(ex),
                               traceback.format_exc())
            self._action_tracking.update_action(action_id=self.action_id, status=tracking.STATUS_FAILED,
                                                status_data={tracking.TASK_TR_ERROR: str(ex)})
        finally:
            self._logger.flush()
