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
import time
from datetime import datetime

import actions
import boto_retry
import configuration
import configuration.task_configuration
import handlers
import handlers.task_tracking_table
import outputs.queued_logger
from helpers import full_stack, safe_dict, safe_json

ECS_TASK_NOT_FOUND_FOR_STEP = "Task {} was not found or is not in a {} state for action step  {}"
ECS_TASK_DOES_NOT_EXIST = "Task {} does not exist"

ERR_ECS_NO_TASK_NAME = "Task name not specified in parameter {}, parameters {}"
ERR_ECS_NO_TASK_ID = "No task id specified in parameter {}, parameters {}"
ERR_ECS_NO_PARAM = "No action specified in parameter {}, parameters {}"

ERR_IS_HANDLING = "Error testing handler {} for event {}, {}"
ERR_HANDLING_REQUEST = "Error handling request {} by handler {}: ({})\n{}"
MSG_NO_REQUEST_HANDLER = "Request was not handled, no handler was able to handle this type of request {}"
MSG_BOTO_STATS = "Boto call statistics: \n{}"

DEBUG_HANDLER_INFO = "Handler is {}"

ENV_DEBUG_MAIN_EVENT_HANDLER = "DEBUG_MAIN_EVENT_HANDLER"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


# load models for services that have not have their latest models deployed to Lambda
def load_models():
    cdw = os.getcwd()
    models = os.path.join(cdw, "models")
    aws_data_path = os.getenv("AWS_DATA_PATH", None)
    if aws_data_path is not None:
        aws_data_path = ":".join([aws_data_path, models])
    else:
        aws_data_path = models
    os.environ["AWS_DATA_PATH"] = aws_data_path


load_models()


class EcsTaskContext(object):

    def __init__(self, timeout_seconds):
        self._started = time.time()
        self._timeout = timeout_seconds
        self.run_local = False
        self.function_name = "ECS"

    def get_remaining_time_in_millis(self):
        return max(self._timeout - (time.time() - self._started), 0) * 1000


def lambda_handler(event, context):
    dt = datetime.utcnow()
    log_stream_name = LOG_STREAM.format("OpsAutomatorMain", dt.year, dt.month, dt.day)

    with outputs.queued_logger.QueuedLogger(logstream=log_stream_name, context=context, buffersize=20) as logger:

        for handler_name in handlers.all_handlers():

            try:
                if not handlers.get_class_for_handler(handler_name).is_handling_request(event, context):
                    continue
            except Exception as ex:
                logger.error(ERR_IS_HANDLING, handler_name, safe_json(event, indent=2), ex)
                break

            if context is not None and os.getenv(ENV_DEBUG_MAIN_EVENT_HANDLER, "false").lower() == "true":
                print(("Handler is {}".format(handler_name)))
                print(("Event is {}".format(safe_json(event, indent=3))))

            handler = handlers.create_handler(handler_name, event, context)
            try:
                logger.debug(DEBUG_HANDLER_INFO, handler_name)
                result = handler.handle_request()
                return safe_dict(result)
            except Exception as e:
                logger.error(ERR_HANDLING_REQUEST, safe_json(event, indent=2), handler_name, e, full_stack())
            finally:
                if len(boto_retry.statistics) > 0:
                    logger.info(MSG_BOTO_STATS, safe_json(boto_retry.statistics, indent=3))
                    boto_retry.clear_statistics()
            return
        else:
            logger.debug(MSG_NO_REQUEST_HANDLER, safe_json(event, indent=2))


def ecs_handler(args):
    dt = datetime.utcnow()
    log_stream = LOG_STREAM.format("OpsAutomatorMainEcs", dt.year, dt.month, dt.day)

    with outputs.queued_logger.QueuedLogger(logstream=log_stream, context=None, buffersize=20) as logger:

        action_step = args.get(handlers.HANDLER_EVENT_ACTION, None)
        if action_step is None:
            logger.error(ERR_ECS_NO_PARAM, handlers.HANDLER_EVENT_ACTION, safe_json(args, indent=3))
            return

        event = {}
        task_item = {}

        if action_step in [handlers.HANDLER_ACTION_EXECUTE, handlers.HANDLER_ACTION_TEST_COMPLETION]:
            task_id = args.get(handlers.TASK_TR_ID, None)
            if task_id is None:
                logger.error(ERR_ECS_NO_TASK_ID, handlers.TASK_TR_ID, safe_json(args, indent=3))
                return

            expected_status = handlers.STATUS_PENDING \
                if action_step == handlers.HANDLER_ACTION_EXECUTE \
                else handlers.STATUS_WAIT_FOR_COMPLETION
            task_item = handlers.task_tracking_table.TaskTrackingTable(
                logger=logger,
                context=EcsTaskContext(timeout_seconds=300)).get_task_item(task_id, status=expected_status)

            if task_item is None:
                logger.error(ECS_TASK_NOT_FOUND_FOR_STEP, task_id, expected_status, action_step)
                return

            event = {i: task_item.get(i) for i in task_item}
            event[handlers.HANDLER_EVENT_ACTION] = action_step

        elif action_step in [handlers.HANDLER_ACTION_SELECT_RESOURCES]:
            task_name = actions.ACTION_ID = args.get(handlers.TASK_NAME, None)
            if task_name is None:
                logger.error(ERR_ECS_NO_TASK_NAME, configuration.CONFIG_TASK_NAME, safe_json(args, indent=3))
                return

            task_item = configuration.task_configuration.TaskConfiguration(logger=logger, context=None).get_task(task_name)
            if task_item is None:
                logger.error(ECS_TASK_DOES_NOT_EXIST, task_name)
                return

            event = {
                handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                handlers.HANDLER_EVENT_TASK: task_item,
                handlers.HANDLER_EVENT_SOURCE: "ecs_handler",
                handlers.HANDLER_EVENT_TASK_DT: datetime.now().isoformat()
            }

        timeout = task_item.get(handlers.TASK_TIMEOUT, 3600)
        if not timeout:
            timeout = 3600

        return lambda_handler(event=event, context=EcsTaskContext(timeout_seconds=timeout))
