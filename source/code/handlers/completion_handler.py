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
from datetime import datetime

import handlers
from handlers.task_tracking_table import TaskTrackingTable
from helpers import safe_dict, safe_json
from outputs.queued_logger import QueuedLogger

ERR_COMPLETION_HANDLER = "Completion handler error {}\n{}"

INF_COMPLETION_ITEMS_SET = "Execution time was {}, {} items set for completion check"
INF_DISABLED_COMPLETION_TIMER = "Disabled CloudWatch Events Rule \"{}\" as there are no tasks waiting for completion"
INF_SET_COMPLETION_TASK_TIMER = "Set new completion time for task {} ({}) to {}"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class CompletionHandler(object):
    """
    Class that handles time based events from CloudWatch rules
    """

    def __init__(self, event, context):
        """
        Initializes the instance.
        :param event: event to handle
        :param context: Lambda context
        """
        self._context = context
        self._event = event
        self._table = None

        # Setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = QueuedLogger(logstream=logstream, buffersize=20, context=context)

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Tests if event is handled by instance of this handler.
        :param _:
        :param event: Tested event
        :return: True if the event is a cloudwatch rule event for task completion
        """
        source = event.get(handlers.HANDLER_EVENT_SOURCE, "")
        if source != "aws.events":
            return False

        resources = event.get("resources", [])
        if len(resources) == 1 and resources[0].partition("/")[2].lower() == os.getenv(handlers.ENV_COMPLETION_RULE).lower():
            return True
        return False

    def handle_request(self):
        """
        Handles the cloudwatch rule timer event
        :return: Started tasks, if any, information
        """

        try:

            start = datetime.now()

            count = 0
            tracking_table = TaskTrackingTable(context=self._context, logger=self._logger)

            for task in tracking_table.get_tasks_to_check_for_completion():
                count += 1

                task_id = task[handlers.TASK_TR_ID]
                last_check_for_completion_time = datetime.now().isoformat()
                tracking_table.update_task(task_id, task=task.get(handlers.TASK_TR_NAME, None),
                                           task_metrics=task.get(handlers.TASK_TR_METRICS, False), status_data={
                        handlers.TASK_TR_LAST_WAIT_COMPLETION: last_check_for_completion_time
                    })

                self._logger.debug("Task is {}", task)
                self._logger.info(INF_SET_COMPLETION_TASK_TIMER, task.get(handlers.TASK_TR_NAME, None),
                                  task_id, last_check_for_completion_time)

            running_time = float((datetime.now() - start).total_seconds())
            self._logger.info(INF_COMPLETION_ITEMS_SET, running_time, count)

            if count == 0 and not handlers.running_local(self._context):
                rule = handlers.disable_completion_cloudwatch_rule(self._context)
                self._logger.info(INF_DISABLED_COMPLETION_TIMER, rule)

            return safe_dict({
                "datetime": datetime.now().isoformat(),
                "running-time": running_time,
                "tasks-to-check": count
            })

        except ValueError as ex:
            self._logger.error(ERR_COMPLETION_HANDLER, ex, safe_json(self._event, indent=2))

        finally:
            self._logger.flush()
