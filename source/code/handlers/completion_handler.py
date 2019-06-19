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
import handlers.task_tracking_table as tracking
from handlers.task_tracking_table import TaskTrackingTable
from util import safe_dict, safe_json
from util.logger import Logger

INF_COMPETION_ITEMS_SET = "Execution time was {}, {} items set fo completion check"
INF_DISABLED_COMPLETION_TIMER = "Disabled CloudWatch Events Rule \"{}\" as there are no tasks waiting for completion"
INF_SET_COMPLETION_TASK_TIMER = "Set new completion time for task {} ({}) to {}"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class CompletionHandler:
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
        self._logger = Logger(logstream=logstream, buffersize=20, context=context)

    @staticmethod
    def is_handling_request(event):
        """
        Tests if event is handled by instance of this handler.
        :param event: Tested event
        :return: True if the event is a cloudwatch rule event for task completion
        """
        source = event.get(handlers.HANDLER_EVENT_SOURCE, "")
        if source != "aws.events":
            return False

        resources = event.get("resources", [])
        if len(resources) == 1 and resources[0].partition("/")[2].startswith(
                        os.getenv(handlers.ENV_STACK_NAME) + "-CompletionRule-"):
            return True
        return False

    def handle_request(self):
        """
        Handles the cloudwatch rule timer event
        :return: Started tasks, if any, information
        """

        try:

            start = datetime.now()
            self._logger.info("Handler {}", self.__class__.__name__)

            count = 0
            tracking_table = TaskTrackingTable(context=self._context)

            for task in tracking_table.get_tasks_to_check_for_completion():

                count += 1

                task_id = task[tracking.TASK_TR_ID]
                last_check_for_completion_time = datetime.now().isoformat()
                tracking_table.update_action(task_id, status_data={
                    tracking.TASK_TR_LAST_WAIT_COMPLETION: last_check_for_completion_time
                })

                self._logger.info(INF_SET_COMPLETION_TASK_TIMER, task[tracking.TASK_TR_NAME],
                                  task_id, last_check_for_completion_time)

            running_time = float((datetime.now() - start).total_seconds())
            self._logger.info(INF_COMPETION_ITEMS_SET, running_time, count)

            if count == 0:
                rule = handlers.disable_completion_cloudwatch_rule(self._context)
                self._logger.info(INF_DISABLED_COMPLETION_TIMER, rule)


            return safe_dict({
                "datetime": datetime.now().isoformat(),
                "running-time": running_time,
                "tasks-to_check": count
            })

        except ValueError as ex:
            self._logger.error("{}\n{}".format(ex, safe_json(self._event, indent=2)))

        finally:
            self._logger.flush()
