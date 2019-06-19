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
from boto_retry import get_client_with_retries
from helpers import safe_json

MESSAGE_TYPE_ENDED = "task-ended"
MESSAGE_TYPE_STARTED = "task-started"

ERR_SEND_NOTIFICATION = "Cannot send notification to topic {}, {}"

ENV_RESULT_TOPIC = "SNS_RESULT_TOPIC_ARN"
MAX_SIZE = 262143


class ResultNotifications(object):

    def __init__(self, context, logger):
        self._sns_client = None
        self._context = context
        self._logger = logger

    @property
    def sns_client(self):
        if self._sns_client is None:
            self._sns_client = get_client_with_retries("sns", methods=["publish"], context=self._context)
        return self._sns_client

    @property
    def topic_arn(self):
        return os.getenv(ENV_RESULT_TOPIC, None)

    @classmethod
    def _build_common_attributes(cls, task):
        message = {a: task.get(a, "") for a in [
            handlers.TASK_TR_ID,
            handlers.TASK_TR_NAME,
            handlers.TASK_TR_ACTION,
            handlers.TASK_TR_ACCOUNT,
            handlers.TASK_TR_RESOURCES,
            handlers.TASK_TR_PARAMETERS
        ]}

        message["Time"] = datetime.now().isoformat()
        return message

    def _publish(self, message):
        self.sns_client.publish_with_retries(TopicArn=self.topic_arn, Message=safe_json(message)[0:MAX_SIZE])

    def publish_started(self, task):
        try:
            if task.get(handlers.TASK_TR_NOTIFICATIONS, False):
                message = self._build_common_attributes(task)
                message["Type"] = MESSAGE_TYPE_STARTED
                self._publish(message)
        except Exception as ex:
            self._logger.error(ERR_SEND_NOTIFICATION, self.topic_arn, ex)

    def publish_ended(self, task):
        try:
            if task.get(handlers.TASK_TR_NOTIFICATIONS, False):
                message = self._build_common_attributes(task)
                message["Type"] = MESSAGE_TYPE_ENDED
                message[handlers.TASK_TR_STATUS] = task.get(handlers.TASK_TR_STATUS, "")
                if task[handlers.TASK_TR_STATUS] == handlers.STATUS_COMPLETED:
                    message[handlers.TASK_TR_RESULT] = task.get(handlers.TASK_TR_RESULT)
                else:
                    message[handlers.TASK_TR_ERROR] = task.get(handlers.TASK_TR_ERROR, "")
                self._publish(message)
        except Exception as ex:
            self._logger.error(ERR_SEND_NOTIFICATION, self.topic_arn, ex)
