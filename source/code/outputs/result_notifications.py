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
        """
        Initialize the context.

        Args:
            self: (todo): write your description
            context: (str): write your description
            logger: (todo): write your description
        """
        self._sns_client = None
        self._context = context
        self._logger = logger

    @property
    def sns_client(self):
        """
        Return an sns client.

        Args:
            self: (todo): write your description
        """
        if self._sns_client is None:
            self._sns_client = get_client_with_retries("sns", methods=["publish"], context=self._context)
        return self._sns_client

    @property
    def topic_arn(self):
        """
        Return the environment variable.

        Args:
            self: (todo): write your description
        """
        return os.getenv(ENV_RESULT_TOPIC, None)

    @classmethod
    def _build_common_attributes(cls, task):
        """
        Builds a dictionary of common attributes.

        Args:
            cls: (todo): write your description
            task: (dict): write your description
        """
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
        """
        Publish a message.

        Args:
            self: (todo): write your description
            message: (str): write your description
        """
        self.sns_client.publish_with_retries(TopicArn=self.topic_arn, Message=safe_json(message)[0:MAX_SIZE])

    def publish_started(self, task):
        """
        Publish a message.

        Args:
            self: (todo): write your description
            task: (dict): write your description
        """
        try:
            if task.get(handlers.TASK_TR_NOTIFICATIONS, False):
                message = self._build_common_attributes(task)
                message["Type"] = MESSAGE_TYPE_STARTED
                self._publish(message)
        except Exception as ex:
            self._logger.error(ERR_SEND_NOTIFICATION, self.topic_arn, ex)

    def publish_ended(self, task):
        """
        Publish a message.

        Args:
            self: (todo): write your description
            task: (dict): write your description
        """
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
