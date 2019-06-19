import os

import boto_retry
from helpers import safe_json

ENV_SNS_ISSUE_TOPIC = "SNS_ISSUES_TOPIC_ARN"


class IssuesTopic(object):

    def __init__(self, log_group, log_stream, context):
        self._sns_client = None
        self._loggroup = log_group
        self._logstream = log_stream
        self._context = context

    @property
    def sns_client(self):
        if self._sns_client is None:
            self._sns_client = boto_retry.get_client_with_retries("sns", ["publish"], context=self._context)
        return self._sns_client

    def publish(self, level, msg, ext_info):

        sns_arn = os.getenv(ENV_SNS_ISSUE_TOPIC, None)
        if sns_arn is not None:
            message = {
                "log-group": self._loggroup,
                "log-stream": self._logstream,
                "level": level,
                "message": msg
            }
            if ext_info not in [None, {}]:
                for i in ext_info:
                    message[i.lower()] = ext_info[i]

            topic_msg = safe_json({"default": safe_json(message, indent=3), "lambda": message})
            resp = self.sns_client.publish_with_retries(TopicArn=sns_arn,
                                                        Message=topic_msg,
                                                        MessageStructure="json")
            print(resp)
