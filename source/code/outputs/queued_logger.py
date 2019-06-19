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
import time
import uuid
from datetime import datetime

import boto_retry
import handlers
import services
from metrics import put_general_errors_and_warnings
from outputs import get_extended_info
from outputs.issues_topic import IssuesTopic

ERR_QUEUE_FOR_LOGGING = "Can not send the following entries to queue {} for logging in in group {} stream {}, {}"

LOG_FORMAT = "{:0>4d}-{:0>2d}-{:0>2d} - {:0>2d}:{:0>2d}:{:0>2d}.{:0>3s} - {:7s} : {}"

ENV_LOG_GROUP = "LOG_GROUP"
ENV_SUPPRESS_LOG_STDOUT = "SUPPRESS_LOG_TO_STDOUT"
ENV_LOGGING_QUEUE_URL = "LOGGING_QUEUE_URL"
ENV_CLOUDWATCH_TRIGGER_TABLE = "CLOUDWATCH_TRIGGER_TABLE"

LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_ERROR = "ERROR"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_DEBUG = "DEBUG"

LOG_MAX_BATCH_SIZE = 1048576
LOG_ENTRY_ADDITIONAL = 26


class QueuedLogger(object):
    """
    Wrapper class for CloudWatch logging with buffering and helper methods
    """

    def __init__(self, logstream, context, loggroup=None, buffersize=50, use_retries=True, debug=False):

        def get_loggroup(lambda_context):
            group = os.getenv(ENV_LOG_GROUP, None)
            if group is None:
                if lambda_context is None:
                    return None
                group = lambda_context.log_group_name
            return group

        self._logstream = logstream
        self._buffer_size = min(buffersize, 10000)
        self._context = context
        self._buffer = []
        self._debug = debug
        self._cached_size = 0
        self._client = None
        self._retries = use_retries
        self._loggroup = loggroup if loggroup is not None else get_loggroup(self._context)
        self._next_log_token = None
        self.issues_topic = IssuesTopic(log_group=self._loggroup, log_stream=self._logstream, context=context)
        self._trigger_table = os.getenv(ENV_CLOUDWATCH_TRIGGER_TABLE)

        put_general_errors_and_warnings(error_count=0, warning_count=0)

        self._sqs_client = None
        self._dynamodb_client = None

        self._num = 0

    def __enter__(self):
        """
        Returns itself as the managed resource.
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Writes all cached action items to dynamodb table when going out of scope
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.flush()

    def _emit(self, level, msg, extended_info, *args):

        self._num += 1
        s = msg if len(args) == 0 else msg.format(*args)
        t = time.time()
        dt = datetime.fromtimestamp(t)
        s = LOG_FORMAT.format(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                              dt.second, str(dt.microsecond)[0:3], level, s)

        log_msg = s
        if extended_info not in [None, {}]:
            log_msg = "{}\n{}".format(s, json.dumps(extended_info, indent=3))

        if self._trigger_table is None:
            print(log_msg)
            return log_msg

        if self._cached_size + (len(log_msg) + LOG_ENTRY_ADDITIONAL) > LOG_MAX_BATCH_SIZE:
            self.flush()

        self._cached_size += len(s) + LOG_ENTRY_ADDITIONAL

        if handlers.running_local(self._context) and str(os.getenv(ENV_SUPPRESS_LOG_STDOUT, False)).lower() != "true":
            print("> " + log_msg)
        self._buffer.append((long(t * 1000), log_msg, self._num))

        if len(self._buffer) >= self._buffer_size:
            self.flush()

        return s

    @property
    def dynamodb_client(self):
        if self._dynamodb_client is None:
            self._dynamodb_client = boto_retry.get_client_with_retries("dynamodb", methods=["put_item"], context=self._context)
        return self._dynamodb_client

    @property
    def sqs_client(self):
        if self._sqs_client is None:
            self._sqs_client = services.get_session().client("sqs")
        return self._sqs_client

    @property
    def debug_enabled(self):
        """
        Return debug on/off switch
        :return: debug on/of
        """
        return self._debug

    @debug_enabled.setter
    def debug_enabled(self, value):
        """
        Sets debug switch
        :param value: True to enable debugging, False to disable
        :return:
        """
        self._debug = value

    def info(self, msg, *args):
        """
        Logs informational message
        :param msg: Message format string
        :param args: Message parameters
        :return:
        """

        self._emit(LOG_LEVEL_INFO, msg, None, *args)

    def error(self, msg, *args):
        """
        Logs error message
        :param msg: Error message format string
        :param args: parameters
        :return:
        """
        ext_error_info = get_extended_info(msg, "ERR")
        s = self._emit(LOG_LEVEL_ERROR, msg, ext_error_info, *args)
        self.issues_topic.publish("Error", s, ext_error_info)
        put_general_errors_and_warnings(error_count=1)

    def warning(self, msg, *args):
        """
        Logs warning message
        :param msg: Warning message format string
        :param args: parameters
        :return:
        """
        ext_warn_info = get_extended_info(msg, "WARN")
        s = self._emit(LOG_LEVEL_WARNING, msg, ext_warn_info, *args)
        self.issues_topic.publish("Warning", s, ext_warn_info)
        put_general_errors_and_warnings(warning_count=1)

    def debug(self, msg, *args):
        """
        Conditionally logs debug message, does not log if debugging is disabled
        :param msg: Debug message format string
        :param args: parameters
        :return:
        """
        if self._debug:
            self._emit(LOG_LEVEL_DEBUG, msg, None, *args)

    def clear(self):
        """
        Clear all buffered error messages
        :return:
        """
        self._buffer = []

    def flush(self):
        """
        Writes all buffered messages to CloudWatch Stream
        :return:
        """

        def trigger_process_queued_entries_execution():

            self.dynamodb_client.put_item_with_retries(
                TableName=self._trigger_table,
                Item={
                    "Name": {
                        'S': 'CloudWatchLogs'},
                    "LastCall": {
                        # remove seconds so Lambda is only triggered once per second max
                        "S": datetime.now().replace(microsecond=0).isoformat()
                    }
                }
            )

        did_write_to_queue = False
        # only write (and possible create stream if there is anything to log
        if len(self._buffer) == 0 or self._trigger_table is None:
            return

        queue_url = os.getenv(ENV_LOGGING_QUEUE_URL)
        fifo = queue_url.lower().endswith("fifo")
        try:
            i = 0
            queue_entries = []
            for entry in self._buffer:
                entry_id = str(uuid.uuid4())
                i += 1
                entry = {
                    "Id": entry_id,
                    "MessageBody": entry[1],
                    "DelaySeconds": 0,
                    "MessageAttributes": {
                        "stream": {
                            "StringValue": self._logstream,
                            "DataType": "String"
                        },
                        "timestamp": {
                            "StringValue": str(entry[0]),
                            "DataType": "String"
                        },
                        "number": {
                            "StringValue":
                                "{:0>4d}".format(entry[2]),
                            "DataType": "String"

                        }
                    }
                }
                if fifo:
                    entry["MessageGroupId"] = self._logstream[0:128]
                    entry["MessageDeduplicationId"] = entry_id

                queue_entries.append(entry)

                if len(queue_entries) == 10:
                    self.sqs_client.send_message_batch(QueueUrl=queue_url, Entries=queue_entries)
                    did_write_to_queue = True
                    queue_entries = []

            if len(queue_entries) > 0:
                self.sqs_client.send_message_batch(QueueUrl=queue_url, Entries=queue_entries)
                did_write_to_queue = True

        except Exception as ex:
            print("Error writing to queue {}, {}".format(queue_url, ex))
            for entry in self._buffer:
                print (entry)
        finally:
            try:
                if did_write_to_queue:
                    if len(self._buffer) < 10:
                        time.sleep(2)
                    trigger_process_queued_entries_execution()
                self._buffer = []
            except Exception as ex:
                print("Error triggering logging {}", ex)
