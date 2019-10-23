import collections
import os
import time
from datetime import datetime

import boto3

MIN_REMAINING_EXEC_TIME_SEC = 60

LOG_MAX_BATCH_SIZE = 1048576
LOG_ENTRY_ADDITIONAL = 26
VERSION = "%version%"


class Throttle(object):

    def __init__(self, max_calls, per_number_of_seconds=1):
        self.max_calls = max_calls
        self.per_number_of_seconds = per_number_of_seconds
        self.calls = list()

    def check(self):

        while len(self.calls) >= self.max_calls:
            while len(self.calls) > 0:
                if self.calls[0] < time.time() - self.per_number_of_seconds:
                    self.calls.pop(0)
                else:
                    break
                if len(self.calls) < self.max_calls:
                    break
        self.calls.append(time.time())


CLOUDWATCH_LOGS_KEY = "CloudWatchLogs"

ENV_LOG_GROUP = "LOG_GROUP"

ENV_CWL_LIMIT_PUT_CALLS_PER_STREAM = "CWL_LIMIT_PUT_CALLS_PER_STREAM"
DEFAULT_CWL_LIMIT_PUT_CALLS_PER_STREAM = 5
ENV_CWL_LIMIT_PUT_CALLS_PER_ACCOUNT = "CWL_LIMIT_PUT_CALLS_PER_ACCOUNT"
DEFAULT_CWL_LIMIT_PUT_CALLS_PER_ACCOUNT = 800
ENV_CWL_LIMIT_API_CALLS = "CWL_LIMIT_API_CALLS"
DEFAULT_CWL_LIMIT_API_CALLS = 40

ENV_LOGGING_QUEUE_URL = "LOGGING_QUEUE_URL"
ENV_CLOUDWATCH_TRIGGER_TABLE = "CLOUDWATCH_TRIGGER_TABLE"


class LogHandler(object):

    def __init__(self):

        self.log_group = os.environ[ENV_LOG_GROUP]

        max_put_calls_per_account = int(os.getenv(ENV_CWL_LIMIT_PUT_CALLS_PER_ACCOUNT, DEFAULT_CWL_LIMIT_PUT_CALLS_PER_ACCOUNT))
        self._max_put_call_account_throttling = Throttle(max_put_calls_per_account)

        self.max_put_calls_per_stream = int(os.getenv(ENV_CWL_LIMIT_PUT_CALLS_PER_STREAM, DEFAULT_CWL_LIMIT_PUT_CALLS_PER_STREAM))
        self._max_put_call_stream_throttling = {}

        max_api_calls = int(os.getenv(ENV_CWL_LIMIT_API_CALLS, DEFAULT_CWL_LIMIT_API_CALLS))
        self._max_cwl_api_calls = Throttle(max_api_calls)

        self._log_client = boto3.client("logs")

        self._stream_tokens = {}

        self._buffer = collections.OrderedDict()
        self._buffer_size = 0
        self.fifo = None

    @property
    def streams_used(self):
        return self._stream_tokens

    def _create_log_stream(self, log_stream):
        self._max_cwl_api_calls.check()
        try:
            print(("Creating log stream {}".format(log_stream)))
            self._log_client.create_log_stream(logGroupName=self.log_group, logStreamName=log_stream)
            self._stream_tokens[log_stream] = "0"
        except Exception as e:
            # if the stream was created in between the call ignore the error
            if type(e).__name__ != "ResourceAlreadyExistsException":
                raise e

    def add_message(self, stream_name, timestamp, message, number):

        if self._buffer_size + (len(message) + LOG_ENTRY_ADDITIONAL) > LOG_MAX_BATCH_SIZE:
            self.flush()

        if stream_name not in self._buffer:
            self._buffer[stream_name] = [(timestamp, message, number)]
        else:
            self._buffer[stream_name].append((timestamp, message, number))

        self._buffer_size += (len(message) + LOG_ENTRY_ADDITIONAL)

    def _check_stream_throttle(self, log_stream):
        if log_stream not in self._max_put_call_stream_throttling:
            self._max_put_call_stream_throttling[log_stream] = Throttle(self.max_put_calls_per_stream)
        self._max_put_call_stream_throttling[log_stream].check()

    def flush(self):

        for log_stream in self._buffer:

            self._buffer[log_stream] = sorted(self._buffer[log_stream], key=lambda e: (e[0], e[2]))

            put_event_args = {
                "logGroupName": self.log_group,
                "logStreamName": log_stream,
                "logEvents": [{"timestamp": r[0], "message": r[1]} for r in self._buffer[log_stream]]
            }

            try:
                retries = 0
                while True:
                    # get the token and use it if the stream was used before
                    next_token = self._stream_tokens.get(log_stream)
                    if next_token is not None:
                        put_event_args["sequenceToken"] = next_token
                    try:
                        # throttle if making call to a stream too rapidly
                        self._check_stream_throttle(log_stream)
                        resp = self._log_client.put_log_events(**put_event_args)
                        self._stream_tokens[log_stream] = resp.get("nextSequenceToken", None)
                        break
                    except Exception as ex:
                        exception_type = type(ex).__name__
                        # stream did not exist, in that case create it and try again with token set in create method
                        if exception_type == "ResourceNotFoundException":
                            self._create_log_stream(log_stream=log_stream)
                        # stream did exist but need new token, get it from exception data
                        elif exception_type in ["InvalidSequenceTokenException", "DataAlreadyAcceptedException"]:
                            # noinspection PyBroadException
                            try:
                                token = ex.message.split(":")[-1].strip()
                                self._stream_tokens[log_stream] = ex.message.split(":")[-1].strip()
                                print(("Token for existing stream {} is {}".format(log_stream, token)))
                            except:
                                self._stream_tokens[log_stream] = None
                        else:
                            # other exceptions retry
                            print(("Error logstream {}, {}".format(log_stream, str(ex))))
                            time.sleep(1)
                            retries += 1
                            if retries > 10:
                                raise ex

            except Exception as ex:
                msg = "Can not write the following entries to log stream {} in group {}, {}".format(self.log_group, log_stream, ex)
                print(msg)
                for entry in self._buffer[log_stream]:
                    print((str(entry)))

        self._buffer.clear()
        self._buffer_size = 0


def lambda_handler(event, context):
    def trigger_next_execution():

        boto3.client("dynamodb").put_item(
            TableName=os.getenv(ENV_CLOUDWATCH_TRIGGER_TABLE),
            Item={
                "Name": {
                    'S': 'CloudWatchLogs'},
                "LastCall": {
                    "S": datetime.now().isoformat()
                }
            }
        )

    print(("CloudWatch Queue handler version {}".format(VERSION)))
    if event != {}:
        keys = [r.get("dynamodb", {}).get("Keys", {}).get("Name", {}).get("S", "") for r in event.get("Records", [])]
        if not any([n == CLOUDWATCH_LOGS_KEY for n in keys]):
            return

    log_messages_written = {}

    log_handler = LogHandler()

    sqs = boto3.client("sqs")

    queue = os.getenv(ENV_LOGGING_QUEUE_URL, "")

    # as this lambda is called once per seconf wait until queued messages for that second are there
    time.sleep(1)
    print(("Reading from logging queue {}".format(queue)))

    while True:
        if context is not None:
            remaining = context.get_remaining_time_in_millis() / 1000
            if remaining < MIN_REMAINING_EXEC_TIME_SEC:
                print(("{} seconds left, triggering follow up execution to process remaining entries from queue".format(
                    MIN_REMAINING_EXEC_TIME_SEC)))
                trigger_next_execution()
                break
        else:
            remaining = MIN_REMAINING_EXEC_TIME_SEC

        resp = sqs.receive_message(QueueUrl=queue, AttributeNames=[], MessageAttributeNames=["All"], MaxNumberOfMessages=10,
                                   VisibilityTimeout=int(remaining), WaitTimeSeconds=15)
        log_messages = resp.get('Messages', [])
        if len(log_messages) == 0:
            break
        for log_message in log_messages:
            try:
                stream_name = log_message["MessageAttributes"]["stream"]["StringValue"]
                timestamp = int(log_message["MessageAttributes"]["timestamp"]["StringValue"])
                number = log_message["MessageAttributes"]["number"]["StringValue"]
                if stream_name in log_messages_written:
                    log_messages_written[stream_name] += 1
                else:
                    log_messages_written[stream_name] = 1
                log_handler.add_message(stream_name, timestamp, log_message["Body"], number)
            except Exception as ex:
                print(("Invalid log message {}, {}".format(log_message, ex)))
        try:
            log_handler.flush()
            sqs.delete_message_batch(QueueUrl=queue,
                                     Entries=[{"Id": log_message["MessageId"], "ReceiptHandle": log_message["ReceiptHandle"]} for
                                              log_message in log_messages])
        except Exception as ex:
            print(("Error writing messages to CloudWatch Logs , {}".format(ex)))
            raise ex

    for s in log_messages_written:
        print(("{} entries written to log stream {}".format(log_messages_written[s], s)))

    return {s: log_messages_written[s] for s in log_messages_written}


if __name__ == '__main__':
    lambda_handler({}, None)
