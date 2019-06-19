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
import copy
from time import sleep, time

from botocore.exceptions import ClientError

import boto_retry
from helpers import full_stack, safe_json


class AwsApiServiceRetry(object):
    """
    Generic AWS retry logic for calling AWS API using the boto3 api
    """

    def __init__(self, call_retry_strategies=None, logger=None, wait_strategy=None, context=None, timeout=300,
                 lambda_time_out_margin=20):
        self.default_strategies = [
            self.api_throttled,
            self.server_error,
            self.read_timeout,
            self.reset_by_peer
        ]

        self._call_retry_strategies = call_retry_strategies if call_retry_strategies else self.default_strategies
        self._wait_strategy = wait_strategy if wait_strategy else boto_retry.ConstantWaitStrategy()
        self._timeout = timeout
        self.context = context
        self.logger = logger
        self._lambda_time_out_margin = lambda_time_out_margin

    @classmethod
    def api_throttled(cls, ex):
        """
        Tests if the API call was throttled
        :param ex: 
        :return: 
        """
        return "throttling" in ex.message.lower()

    @classmethod
    def server_error(cls, ex):
        """
        Tests if the service was temporary not available
        :param ex: 
        :return: 
        """
        if type(ex) == ClientError:
            return False

        response = getattr(ex, "response", {})
        metadata = response.get("ResponseMetaData", {})
        return metadata.get("HTTPStatusCode", 0) in [500, 502, 503, 504]

    @classmethod
    def read_timeout(cls, ex):
        """
        Tests if the service was temporary not available
        :param ex:
        :return:
        """
        return "The read operation timed out".lower() in str(ex).lower()

    @classmethod
    def reset_by_peer(cls, ex):
        """
        Tests if the service was temporary not available
        :param ex:
        :return:
        """
        return "Connection reset by peer".lower() in str(ex).lower()

    def _log_failed_call_exception(self, name, arguments, exception, retry_wait=0):
        """
        Logs all details of failed boto3 calls
        :param name: Name of the method
        :param arguments: arguments used in the call
        :param exception: raised exception
        :param retry_wait period for retry  if can be retried
        :return:
        """

        # logger information, method and arguments
        s = "Boto3 method call: {}\nArguments:\n{}\n".format(name, safe_json(arguments, indent=3))
        d = getattr(exception, "__dict__", None)
        if d is not None:
            s += safe_json(d, indent=3) + "\n"

        if retry_wait == 0:
            # exception and stack trace
            s += full_stack()
        else:
            s += "Call will be retried in {} seconds".format(retry_wait)

        if self.logger is None:
            print(s)
        else:
            try:
                if retry_wait == 0:
                    self.logger.error(s)
                else:
                    self.logger.warning(s)
            except Exception as ex:
                print("Logging to stream failed, {}".format(ex))
                print(s)
            finally:
                if self.logger is not None:
                    self.logger.flush()

    def can_retry(self, ex):
        """
        Tests if a retry can be done based on the exception of an earlier call
        :param ex: Execution raise by earlier call of the boto3 method
        :return: True if any of the call_retry_strategy returns True, else False
        """
        return any([rt(ex) for rt in self._call_retry_strategies])

    def call(self, boto_client_or_resource, method_name, call_arguments):
        """
        Calls the original boto3 methods that is wrapped in the retry logic
        :param boto_client_or_resource: Boto3 client or resource instance
        :param method_name: Name of the wrapped method with retries
        :param call_arguments: Boto3 method parameters
        :return: result of the wrapped boto3 method
        """

        def timed_out_by_specified_timeout(start_time, time_now, next_wait):
            if self._timeout is None:
                return False

            return (time_now - start_time) > (self._timeout - next_wait)

        def timed_out_by_lambda_timeout(next_wait):
            if self.context is None:
                return False

            context_seconds_left = self.context.get_remaining_time_in_millis() * 1000
            return context_seconds_left < (self._lambda_time_out_margin + next_wait)

        start = time()
        # gets the method with the retry logic
        method = getattr(boto_client_or_resource, method_name)

        # reset wait time strategy
        self._wait_strategy.reset()

        expected_exceptions = []
        if boto_retry.EXPECTED_EXCEPTIONS not in call_arguments:
            used_call_arguments = call_arguments
        else:
            expected_exceptions = call_arguments[boto_retry.EXPECTED_EXCEPTIONS]
            used_call_arguments = copy.deepcopy(call_arguments)
            del used_call_arguments[boto_retry.EXPECTED_EXCEPTIONS]

        stats_retries = 0
        stats_timed_out = False
        stats_failed = 0
        self._wait_strategy.reset()
        try:
            for wait_until_next_retry in self._wait_strategy:
                try:
                    # make the "wrapped" call
                    boto_retry.update_calls(boto_client_or_resource, method_name, stats_retries)
                    resp = method(**used_call_arguments)
                    # no exceptions, just return result
                    return resp
                except Exception as ex:
                    # is this an exception we expect then raise it without retries
                    if type(ex).__name__ in expected_exceptions or \
                            getattr(ex, "response", {}).get("Error", {}).get("Code", "") in expected_exceptions:
                        raise ex
                    # there was an exception
                    now = time()
                    # test if there should be a retry based on the type of the exception
                    if self.can_retry(ex):
                        stats_failed += 1
                        # test if there is enough time left for the next retry, if not raise the exception
                        if timed_out_by_specified_timeout(start, now, wait_until_next_retry) or \
                                timed_out_by_lambda_timeout(wait_until_next_retry):
                            stats_timed_out = True
                            self._log_failed_call_exception(method_name, call_arguments, ex)
                            raise Exception("Call {} timed out, last exception was {}".format(method_name, ex))
                        else:
                            stats_retries += 1
                            # else wait until next retry
                            self._log_failed_call_exception(method_name, call_arguments, ex, wait_until_next_retry)
                            sleep(wait_until_next_retry)
                            continue
                    else:
                        # No recovery for this type of exception
                        self._log_failed_call_exception(method_name, call_arguments, ex)
                        raise ex
        finally:
            if stats_retries > 0:
                boto_retry.update_retries(boto_client_or_resource, method_name, stats_failed, stats_retries, stats_timed_out)
