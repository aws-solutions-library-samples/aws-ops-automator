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
############################################################################################################################################################################################################################################

import os
import random
import types
from datetime import datetime
from time import time

import botocore.config

import services
from .aws_service_retry import AwsApiServiceRetry
from .dynamodb_service_retry import DynamoDbServiceRetry
from .ec2_service_retry import Ec2ServiceRetry
from .logs_service_retry import CloudWatchLogsServiceRetry

DEFAULT_SUFFIX = "_with_retries"
DEFAULT_WAIT_SECONDS = 10
DEFAULT_MAX_WAIT = 60
DEFAULT_RANDOM_FACTOR = 0.25

MAX_WAIT = 24 * 3600

EXPECTED_EXCEPTIONS = "_expected_boto3_exceptions_"

STATS_FORMAT = "{}: , calls: {}, failed: {}, retries: {}, timed-out {}"
LOG_FORMAT = "{:0>4d}-{:0>2d}-{:0>2d} - {:0>2d}:{:0>2d}:{:0>2d}.{:0>3s} {}, retry: {}"
ENV_BOTO_RETRY_STATS = "BOTO_RETRY_STATS"
ENV_BOTO_STATS_OUTPUT = "BOTO_RETRY_OUTPUT"
ENV_USER_AGENT = "USER_AGENT"

stats_enabled = False

boto_retry_stats = str(os.getenv(ENV_BOTO_RETRY_STATS, "false")).lower() == "true" or stats_enabled
boto_stats_output = str(os.getenv(ENV_BOTO_STATS_OUTPUT, "false")).lower() == "true"

statistics = {}


def make_method_with_retries(boto_client_or_resource, name, service_retry_strategy=None, method_suffix=DEFAULT_SUFFIX):
    """
    Creates a wrapper for a boto3 method call that handles boto_retry in case of an exception from which
    it can recover. Situations in which case this is possible are defined in the service specific 
    service_retry_strategy class
    :param boto_client_or_resource: boto client or resource to add method to
    :param name: Name of the boto call
    :param service_retry_strategy: Strategy that implements the logic that determines if boto_retry are possible 
    in case of an exception
    :param method_suffix: suffix for wrapped boto method
    :return: 
    """

    # default strategy
    retry_strategy = service_retry_strategy if service_retry_strategy is not None else AwsApiServiceRetry()
    # new method name
    method_name = name + method_suffix

    # closure function
    def wrapped_api_method(client_or_resource, **args):
        return retry_strategy.call(client_or_resource, name, args)

    # add closure function to the client or resource
    # noinspection PyArgumentList
    setattr(boto_client_or_resource, method_name, types.MethodType(wrapped_api_method, boto_client_or_resource))

    # return the method, but it can also be called directly as method of the boto client
    return wrapped_api_method


def get_default_wait_strategy(service):
    """
    Returns the default wait strategy for a service
    :param service:  service name
    :return: Default wait strategy
    """

    if service == "logs":
        return MultiplyWaitStrategy(start=2, factor=2, max_wait=15, random_factor=DEFAULT_RANDOM_FACTOR)

    return MultiplyWaitStrategy(start=DEFAULT_WAIT_SECONDS, max_wait=DEFAULT_MAX_WAIT, random_factor=DEFAULT_RANDOM_FACTOR)


def get_default_retry_strategy(service, wait_strategy=None, context=None, logger=None):
    if wait_strategy is None:
        wait_strategy = get_default_wait_strategy(service)
    service_retry_strategy_class = _get_service_retry_strategy_class(service)
    strategy = service_retry_strategy_class(wait_strategy=wait_strategy, context=context, logger=logger)
    return strategy


def _get_service_retry_strategy_class(service):
    """
    Returns the default wait strategy class for a service
    :param service: Name of the service
    :return: Class that implements the default strategy for a service
    """
    if service == "ec2":
        retry_class = Ec2ServiceRetry
    elif service == "dynamodb":
        retry_class = DynamoDbServiceRetry
    elif service == "logs":
        retry_class = CloudWatchLogsServiceRetry
    else:
        retry_class = AwsApiServiceRetry

    return retry_class


def get_client_with_retries(service_name, methods, context=None, region=None, session=None, wait_strategy=None,
                            method_suffix=DEFAULT_SUFFIX, logger=None):
    args = {
        "service_name": service_name,
    }

    if region is not None:
        args["region_name"] = region

    user_agent = os.getenv(ENV_USER_AGENT, None)
    if user_agent is not None:
        session_config = botocore.config.Config(user_agent=user_agent)
        args["config"] = session_config

    if session is not None:
        aws_session = session
    else:
        aws_session = services.get_session()

    result = aws_session.client(**args)

    # get strategy for the service
    service_retry_strategy = get_default_retry_strategy(context=context, service=service_name,
                                                        wait_strategy=wait_strategy, logger=logger)

    # add a new method to the client instance that wraps the original method with service specific retry logic
    for method in methods:
        make_method_with_retries(boto_client_or_resource=result,
                                 name=method,
                                 service_retry_strategy=service_retry_strategy,
                                 method_suffix=method_suffix)
    return result


def add_retry_methods_to_resource(resource, methods, context=None, method_suffix=DEFAULT_SUFFIX):
    """
    Adds new methods to a boto3 resource that wrap the original methods with retry logic.
    :param resource: Boto3 resource
    :param methods: List of methods for which a new method will be added to the client wrapped in retry logic
    :param context: Lambda execution context
    :param method_suffix: 
    :return: Suffix to add to the methods with retry logic that are added to the client, use none for DEFAULT_SUFFIX
    """
    # get name of the service and get the default strategy for that service
    service_name = type(resource).__name__.split(".")[0]
    service_retry_strategy_class = _get_service_retry_strategy_class(service_name)
    retry_wait_strategy = get_default_wait_strategy(service_name)

    # add a new method to the resource instance that wraps the original method with service specific retry logic
    for method in methods:
        make_method_with_retries(boto_client_or_resource=resource,
                                 name=method,
                                 method_suffix=method_suffix,
                                 service_retry_strategy=service_retry_strategy_class(
                                     wait_strategy=retry_wait_strategy,
                                     context=context)
                                 )
    return resource


def _apply_randomness(value, random_factor):
    """
    Applies a random factor to the value
    :param value: Input value
    :param random_factor: Random factor, must be between 0 (no random) and 1 (output is between 0 and 2* value)
    :return: Value with random factor applied
    """
    if random_factor < 0 or random_factor > 1:
        raise ValueError("Random factor must be in range 0 to 1")
    return value + (random.uniform(random_factor * -1, random_factor) * value) if random_factor != 0 else value


class WaitStrategy(object):
    """
    Implements wait strategy with defined wait
    """

    def __init__(self, waits, random_factor=0):
        """
        Initializes constant wait strategy
        :param waits: list of wait waits
        """
        self.waits = waits
        self.random_factor = random_factor
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns next wait period
        :return: Next wait period
        """
        if self._index < len(self.waits):
            val = self.waits[self._index]
            self._index += 1
            return _apply_randomness(val, self.random_factor)
        raise StopIteration

    def reset(self):
        """
        Resets wait strategy (
        :return: 
        """
        self._index = 0


class ConstantWaitStrategy(object):
    """
    Implements wait strategy with constant wait waits [step,step,step...]
    """

    def __init__(self, step=DEFAULT_WAIT_SECONDS, random_factor=0):
        """
        Initializes constant wait strategy
        :param step: wait interval
        """
        self.step = step
        self.random_factor = random_factor

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns next wait period
        :return: Next wait period
        """
        return _apply_randomness(self.step, self.random_factor)

    @classmethod
    def reset(cls):
        """
        Resets wait strategy (No action for this strategy)
        :return: 
        """
        pass


class LinearWaitStrategy(object):
    """
    Implements wait strategy with incrementing wait waits [start, start+incr, start+incr+incr..., max_wait]
    """

    def __init__(self, start=DEFAULT_WAIT_SECONDS, incr=DEFAULT_WAIT_SECONDS, max_wait=MAX_WAIT, random_factor=0.0):
        """
        Initializes Linear wait strategy implementation
        :param start: First wait period
        :param incr: Wait period increment
        :param max_wait: Max wait period
        """
        self.start = start
        self.incr = incr
        self.max_wait = max_wait
        self.random_factor = random_factor
        self._val = start

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns next wait period
        :return: Next wait period
        """
        val = self._val
        self._val = min(self._val + self.incr, self.max_wait)
        return _apply_randomness(val, self.random_factor)

    def reset(self):
        """
        Reset wait period to start wait period
        :return: 
        """
        self._val = self.start


class MultiplyWaitStrategy(object):
    """
    Implements wait strategy with multiplied wait waits [start, start* factor, start*factor*factor..., max_wait]
    """

    def __init__(self, start=DEFAULT_WAIT_SECONDS, factor=2, max_wait=MAX_WAIT, random_factor=0.0):
        """
        Initializes Multiply wait strategy
        :param start: Start wait period
        :param factor: Wait period multiply factor
        :param max_wait: Max wait period
        """
        self.start = start
        self.factor = factor
        self.max_wait = max_wait
        self.random_factor = random_factor
        self._val = start

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns next wait period
        :return: Next wait period
        """
        val = self._val
        self._val = min(self._val * self.factor, self.max_wait)
        return _apply_randomness(val, self.random_factor)

    def reset(self):
        self._val = self.start


def update_calls(client_or_resource, method_name, retry):
    if boto_retry_stats:
        dt = datetime.fromtimestamp(time())
        full_name = "{}.{}".format(type(client_or_resource).__name__, method_name)
        if boto_stats_output:
            print((LOG_FORMAT.format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, str(dt.microsecond)[0:3], full_name,
                                    retry)))
        if method_name in statistics:
            statistics[full_name]["calls"] += 1
        else:
            statistics[full_name] = {"calls": 1, "retries": 0, "failed": 0, "timed-out": 0}


def update_retries(client_or_resource, method_name, failed, retries, timed_out):
    if boto_retry_stats:
        full_name = "{}.{}".format(type(client_or_resource).__name__, method_name)
        statistics[full_name]["retries"] += retries
        statistics[full_name]["failed"] += failed
        timed_out[full_name]["timed-out"] += 1 if timed_out else 0


def print_statistics():
    if boto_retry_stats and boto_stats_output:
        for name in sorted(statistics):
            print((STATS_FORMAT.format(name, statistics[name]["calls"], statistics[name]["failed"], statistics[name]["retries"],
                                      statistics["timed-out"])))


def clear_statistics():
    global statistics
    statistics = {}
