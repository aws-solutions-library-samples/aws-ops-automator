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


import importlib
import inspect
import sys
from datetime import datetime, timedelta
from os import getenv, listdir
from os.path import isfile, join

from boto_retry import get_client_with_retries
from scheduling.cron_expression import CronExpression
from util import pascal_to_snake_case

COMPLETION_RULE = "CompletionRule"
SCHEDULE_RULE = "SchedulerRule"
COMPLETION_METHOD = "is_completed"

# name of task tracking table
ENV_ACTION_TRACKING_TABLE = "ACTION_TRACKING_TABLE"
# name of table that holds moment of last scheduler execution
ENV_LAST_RUN_TABLE = "LAST_SCHEDULER_RUN_TABLE"
# name of the tag that holds the list of tasks to be applied to a resource
ENV_SCHEDULER_TAG_NAME = "SCHEDULER_TAG_NAME"
# name of the concurrency table
ENV_CONCURRENCY_TABLE = "CONCURRENCY_TABLE"
# name of the waiting for completion table
ENV_WAITING_COMPLETION_TABLE = "WAITING_COMPLETION_TABLE"
# name of the stack
ENV_STACK_NAME = "STACK_NAME"
# id of the stack
ENV_STACK_ID = "STACK_ID"
# name of the cloudwatch rule that triggers the scheduler for checking for time scheduled tasks
ENV_RULE_SCHEDULING = "SCHEDULER_RULE"
# name of the cloudwatch rule that triggers the scheduler for checking task completion
ENV_RULE_COMPLETION = "COMPLETION_RULE"

# Default tag for resource tasks
DFLT_SCHEDULER_TAG = "AutomationTasks"

# task attributes
TASK_ACTION = "action"
TASK_CROSS_ACCOUNT_ROLES = "cross_account_roles"
TASK_DEBUG = "debug"
TASK_DESRIPTION = "description"
TASK_DRYRUN = "dryrun"
TASK_ENABLED = "enabled"
TASK_EVENTS = "events"
TASK_INTERNAL = "internal"
TASK_INTERVAL = "interval"
TASK_NAME = "name"
TASK_PARAMETERS = "parameters"
TASK_REGIONS = "regions"
TASK_TAG_FILTER = "tag_filter"
TASK_THIS_ACCOUNT = "this_account"
TASK_TIMEZONE = "timezone"
TASK_TIMOUT = "timeout"

HANDLER_EVENT_ACTION = "action"
HANDLER_ACTION_EXECUTE = "execute-action"
HANDLER_ACTION_TEST_COMPLETION = "execute-test-completion"
HANDLER_ACTION_SELECT_RESOURCES = "select-resources"
HANDLER_SELECT_ARGUMENTS = "select-args"

HANDLER_EVENT_TASK_DT = "task-datetime"
HANDLER_EVENT_TASK = "task"
HANDLER_EVENT_ACCOUNT = "account"
HANDLER_EVENT_REGIONS = "regions"
HANDLER_EVENT_SOURCE = "source"
HANDLER_EVENT_DYNAMO_SOURCE = "eventSource"

UNKNOWN_SOURCE = "unknown"

ERR_NO_MODULE_FOR_HANDLER = "Can not load module {} for handler {}, available handlers are {}"
ERR_UNEXPECTED_HANDLER_CLASS_IN_MODULE = "Unable to load class {0}Handler for handler {0} from module {1}, " \
                                         "handler class found in module was {2}"
ERR_EVENT_RULE_NOT_FOUND = "Can not get name of CloudWatch rule {}-{} in stack"

DESC_NO_EXECUTIONS_FOR_EXPR = "No executions scheduled within the next 24 hours"
DESC_EXPRESSION_SET = "Schedule expression set to {}, next execution will be at {}"

HANDLERS = "handlers"
HANDLERS_MODULE_NAME = HANDLERS + ".{}"
HANDLERS_PATH = "./" + HANDLERS

HANDLER = "Handler"
HANDLER_CLASS = "{}" + HANDLER

__handlers = {}


def _get_handler_class(handler_module):
    """
    Gets the service class from the module using naming pattern, the name of the class must end with "Service"
    :param handler_module: The service class from the module, None if no service class was found
    :return:
    """
    for cls in inspect.getmembers(handler_module, inspect.isclass):
        if cls[1].__module__ != handler_module.__name__ or not cls[1].__name__.endswith(HANDLER):
            continue
        return cls
    return None


def _get_module(module_name):
    """
    Gets a module by its name
    :param module_name: Name of the module
    :return: The loaded module
    """
    handler_module = sys.modules.get(module_name)
    if handler_module is None:
        handler_module = importlib.import_module(module_name)
    return handler_module


def get_module_for_handler(handler_name):
    """
    Gets the module for a handler using naming convention. First the name of the handler is capitalized and appended by the
    string "Handler". Then it is converted from camel to snake case to get the name of the module that will be loaded. Raises an
    ImportError exception if no module is found for the constructed module name
    :param handler_name:
    :return:
    """

    module_name = HANDLERS_MODULE_NAME.format(pascal_to_snake_case(handler_name))
    try:
        return _get_module(module_name)
    except:
        raise ImportError(ERR_NO_MODULE_FOR_HANDLER.format(module_name, handler_name, ", ".join(all_handlers())))


def all_handlers():
    """
    Return as list of all supported handler names
    :return: list of all supported handler names
    """
    result = []
    for f in listdir(HANDLERS_PATH):
        if isfile(join(HANDLERS_PATH, f)) and f.endswith("_{}.py".format(HANDLER.lower())):
            module_name = HANDLERS_MODULE_NAME.format(f[0:-len(".py")])
            module = _get_module(module_name)
            cls = _get_handler_class(module)
            if cls is not None:
                handler_name = cls[0]
                result.append(handler_name)
    return result


def create_handler(handler_name, event, context):
    """
   Creates an instance of the class for the specified handler name. An ImportError exception is raises if there is no module
   that implements the class for the requested handler.
   :param handler_name: name of the handler
    :param event: Event to handle
    :param context: Lambda context
    :return: Instance of the class for the named handler
    """
    return get_class_for_handler(handler_name)(event, context)


def get_class_for_handler(handler_name):
    """
    Returns the class for the handler
    :param handler_name: Name of the handler
    :return: class that implements the handler
    """
    if handler_name not in __handlers:
        module = get_module_for_handler(handler_name)
        cls = _get_handler_class(module)
        if cls is None or cls[0] != handler_name:
            raise ImportError(ERR_UNEXPECTED_HANDLER_CLASS_IN_MODULE.format(handler_name, module, cls[0] if cls else "None"))
        __handlers[handler_name] = cls

    return __handlers[handler_name][1]


def _get_cloudwatch_rule(name, client):
    """
    Get the CloudWatch event rule with the name prefix that is the stack name + name  in the current stack
    :param name: part of the name that is added to the stack name to build the name prefix of the cloudwatch rule
    :param client: CloudWatch client

    :return: CloudWatch rules 
    """
    stack_name = getenv(ENV_STACK_NAME)
    resp = client.list_rules_with_retries(NamePrefix="{}-{}".format(stack_name, name))
    rules = resp.get("Rules", [])
    if len(rules) != 1:
        raise Exception(ERR_EVENT_RULE_NOT_FOUND.format(stack_name, name, stack_name))
    return rules[0]


def _get_cloudwatch_events_client(context):
    """
    Builds client for making boto3 CloudWatch rule api calls with retries
    :param context: 
    :return: 
    """
    return get_client_with_retries("events", ["enable_rule", "disable_rule", "list_rules", "put_rule"], context)


def enable_completion_cloudwatch_rule(context=None):
    """
    Enables the task completion CloudWatch rule
    :param context: Lambda execution context
    :return: Name of the Cloudwatch event that was enabled
    """
    events_client = _get_cloudwatch_events_client(context)
    event_rule_name = _get_cloudwatch_rule(COMPLETION_RULE, events_client)["Name"]
    events_client.enable_rule_with_retries(Name=event_rule_name)
    return event_rule_name


def disable_completion_cloudwatch_rule(context=None):
    """
      Disables the task completion CloudWatch rule
      :param context: Lambda execution context
      :return: Name of the Cloudwatch event that was disabled
      """
    events_client = _get_cloudwatch_events_client(context)
    event_rule_name = _get_cloudwatch_rule(COMPLETION_RULE, events_client)["Name"]
    events_client.disable_rule_with_retries(Name=event_rule_name)
    return event_rule_name


def set_scheduler_rule_every_minute():
    """
    Sets the scheduler CloudWatch Event rule to execute every minutes
    :return: 
    """
    _set_scheduler_cloudwatch_rule_expression("cron(0/1 * * * ? *)")


def set_event_for_time(next_time):
    """
    Sets the expression for a cloudwatch event rule to ready to execute at a specified time.
    Note that the time s actually set 5 minutes before the actual time. This is done because the rule may not
    happen if set in the same minute. A 5 minute safety period will also let the retry logic perform retries
    for the calls that does set the new expression 
    :param next_time: 
    :return: 
    """
    next_event_time = next_time - timedelta(minutes=5)
    cron = "cron({} {} * * ? *)".format(next_event_time.minute, next_event_time.hour)
    _set_scheduler_cloudwatch_rule_expression(cron)
    return next_event_time


def _set_scheduler_cloudwatch_rule_expression(expression, context=None):
    """
    Sets a new expression for a CloudWatch rule
    :param expression: new cloudwatch expression in the syntax cron(x x x x x x). Note that this format has an additional year
    field that is not used by the scheduler. 
    :param context: Lambda execution context
    :return: 
    """
    events_client = _get_cloudwatch_events_client(context)
    event_rule = _get_cloudwatch_rule(SCHEDULE_RULE, events_client)

    try:
        # get the con expression from the expression
        cron_str = " ".join(expression[expression.index("(") + 1:expression.index(")")].split(" ")[0:5])
        cron = CronExpression(cron_str)
        next_execution_time = cron.first_within_next(start_dt=datetime.utcnow(), timespan=timedelta(hours=24))
        if next_execution_time is not None:
            description = DESC_EXPRESSION_SET.format(expression, next_execution_time.isoformat())
        else:
            description = DESC_NO_EXECUTIONS_FOR_EXPR
    except ValueError:
        description = ""

    if event_rule["ScheduleExpression"] != expression:
        args = {
            "Name": event_rule["Name"],
            "ScheduleExpression": expression,
            "Description": description
        }
        events_client.put_rule_with_retries(**args)
