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


import base64
import copy
import importlib
import inspect
import json
import os
import sys
import time
from datetime import datetime, timedelta
from os import getenv, listdir
from os.path import abspath, dirname, isdir, isfile, join

import boto_retry
import pytz
import services
from configuration import ENV_CONFIG_BUCKET as CONFIGURATION_BUCKET
from helpers import pascal_to_snake_case, safe_json
from outputs import raise_exception
from scheduling.cron_expression import CronExpression

ARN_ROLE_TEMPLATE = "arn:aws:iam::{}:role/{}"

INF_NEW_CRON = "New cron expression for rule is {}"
INF_NEXT_EVENT_IN_PAST = "As next trigger time for rule is before {} it will be set to trigger every minute"
INF_NEXT_EVENT = "Next event is at {}, new time for rule to trigger is {}"

COMPLETION_METHOD = "is_completed"
# optional static method for actions that require concurrency control
ACTION_CONCURRENCY_KEY_METHOD = "action_concurrency_key"
# optional static method for actions to perform additional parameter checking
ACTION_VALIDATE_PARAMETERS_METHOD = "action_validate_parameters"

DEFAULT_ACCOUNT_ROLENAME = "{}ActionsRole"

ENV_LAMBDA_NAME = "LAMBDA_NAME"
# name of the Ops Automator Rule
ENV_OPS_AUTOMATOR_RULE = "OPS_AUTOMATOR_RULE"
# name of the Completion Rule
ENV_COMPLETION_RULE = "COMPLETION_RULE"
# name of task tracking table
ENV_ACTION_TRACKING_TABLE = "ACTION_TRACKING_TABLE"
# name of table that holds moment of last scheduler execution
ENV_LAST_RUN_TABLE = "LAST_SCHEDULER_RUN_TABLE"
# name of the tag that holds the list of tasks to be applied to a resource
ENV_AUTOMATOR_TAG_NAME = "TASKLIST_TAG_NAME"
# name of the concurrency table
ENV_CONCURRENCY_TABLE = "CONCURRENCY_TABLE"
# name of the waiting for completion table
ENV_WAITING_COMPLETION_TABLE = "WAITING_COMPLETION_TABLE"
# name of the stack
ENV_STACK_NAME = "STACK_NAME"
# id of the stack
ENV_STACK_ID = "STACK_ID"
# ops automator deployment account
ENV_OPS_AUTOMATOR_ACCOUNT = "OPS_AUTOMATOR_ACCOUNT"
# timeout for used lambda function
ENV_LAMBDA_TIMEOUT = "LAMBDA_TIMEOUT"
# arn of the topic for posting warning and errors
ENV_ISSUES_TOPIC_ARN = "ISSUES_TOPIC_ARN"
# arn of the topic for receiving events to be handled
ENV_EVENTS_TOPIC_ARN = "EVENTS_TOPIC_ARN"
# name of resource bucket
ENV_RESOURCE_BUCKET = "RESOURCE_BUCKET"
# max size for resource before storing them into S3
ENV_RESOURCE_TO_S3_SIZE = "RESOURCE_TO_S3_SIZE"
# Enable cloudwatch metrics
ENV_CLOUDWATCH_METRICS = "CLOUDWATCH_METRICS"
# use ECS for execution
ENV_USE_ECS = "USE_ECS"
# this environment variable is set to True if running as ECS job
ENV_IS_ECS_JOB = "IS_ECS_JOB"
# name of configuration bucket
ENV_CONFIG_BUCKET = CONFIGURATION_BUCKET
# ECS cluster name
ENV_ECS_CLUSTER = "ECS_CLUSTER"
# ECS repository name
ENV_ECS_TASK = "ECS_OPS_AUTOMATOR_TASK"
# Setting to delete tasks from tracking table using TTL
ENV_TASK_CLEANUP_ENABLED = "TASK_CLEANUP_ENABLED"
ENV_TASK_RETENTION_HOURS = "TASK_RETENTION_HOURS"
ENV_KEEP_FAILED_TASKS = "KEEP_FAILED_TASKS"
# concurrent number of snapshot and images copies
ENV_SERVICE_LIMIT_CONCURRENT_EBS_SNAPSHOT_COPY = "SERVICE_LIMIT_CONCURRENT_EBS_SNAPSHOT_COPY"
ENV_SERVICE_LIMIT_CONCURRENT_RDS_SNAPSHOT_COPY = "SERVICE_LIMIT_CONCURRENT_RDS_SNAPSHOT_COPY"
ENV_SERVICE_LIMIT_CONCURRENT_IMAGE_COPY = "SERVICE_LIMIT_CONCURRENT_IMAGE_COPY"
# ops automator role
ENV_OPS_AUTOMATOR_ROLE_ARN = "OPS_AUTOMATOR_ROLE_ARN"
# key use to encrypt resource data in DynamoDB and S3
ENV_RESOURCE_ENCRYPTION_KEY = "RESOURCE_ENCRYPTION_KEY"

# Default tag for resource tasks
DEFAULT_SCHEDULER_TAG = "OpsAutomatorTaskList"

FORWARDED_EVENT = "ops-automator:{}"

EC2_EVENT_SOURCE = "aws.ec2"
RDS_EVENT_SOURCE = "aws.rds"
S3_EVENT_SOURCE = "aws:s3"
TAG_EVENT_SOURCE = "aws.tag"

TAG_CHANGE_EVENT_SOURCE_DETAIL_TYPE = "Tag Change on Resource"
TAG_CHANGE_EVENT = "TagChangeOnResource"

ECS_DEFAULT_MEMORY_RESERVATION = 128

TASK_TR_ACCOUNT = "Account"
TASK_TR_ACTION = "Action"
TASK_TR_ASSUMED_ROLE = "AssumedRole"
TASK_TR_COMPLETION_SIZE = "CompletionSize"
TASK_TR_CONCURRENCY_ID = "ConcurrencyId"
TASK_TR_CONCURRENCY_KEY = "ConcurrencyKey"
TASK_TR_CREATED = "Created"
TASK_TR_CREATED_TS = "CreatedTs"
TASK_TR_DEBUG = "Debug"
TASK_TR_DRYRUN = "Dryrun"
TASK_TR_DT = "TaskDatetime"
TASK_TR_ENCRYPTED_RESOURCES = "EncryptedResources"
TASK_TR_ERROR = "Error"
TASK_TR_EVENTS = "Events"
TASK_TR_EXECUTE_SIZE = "ExecuteSize"
TASK_TR_EXECUTION_LOGSTREAM = "LogStream"
TASK_TR_EXECUTION_TIME = "ExecutionTime"
TASK_TR_HAS_COMPLETION = "HasCompletion"
TASK_TR_ID = "Id"
TASK_TR_INTERNAL = "Internal"
TASK_TR_INTERVAL = "Interval"
TASK_TR_GROUP = "TaskGroup"
TASK_TR_LAST_WAIT_COMPLETION = "LastCompletionCheck"
TASK_TR_METRICS = "TaskMetrics"
TASK_TR_NAME = "TaskName"
TASK_TR_NOTIFICATIONS = "TaskNotifications"
TASK_TR_PARAMETERS = "Parameters"
TASK_TR_RESOURCES = "Resources"
TASK_TR_RESULT = "ActionResult"
TASK_TR_RUN_LOCAL = "RunLocal"
TASK_TR_S3_RESOURCES = "S3Resources"
TASK_TR_REGION = "Region"
TASK_TR_RESOURCE_TYPE = "ResourceType"
TASK_TR_SELECT_SIZE = "SelectSize"
TASK_TR_SERVICE = "Service"
TASK_TR_SOURCE = "Source"
TASK_TR_START_EXECUTION_TIME = "StartExecutionTime"
TASK_TR_START_RESULT = "ActionStartResult"
TASK_TR_STARTED_TS = "StartedTs"
TASK_TR_STATUS = "Status"
TASK_TR_TAGFILTER = "TagFilter"
TASK_TR_TIMEOUT = "TaskTimeout"
TASK_TR_TIMEZONE = "Timezone"
TASK_TR_TTL = "TTL"
TASK_TR_UPDATED = "Updated"
TASK_TR_UPDATED_TS = "UpdatedTs"

STATUS_PENDING = "pending"
STATUS_STARTED = "started"
STATUS_WAIT_FOR_COMPLETION = "wait-to-complete"
STATUS_COMPLETED = "completed"
STATUS_TIMED_OUT = "timed-out"
STATUS_FAILED = "failed"
STATUS_WAITING = "wait-for-exec"

# task attributes
TASK_ACTION = "action"
TASK_COMPLETION_ECS_MEMORY = "completion_ecs_memory"
TASK_COMPLETION_SIZE = "completion_size"
TASK_ACCOUNTS = "accounts"
TASK_ROLE = "cross_account_role"
TASK_DEBUG = "debug"
TASK_DESCRIPTION = "description"
TASK_DRYRUN = "dryrun"
TASK_ENABLED = "enabled"
TASK_EVENT_SCOPES = "event_scopes"
TASK_EVENT_SOURCE_TAG_FILTER = "EventSourceTagFilter"
TASK_EVENTS = "events"
TASK_EXECUTE_ECS_MEMORY = "execute_ecs_memory"
TASK_EXECUTE_SIZE = "execute_size"
TASK_ID = "id"
TASK_INTERNAL = "internal"
TASK_INTERVAL = "interval"
TASK_METRICS = "task_metrics"
TASK_NAME = "name"
TASK_NOTIFICATIONS = "notifications"
TASK_PARAMETERS = "parameters"
TASK_REGIONS = "regions"
TASK_SELECT_ECS_MEMORY = "select_ecs_memory"
TASK_SELECT_SIZE = "select_size"
TASK_SERVICE = "service"
TASK_RESOURCE_TYPE = "resource_type"
TASK_TAG_FILTER = "tag_filter"
TASK_THIS_ACCOUNT = "this_account"
TASK_TIMEOUT = "timeout"
TASK_TIMEZONE = "timezone"

HANDLER_EVENT_ACTION = "action"
HANDLER_ACTION_EXECUTE = "execute-action"
HANDLER_ACTION_TEST_COMPLETION = "test-completion-action"
HANDLER_ACTION_SELECT_RESOURCES = "select-resources"
HANDLER_SELECT_ARGUMENTS = "select-args"
HANDLER_SELECT_RESOURCES = "resources"
HANDLER_EVENT_SCHEDULER_EXECUTE_TASK = "scheduler-execute-task"
HANDLER_EVENT_TASK_NAME = "task-name"

HANDLER_EVENT_TASK_DT = "task-datetime"
HANDLER_EVENT_TASK = "task"
HANDLER_EVENT_ACCOUNT = "account"
HANDLER_EVENT_REGIONS = "regions"
HANDLER_EVENT_SOURCE = "source"
HANDLER_EVENT_DYNAMO_SOURCE = "eventSource"
HANDLER_EVENT_SUB_TASK = "sub-task"
HANDLER_EVENT_RESOURCE_NAME = "resource-name"
HANDLER_EVENT_CUSTOM_SELECT = "custom-select"
HANDLER_EVENT_TASK_GROUP = "task-group"

EVENT_SCOPE_RESOURCE = "resource"
EVENT_SCOPE_REGION = "region"

UNKNOWN_SOURCE = "unknown"

ERR_NO_MODULE_FOR_HANDLER = "Can not load module {} for handler {}, available handlers are {}"
ERR_UNEXPECTED_HANDLER_CLASS_IN_MODULE = "Unable to load class {0}Handler for handler {0} from module {1}, " \
                                         "handler class found in module was {2}"
ERR_EVENT_RULE_NOT_FOUND = "Can not get CloudWatch rule {} in stack {}"
ERR_FAILED_TO_START_ECS_TASK = "Failed to start ECS job for {}, failures are {}"
ERR_CREATING_SESSION = "Error creating session {}"

DESC_NO_EXECUTIONS_FOR_EXPR = "No task scheduled within the next 24 hours"
DESC_EXPRESSION_SET = "Schedule expression set to {}"

HANDLERS = "handlers"
HANDLERS_MODULE_NAME = HANDLERS + ".{}"
HANDLERS_PATH = "./" + HANDLERS

HANDLER = "Handler"
HANDLER_CLASS = "{}" + HANDLER

__handlers = {}
__actions = None

_kms_client = None


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
    # noinspection PyPep8
    try:
        return _get_module(module_name)
    except:
        raise ImportError(ERR_NO_MODULE_FOR_HANDLER.format(module_name, handler_name, ", ".join(all_handlers())))


def all_handlers():
    global __actions
    if __actions is None:
        __actions = []
        current = abspath(os.getcwd())
        while True:
            if isdir(os.path.join(current, "handlers")):
                break
            parent = dirname(current)
            if parent == current:
                # at top level
                raise Exception("Could not find handlers directory")
            else:
                current = parent

        for f in listdir(os.path.join(current, "handlers")):
            if isfile(join(current, "handlers", f)) and f.endswith("_{}.py".format(HANDLER.lower())):
                module_name = HANDLERS_MODULE_NAME.format(f[0:-len(".py")])
                m = _get_module(module_name)
                cls = _get_handler_class(m)
                if cls is not None:
                    handler_name = cls[0]
                    __actions.append(handler_name)
    return __actions


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
        m = get_module_for_handler(handler_name)
        cls = _get_handler_class(m)
        if cls is None or cls[0] != handler_name:
            raise ImportError(ERR_UNEXPECTED_HANDLER_CLASS_IN_MODULE.format(handler_name, m, cls[0] if cls else "None"))
        __handlers[handler_name] = cls

    return __handlers[handler_name][1]


def _get_cloudwatch_rule(name, client):
    """
    Get the CloudWatch event rule with the name prefix that is the stack name + name  in the current stack
    :param name: part of the name that is added to the stack name to build the name prefix of the cloudwatch rule
    :param client: CloudWatch client

    :return: CloudWatch rules
    """

    resp = client.list_rules_with_retries(NamePrefix=name)
    rules = resp.get("Rules", [])
    if len(rules) != 1:
        raise_exception(ERR_EVENT_RULE_NOT_FOUND, name, getenv(ENV_STACK_NAME))
    return rules[0]


def _get_cloudwatch_events_client(context):
    """
    Builds client for making boto3 CloudWatch rule api calls with retries
    :param context:
    :return:
    """
    return boto_retry.get_client_with_retries("events", ["enable_rule", "disable_rule", "list_rules", "put_rule"], context)


def enable_completion_cloudwatch_rule(context=None):
    """
    Enables the task completion CloudWatch rule
    :param context: Lambda execution context
    :return: Name of the Cloudwatch event that was enabled
    """
    events_client = _get_cloudwatch_events_client(context)
    event_rule_name = _get_cloudwatch_rule(os.getenv(ENV_COMPLETION_RULE), events_client)["Name"]
    events_client.enable_rule_with_retries(Name=event_rule_name)
    return event_rule_name


def disable_completion_cloudwatch_rule(context=None):
    """
      Disables the task completion CloudWatch rule
      :param context: Lambda execution context
      :return: Name of the Cloudwatch event that was disabled
      """
    events_client = _get_cloudwatch_events_client(context)
    event_rule_name = _get_cloudwatch_rule(os.getenv(ENV_COMPLETION_RULE), events_client)["Name"]
    events_client.disable_rule_with_retries(Name=event_rule_name)
    return event_rule_name


def set_scheduler_rule_every_minute(task=None, context=None):
    """
    Sets the scheduler CloudWatch Event rule to execute every minutes
    :return:
    """
    _set_scheduler_cloudwatch_rule_expression("cron(0/1 * * * ? *)", task=task, context=context)


def set_event_for_time(next_time, task=None, logger=None, context=None):
    next_event_time = next_time - timedelta(minutes=4)
    if logger is not None:
        logger.info(INF_NEXT_EVENT, next_time, next_event_time)
    utc_now = datetime.utcnow().replace(microsecond=0, second=0, tzinfo=pytz.utc)
    if next_event_time <= utc_now:
        if logger is not None:
            logger.info(INF_NEXT_EVENT_IN_PAST, utc_now)
        set_scheduler_rule_every_minute(task, context)
    else:
        cron = "cron({} {} * * ? *)".format(next_event_time.minute, next_event_time.hour)
        _set_scheduler_cloudwatch_rule_expression(cron, task=task, context=context, logger=logger)
    return next_event_time


def _set_scheduler_cloudwatch_rule_expression(expression, context=None, task=None, logger=None):
    """
    Sets a new expression for a CloudWatch rule
    :param expression: new cloudwatch expression in the syntax cron(x x x x x x). Note that this format has an additional year
    field that is not used by the scheduler.
    :param context: Lambda execution context
    :return:
    """
    events_client = _get_cloudwatch_events_client(context)
    event_rule = _get_cloudwatch_rule(os.getenv(ENV_OPS_AUTOMATOR_RULE), events_client)

    try:
        # get the con expression from the expression
        cron_str = " ".join(expression[expression.index("(") + 1:expression.index(")")].split(" ")[0:5])
        if logger is not None:
            logger.info(INF_NEW_CRON, expression)
        cron = CronExpression(cron_str)
        next_execution_time = cron.first_within_next(start_dt=datetime.utcnow(), timespan=timedelta(hours=24))
        if next_execution_time is not None:
            description = DESC_EXPRESSION_SET.format(expression)
            if task is not None:
                description += " for task {} scheduled at {}".format(task.get(TASK_NAME, ""),
                                                                     CronExpression(task[TASK_INTERVAL]).first_within_next(
                                                                         start_dt=datetime.utcnow(),
                                                                         timespan=timedelta(hours=24)).isoformat())
        else:
            description = DESC_NO_EXECUTIONS_FOR_EXPR
    except ValueError:
        description = ""

    if event_rule["ScheduleExpression"] != expression or event_rule.get("Description", "") != description:
        args = {
            "Name": event_rule["Name"],
            "ScheduleExpression": expression,
            "Description": description
        }
        events_client.put_rule_with_retries(**args)


def running_local(context):
    return context is None or getattr(context, "run_local", False)


def run_as_ecs_job(args, ecs_memory_size, context=None, logger=None):
    """
    Runs a teak step as ecs task
    :param args: ecs task parameters
    :param ecs_memory_size: reserved memory size for ecs task container
    :param context: lambda context
    :param logger: logger
    :return: result of ecs task submission
    """
    start_time = time.time()
    start_task_timeout = 300

    def timed_out_no_context(next_wait):
        return (time.time() - start_time) > (start_task_timeout - next_wait)

    def timed_out_by_lambda_timeout(next_wait):
        if context is None:
            return False

        context_seconds_left = context.get_remaining_time_in_millis() * 1000
        return context_seconds_left < (5 + next_wait)

    runner_args = copy.deepcopy(args)

    ecs_client = boto_retry.get_client_with_retries("ecs", ["run_task"], context=context)
    stack_name = os.getenv(ENV_STACK_NAME)

    runner_args["stack"] = stack_name
    runner_args["stack_region"] = ecs_client.meta.region_name

    ecs_params = {
        "cluster": os.getenv(ENV_ECS_CLUSTER),
        "taskDefinition": os.getenv(ENV_ECS_TASK),
        "startedBy": "{}:{}".format(stack_name, args[TASK_NAME])[0:35],
        "overrides": {
            "containerOverrides": [
                {
                    "name": "ops-automator",
                    "command": ["python", "ops-automator-ecs-runner.py", safe_json(runner_args)],
                    "memoryReservation": int(ecs_memory_size if ecs_memory_size is not None else ECS_DEFAULT_MEMORY_RESERVATION)
                }
            ],
        },
    }

    for wait_until_next_retry in boto_retry.LinearWaitStrategy(start=5, incr=5, max_wait=30, random_factor=0.50):

        # try to start task
        resp = ecs_client.run_task_with_retries(**ecs_params)

        # test if task was started
        if len(resp["tasks"]) != 0:
            if logger is not None:
                logger.info("{} executed as ECS job:{}\n", args[HANDLER_EVENT_ACTION],
                            safe_json(resp.get("tasks", []), indent=3))
            return resp

        # investigate failures, note that no exceptions are raised if tasks fails because of insufficient resources in cluster
        failures = resp.get("failures", [])

        # test for other failures than not enough memory resources on cluster instances
        # and test if there is time left for another retry based on on Lambda timeout or fixed timeout when not running in Lambda
        if not all([f["reason"] == "RESOURCE:MEMORY" for f in resp["failures"]]) or \
                (timed_out_by_lambda_timeout(next_wait=wait_until_next_retry) or
                 timed_out_no_context(next_wait=wait_until_next_retry)):
            raise_exception(ERR_FAILED_TO_START_ECS_TASK, safe_json(args), safe_json(failures, indent=3))
        else:
            # sleep until nxt retry
            time.sleep(wait_until_next_retry)


def get_item_resource_data(item, context):
    global _kms_client
    resource_data = item.get(TASK_TR_RESOURCES, "{}")
    if item.get(TASK_TR_ENCRYPTED_RESOURCES):
        if _kms_client is None:
            _kms_client = boto_retry.get_client_with_retries("kms", ["decrypt"], context=context)
        resource_data = _kms_client.decrypt(CiphertextBlob=base64.b64decode(resource_data))["Plaintext"]
    return resource_data if type(resource_data) in [dict, list] else json.loads(resource_data)


def default_rolename_for_stack():
    return DEFAULT_ACCOUNT_ROLENAME.format(os.getenv(ENV_STACK_NAME))


def log_to_debug(logger, msg, *args):
    if logger is not None:
        logger.debug(msg, *args)


def get_task_session(account, task, this_account=False, logger=None):
    log_to_debug(logger, "Getting session for account \"{}\", task is \"{}\"", account, task[TASK_NAME])

    role_arn = get_account_role(account, task, logger=logger)

    try:
        return services.get_session(role_arn=role_arn, logger=logger), role_arn
    except Exception as ex:
        if logger is not None:
            logger.error(ERR_CREATING_SESSION, ex)
        return None, role_arn


def get_account_role(account, task, logger=None, param_name=None):
    def build_arn(role):
        if role is None:
            arn = None
        else:
            arn = ARN_ROLE_TEMPLATE.format(account, role)
        log_to_debug(logger, "Role arn is \"{}\"", format(arn))
        return arn

    role_name = None

    if param_name is not None:
        role_name = task.get(param_name, None)

    if role_name is None:
        role_name = task.get(TASK_ROLE, None)

    if role_name is None:
        if account == os.getenv(ENV_OPS_AUTOMATOR_ACCOUNT) and task.get(TASK_THIS_ACCOUNT, False):
            log_to_debug(logger, "No role assumed, using lambda role \"{}\"", os.getenv(ENV_OPS_AUTOMATOR_ROLE_ARN))
            return None
        else:
            log_to_debug(logger, "No role specified, using default role for account {}", account)
            return build_arn(default_rolename_for_stack())
    else:
        log_to_debug(logger, "Using specified role with name  \"{}\"", role_name)
        return build_arn(role_name)
