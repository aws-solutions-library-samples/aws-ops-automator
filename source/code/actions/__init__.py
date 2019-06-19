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
import os
import sys
from datetime import datetime
from os import listdir
from os.path import isfile, join, isdir

import handlers
from handlers import ENV_STACK_NAME
from helpers import pascal_to_snake_case
from outputs.report_output_writer import create_output_writer

# allowed valued for a parameter, []

PARAM_ALLOWED_VALUES = "AllowedValues"
# default value for a parameter
PARAM_DEFAULT = "Default"
# max length for a string parameter, int
PARAM_MAX_LEN = "MaxLength"
# max value for a numeric parameter, numeric
PARAM_MAX_VALUE = "MaxValue"
# min length for a string parameter, int
PARAM_MIN_LEN = "MinLength"
# min value for a numeric parameter, numeric
PARAM_MIN_VALUE = "MinValue"
# allowed pattern for a string parameter, regex string
PARAM_PATTERN = "AllowedPattern"
# parameter is required, boolean
PARAM_REQUIRED = "Required"
# (Python) type of a parameter, type
PARAM_TYPE = "Type"
# name of a parameter if it must be used as a parameter in the describe method for a resource, string
PARAM_DESCRIBE_PARAMETER = "DescribeParameter"
# parameter description, string
PARAM_DESCRIPTION = "Description"
# label or identifier for parameter
PARAM_LABEL = "Label"
# AWS specific types
PARAM_TYPE_AWS = "AwsType"
# don't generate UI for hidden parameters
PARAM_HIDDEN = "Hidden"

# permissions on action owned resources
ACTION_STACK_RESOURCES_PERMISSIONS = "StackResourcePermissions"
# action owned resources
ACTION_STACK_RESOURCES = "StackResources"
# action parameters, dictionary holding keys PARAM_* (see constants above), dictionary
ACTION_PARAMETERS = "Parameters"
# name of the resources processed by the action, string
ACTION_RESOURCES = "Resources"
# name of the service for the resources processed by this action, string
ACTION_SERVICE = "Service"
# batch size for action execution for aggregated resources, int
ACTION_BATCH_SIZE = "BatchSize"
# JMES Path statement for selecting specific attributes and filtering resources (similar to select statement of aws cli), string
ACTION_SELECT_EXPRESSION = "Select"
# Flag to indicate that the select filter requires the tags to be pre-loaded
ACTION_SELECTION_REQUIRES_TAGS = "SelectRequireTags"
# Parameters to be passed to method that selects the resources
ACTION_SELECT_PARAMETERS = "SelectParams"
# permissions required to execute the action
ACTION_PERMISSIONS = "Permissions"
# Memory required for selecting resources
ACTION_SELECT_SIZE = "SelectMemory"
# Memory required for executing action
ACTION_EXECUTE_SIZE = "ExecuteMemory"
# Memory required for executing completion logic
ACTION_COMPLETION_SIZE = "CompletionMemory"
# action version, string
ACTION_VERSION = "Version"
# action description, string
ACTION_DESCRIPTION = "Description"
# action author, string
ACTION_AUTHOR = "Author"
# action title, string
ACTION_TITLE = "Title"
# filter for events an action can handle, regex string
ACTION_EVENT_FILTER = "Events"
# allow cross account operations
ACTION_CROSS_ACCOUNT = "CrossAccount"
# no selection on tags, forced tagfiler "*"
ACTION_NO_TAG_SELECT = "NoTagSelect"
# class that implements the action
ACTION_CLASS_NAME = "ClassName"
# Unique id for an action
ACTION_ID = "ActionId"
# Keep tags of resources selected for the action
ACTION_KEEP_RESOURCE_TAGS = "KeepTags"
# Purpose of action is to use it for scheduler internal tasks only
ACTION_INTERNAL = "Internal"
# Allow action only to run in its own region or in other regions
ACTION_MULTI_REGION = "MultiRegion"
# maximum concurrent running instances of the action
ACTION_MAX_CONCURRENCY = "MaxConcurrent"
# timeout for action to complete
ACTION_COMPLETION_TIMEOUT_MINUTES = "CompletionTimeout"
# Allow wildcards in tag filter
ACTION_ALLOW_TAGFILTER_WILDCARD = "AllowTagFilterWildcards"
# events that can trigger an action
ACTION_EVENTS = "Events"
# sources that can trigger a task for an action
ACTION_TRIGGERS = "TriggeredBy"
# Minimum interval between scheduled executions
ACTION_MIN_INTERVAL_MIN = "MinIntervalMinutes"
# event scopes
ACTION_EVENT_SCOPES = "EventScopes"

ACTION_TRIGGER_INTERVAL = ["Interval"]
ACTION_TRIGGER_EVENTS = ["Events"]
ACTION_TRIGGER_BOTH = ACTION_TRIGGER_INTERVAL + ACTION_TRIGGER_EVENTS

DEFAULT_COMPLETION_TIMEOUT_MINUTES_DEFAULT = 60

ACTION_SIZE_STANDARD = "Standard"
ACTION_SIZE_MEDIUM = "Medium"
ACTION_SIZE_LARGE = "Large"
ACTION_SIZE_XLARGE = "XLarge"
ACTION_SIZE_XXLARGE = "XXLarge"
ACTION_SIZE_XXXLARGE = "XXXLarge"
ACTION_USE_ECS = "ECS"

RESTRICTED_TAG_VALUE_SET_CHARACTERS = r"[^a-zA-Z0-9\s_\.:+/=\\@-]"

DUMMY_VOLUME_IF_FOR_COPIED_SNAPSHOT = "vol-ffffffff"

ACTION_SIZE_LAMBDA_ALL = [ACTION_SIZE_STANDARD,
                          ACTION_SIZE_MEDIUM,
                          ACTION_SIZE_LARGE,
                          ACTION_SIZE_XLARGE,
                          ACTION_SIZE_XXLARGE,
                          ACTION_SIZE_XXXLARGE]

ACTION_SIZE_ALL_WITH_ECS = ACTION_SIZE_LAMBDA_ALL + [ACTION_USE_ECS]

ACTION_PARAM_ACCOUNT = "account"
ACTION_PARAM_ASSUMED_ROLE = "assumed_role"
ACTION_PARAM_CONTEXT = "context"
ACTION_PARAM_DEBUG = "debug"
ACTION_PARAM_DRYRUN = "dryrun"
ACTION_PARAM_EVENT = "event"
ACTION_PARAM_EVENTS = "events"
ACTION_PARAM_EVENT_SOURCE_TAG_FILTER = "EventSourceTagFilter"
ACTION_PARAM_HAS_COMPLETION = "has_completion"
ACTION_PARAM_INTERVAL = "interval"
ACTION_PARAM_LOGGER = "logger"
ACTION_PARAM_RESOURCES = "resources"
ACTION_PARAM_SESSION = "session"
ACTION_PARAM_STACK = "stack"
ACTION_PARAM_STACK_ID = "stack_id"
ACTION_PARAM_STACK_RESOURCES = "stack_resources"
ACTION_PARAM_START_RESULT = "start_result"
ACTION_PARAM_STARTED_AT = "started_at"
ACTION_PARAM_TAGFILTER = "tagfilter"
ACTION_PARAM_TASK = "task"
ACTION_PARAM_TASK_ID = "task_id"
ACTION_PARAM_TASK_TIMEZONE = "task_timezone"
ACTION_PARAM_TAG_FILTER = "tagfilter"
ACTION_PARAM_TIMEOUT = "timeout"
ACTION_PARAM_TIMEOUT_EVENT = "timeout_event"

# grouping for action parameters in UI's
ACTION_PARAMETER_GROUPS = "ParameterGroups"
# parameter group title
ACTION_PARAMETER_GROUP_TITLE = "Title"
# action parameter group parameter list
ACTION_PARAMETER_GROUP_LIST = "Parameters"

LAMBDA_DEFAULT_MEMORY = 128

# aggregation level for task resources
ACTION_AGGREGATION = "Aggregation"
# aggregation levels
ACTION_AGGREGATION_RESOURCE = "Resource"
ACTION_AGGREGATION_ACCOUNT = "Account"
ACTION_AGGREGATION_REGION = "Region"
ACTION_AGGREGATION_TASK = "Task"

ERR_NO_MODULE_FOR_ACTION = "Can not load module {} for action {} ({}), available actions are {}"
ERR_UNEXPECTED_ACTION_CLASS_IN_MODULE = "Unable to load class {0}Action for action {0} from module {1}, action class in module " \
                                        "was {2}"

ACTIONS = "actions"
ACTION_MODULE_NAME = ACTIONS + ".{}"
ACTIONS_DIR = "actions"

ACTION = "Action"
ACTION_CLASS = "{}" + ACTION

CHECK_CAN_EXECUTE = "can_execute"
CUSTOM_AGGREGATE_METHOD = "custom_aggregation"
FILTER_RESOURCE_METHOD = "filter_resource"
SELECT_AND_PROCESS_RESOURCE_METHOD = "process_and_select_resource"
METRICS_DATA = "metrics"

MARKER_EC2_IMAGE_INSTANCE_TAG_TEMPLATE = "OpsAutomator:{}-SourceInstanceId"
MARKER_EC2_TAG_SOURCE_VOLUME_TEMPLATE = "OpsAutomator:{}-Snapshot-SourceVolume"

MARKER_RDS_TAG_SOURCE_DB_INSTANCE_ID = "OpsAutomator:{}-RdsInstanceSnapshot-SourceDbInstanceId"
MARKER_RDS_TAG_SOURCE_DB_CLUSTER_ID = "OpsAutomator:{}-RdsClusterSnapshot-SourceDbClusterId"

__actions = {}

_date_time_provider_ = datetime

_report_writer_provider_ = create_output_writer


def date_time_provider():
    return _date_time_provider_


def set_date_time_provider(provider):
    global _date_time_provider_
    _date_time_provider_ = provider


def reset_date_provider():
    global _date_time_provider_
    _date_time_provider_ = datetime


def get_report_output_writer(context=None, logger=None):
    global _report_writer_provider_
    return _report_writer_provider_(context, logger)


def set_report_output_provider(provider):
    global _report_writer_provider_
    _report_writer_provider_ = provider


def reset_report_output_provider():
    global _report_writer_provider_
    _report_writer_provider_ = create_output_writer


def _get_action_class_from_module(class_module):
    """
    Find the action class in a module using naming pattern.
    :param class_module: The module
    :return: Class for the action, None if no action class was found
    """
    for cls in inspect.getmembers(class_module, inspect.isclass):
        if cls[1].__module__ != class_module.__name__ or not cls[1].__name__.endswith(ACTION):
            continue
        return cls
    return None


def get_action_module(module_name):
    """
    Loads a module by its name
    :param module_name: Name of the module
    :return: Loaded module
    """
    mod = sys.modules.get(module_name)
    if mod is None:
        mod = importlib.import_module(module_name)
    return mod


def all_actions():
    """
    Returns a list of all available actions from the *.py files in the actions directory
    :return: ist of all available action
    """
    result = []
    directory = os.getcwd()
    while True:
        if any([entry for entry in listdir(directory) if entry == ACTIONS_DIR and isdir(os.path.join(directory, entry))]):
            break
        directory = os.path.abspath(os.path.join(directory, '..'))

    actions_dir = os.path.join(directory, ACTIONS_DIR)
    for f in listdir(actions_dir):
        if isfile(join(actions_dir, f)) and f.endswith("_{}.py".format(ACTION.lower())):
            module_name = ACTION_MODULE_NAME.format(f[0:-len(".py")])
            mod = get_action_module(module_name)
            cls = _get_action_class_from_module(mod)
            if cls is not None:
                action_name = cls[0][0:-len(ACTION)]
                result.append(action_name)
    return result


def get_action_class(action_name):
    """
    Gets the class that implements the specified action
    :param action_name: Name of the action
    :return: Class that implements the specified action. Raises an error if the class cant be found in the actions module of if
    the module contains an unexpected class name based on its filename
    """

    if action_name not in __actions:
        class_name = ACTION_CLASS.format(action_name)
        module_name = ACTION_MODULE_NAME.format(pascal_to_snake_case(class_name))
        try:
            mod = get_action_module(module_name)
        except Exception as ex:
            raise ImportError(ERR_NO_MODULE_FOR_ACTION.format(module_name, action_name, ex, ", ".join(all_actions())))

        cls = _get_action_class_from_module(mod)
        if cls is None or cls[0][0:-len(ACTION)] != action_name:
            raise ImportError(ERR_UNEXPECTED_ACTION_CLASS_IN_MODULE.format(action_name, module_name, cls[0] if cls else "None"))
        __actions[action_name] = cls
    return __actions[action_name][1]


def create_action(action_name, kwargs):
    """
    Creates and returns an instance of a class that implements the specified action, raises ImportError exception if there is no
    class that implements the action
    :param action_name: Name of the action
    :param kwargs: Optional parameters for creating the action
    :return: instance of a class that implements the specified action
    """
    return get_action_class(action_name)(kwargs)


def get_action_properties(action_name):
    """
    Gets the meta data properties for the specified action, raises ImportError exception if there is no class that implements
    the action
    :param action_name: Name of the action
    :return: Dictionary with action properties. See PARAM_* and ACTION_* constants for details
    """
    action_class = get_action_class(action_name)
    properties = action_class.properties
    properties[ACTION_CLASS_NAME] = action_class.__name__
    return properties


def build_action_metrics(action, **data):
    """
    Builds action metrics data
    :param action: the action
    :param data: the metrics data
    :return:
    """
    metrics = {
        "Type": "action",
        "Action": action.__class__.__name__,
        "Version": action.properties["Version"],
        "ActionId": action.properties["ActionId"],
        "Data": {}
    }
    for d in data:
        metrics["Data"][d] = data[d]

    return metrics


def marker_image_source_instance_tag():
    return MARKER_EC2_IMAGE_INSTANCE_TAG_TEMPLATE.format(os.getenv(handlers.ENV_STACK_NAME))


def log_stream_datetime():
    dt = datetime.utcnow()
    return "{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)


def log_stream_date():
    dt = datetime.utcnow()
    return "{:0>4d}{:0>2d}{:0>2d}".format(dt.year, dt.month, dt.day)


def marker_snapshot_tag_source_source_volume_id():
    return MARKER_EC2_TAG_SOURCE_VOLUME_TEMPLATE.format(os.getenv(ENV_STACK_NAME))


def get_resource_data(res, attribute_names, tag_names=None):
    data = [res.get(a, "") for a in attribute_names]
    tag_data = []
    if tag_names not in [[], None]:
        tags = res.get("Tags", {})
        if tag_names == "*":
            tag_names = tags.keys()
        tag_data = [tags.get(t, "") for t in tag_names]
    return data, tag_data


