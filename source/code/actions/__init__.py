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
from os import listdir
from os.path import isfile, join

from util import pascal_to_snake_case

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


# permissions on stack owned resources
ACTION_STACK_RESOURCES_PERMISSIONS = "StackResourcePermissions"
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
# Parameters to be passed to method that selects the resources
ACTION_SELECT_PARAMETERS = "SelectParams"
# permissions required to execute the action
ACTION_PERMISSIONS = "Permissions"
# memory requirements for lambda function executing the action in MB, int
ACTION_MEMORY = "Memory"
# action version, string
ACTION_VERSION = "Version"
# action description, string
ACTION_DESCRIPION = "Description"
# action author, string
ACTION_AUTHOR = "Author"
# action title, string
ACTION_TITLE = "Title"
# filter for events an action can handle, regex string
ACTION_EVENT_FILTER = "Events"
# allow cross account operations
ACTION_CROSS_ACCOUNT = "CrossAccount"
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

DEFAULT_COMPLETION_TIMEOUT_MINUTES_DEFAULT = 60

ACTION_PARAM_ACCOUNT = "account"
ACTION_PARAM_TASK = "task"
ACTION_PARAM_DRYRUN = "dryrun"
ACTION_PARAM_DEBUG = "debug"
ACTION_PARAM_RESOURCES = "resources"
ACTION_PARAM_LOGGER = "logger"
ACTION_PARAM_SESSION = "session"
ACTION_PARAM_STACK = "stack"
ACTION_PARAM_STACK_ID = "stack-id"
ACTION_PARAM_CONTEXT = "context"
ACTION_PARAM_EVENT = "event"
ACTION_PARAM_START_RESULT = "start-result"
ACTION_PARAM_STACK_RESOURCES = "stack-resources"
ACTION_PARAM_ACTION_ID = "action-id"

# optional static method for actions to perform additional parameter checking
ACTION_VALIDATE_PARAMETERS_METHOD = "action_validate_parameters"
# optional static method for actions that require concurrency control
ACTION_CONCURRERNCY_KEY_METHOD = "action_concurrency_key"

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
ACTION_AGGREGATION_TASK = "Task"

ERR_NO_MODULE_FOR_ACTION = "Can not load module {} for action {} ({}), available actions are {}"
ERR_UNEXPECTED_ACTION_CLASS_IN_MODULE = "Unable to load class {0}Action for action {0} from module {1}, action class in module " \
                                        "was {2}"

ACTIONS = "actions"
ACTION_MODULE_NAME = ACTIONS + ".{}"
ACTION_PATH = "./actions"

ACTION = "Action"
ACTION_CLASS = "{}" + ACTION

CHECK_CAN_EXECUTE = "can_execute"
CUSTOM_AGGREGATE_METHOD = "custom_aggregation"
METRICS_DATA = "metrics"

__actions = {}


def _get_action_class_from_module(module):
    """
    Find the action class in a module using naming pattern.
    :param module: The module
    :return: Class for the action, None if no action class was found
    """
    for cls in inspect.getmembers(module, inspect.isclass):
        if cls[1].__module__ != module.__name__ or not cls[1].__name__.endswith(ACTION):
            continue
        return cls
    return None


def _get_module(module_name):
    """
    Loads a module by its name
    :param module_name: Name of the module
    :return: Loaded module
    """
    module = sys.modules.get(module_name)
    if module is None:
        module = importlib.import_module(module_name)
    return module


def all_actions():
    """
    Returns a list of all available actions from the *.py files in the actions directory
    :return: ist of all available action
    """
    result = []
    for f in listdir(ACTION_PATH):
        if isfile(join(ACTION_PATH, f)) and f.endswith("_{}.py".format(ACTION.lower())):
            module_name = ACTION_MODULE_NAME.format(f[0:-len(".py")])
            module = _get_module(module_name)
            cls = _get_action_class_from_module(module)
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
            module = _get_module(module_name)
        except Exception as ex:
            raise ImportError(ERR_NO_MODULE_FOR_ACTION.format(module_name, action_name, ex, ", ".join(all_actions())))

        cls = _get_action_class_from_module(module)
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
        "Version": action.properties[ACTION_VERSION],
        "ActionId": action.properties[ACTION_ID],
        "Data": {}
    }
    for d in data:
        metrics["Data"][d] = data[d]

    return metrics

