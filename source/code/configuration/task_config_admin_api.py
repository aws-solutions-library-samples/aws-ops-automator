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
from datetime import datetime

import actions
import configuration
from configuration.task_configuration import TaskConfiguration
from util import safe_json
from util.logger import Logger

ERR_ACTION_DOES_NOT_EXIST = "not found: action {} does not exist, possible actions are {}"
ERR_NO_TASK_NAME = "error: task name attribute must be present and can not be empty"
ERR_TASK_DOES_ALREADY_EXIST = "error: task with name {} already exists"
ERR_TASK_DOES_NOT_EXIST = "not found: task with name {} does not exist"

LOG_STREAM = "TaskConfigAdminApi-{:0>4d}{:0>2d}{:0>2d}"


def _get_logger(context):
    dt = datetime.utcnow()
    logstream = LOG_STREAM.format(dt.year, dt.month, dt.day)
    return Logger(logstream=logstream, buffersize=10, context=context)


def get_tasks(include_internal=False, context=None):
    """
    Returns all available tasks
    :param include_internal: True if internal tasks must be included
    :param context: Lambda context
    :return: all available tasks
    """

    with _get_logger(context=context) as logger:
        logger.info("get_tasks()")
        tasks = [t for t in TaskConfiguration(context=context, logger=logger).config_items(include_internal)]
        return safe_json(tasks)


def get_task(name, context=None):
    """
    Returns item for specified task
    :param name: Name of the task
    :param context: Lambda context
    :return: Task item, raises exception if task with specified name does not exist
    """

    with _get_logger(context=context) as logger:
        logger.info("get_task")
        item = TaskConfiguration(context=context, logger=logger).get_config_item(name)
        if item is None:
            raise ValueError("not found: task with name {} does not exist".format(name))
        return safe_json(item)


def create_task(context=None, **kwargs):
    """
    Creates a new task

    :param kwargs: Task parameters
    :param context: Lambda context

    Constants can be found in configuration/__init__.py

    -CONFIG_ACTION_NAME: Name of the action executed by the task, exception is raised if not specified or action does not
    exist (mandatory, string)

    -CONFIG_DEBUG: Set to True to log additional debug information for this task (optional, default False, boolean)

    -CONFIG_DESCRIPTION: Task description(optional, default None, string)

    -CONFIG_CROSS_ACCOUNT_ROLES: List of cross accounts for cross account processing. Note that roles if the account of a role
    has already been found in another role, or if the account of a role is the processed account of the scheduler a warning
    is generated when executing the task and the role is skipped (optional, default [], List<string>)

    -CONFIG_ENABLED: Set to True to enable execution of task, False to suspend executions (optional, default True, boolean)

    -CONFIG_INTERNAL: Flag to indicate task is used for internal  tats of the scheduler (optional, default False, boolean)

    -CONFIG_INTERVAL: Cron expression to schedule time/date based execution of task (optional, default "", string)
    
    -CONFIG_TASK_TIMEOUT: Timeout in minutes for task to complete (optional, default is action's value or global timeout, number)

    -CONFIG_TASK_NAME: Name of the task, exception is raised if not specified or name does already exist (mandatory, string)

    -CONFIG_PARAMETERS: dictionary with names and values passed to the executed action of this task(optional,default {}, dictionary)

    -CONFIG_THIS_ACCOUNT: Set to True to run tasks for resources in the account of the (optional, default True, boolean)

    -CONFIG_TIMEZONE: Timezone for time/date based tasks for this task (optional, default UTC, string)

    -CONFIG_TAG_FILTER: Tag filter used to select resources for the task instead of name of task in the list of values for the
    automation tag. Only allowed if selected resources support tags (optional, default "", string)

    -CONFIG_REGIONS: Regions in which to run the task. Use "*" for all regions in which the service for this tasks action 
    is available. If no regions are specified the region in which the scheduler is installed is used as default. Specifying one 
    or more regions for services tha are not region specific will generate a warning when processing the task. (optional,
    default current region, List<string>)

    -CONFIG_STACK_ID: Id of the stack if the task is created as part of a cloudformation template (optional, default None, string)

    -CONFIG_DRYRUN: Dryrun parameter passed to the executed action (optional, default False, boolean)

    -CONFIG_EVENTS: List of resource events that trigger the task to be executed  (optional, default, List<string>)

    -CONFIG_DRYRUN: Dryrun parameter passed to the executed action (optional, default False, boolean)

    -CONFIG_EVENTS: List of resource events that trigger the task to be executed  (optional, default, List<string>)

    :return: Item created in the task configuration
    """

    with _get_logger(context=context) as logger:
        logger.info("create_task")
        config = TaskConfiguration(context=context, logger=logger)
        name = kwargs.get(configuration.CONFIG_TASK_NAME)
        if name is None or len(name) == 0:
            raise ValueError(ERR_NO_TASK_NAME)

        item = config.get_config_item(name)
        if item is not None:
            raise ValueError(ERR_TASK_DOES_ALREADY_EXIST.format(name))

        new_item = config.put_config_item(**kwargs)
        return safe_json(new_item)


def update_task(name, context=None, **kwargs):
    """
    Updates the specified task. An exception is raised when the action does not exist.
    :param name: Name of the task. This name overwrites the name in kwargs if it is used there
    :param kwargs: Task parameters dictionary, see create_task for details.
    :param context: Lambda context
    :return: Updated task item
    """
    with _get_logger(context=context) as logger:
        logger.info("update_task")
        config = TaskConfiguration(context=context, logger=logger)
        if name is None or len(name) == 0:
            raise ValueError(ERR_NO_TASK_NAME)
        item = config.get_config_item(name)
        if item is None:
            raise ValueError(ERR_TASK_DOES_NOT_EXIST.format(name))

        # copy to avoid side effects when modifying arguments
        args = copy.deepcopy(kwargs)
        args[configuration.CONFIG_TASK_NAME] = name
        stack_id = item.get(configuration.CONFIG_STACK_ID)
        if stack_id is not None:
            args[configuration.CONFIG_STACK_ID] = stack_id
        item = config.put_config_item(**args)
        return safe_json(item)


def delete_task(name, exception_if_not_exists=False, context=None):
    """
    Deletes the specified task
    :param name: Name of the task to be deleted, if the task does not exist an exception is raised
    :param exception_if_not_exists: if set to True raises an exception if the item does not exist
    :param context: Lambda context
    :return: Deleted task item
    """
    with _get_logger(context=context) as logger:
        logger.info("delete_task")
        config = TaskConfiguration(context=context, logger=logger)
        if exception_if_not_exists:
            item = config.get_config_item(name)
            if item is None:
                raise ValueError(ERR_TASK_DOES_NOT_EXIST.format(name))
        else:
            item = {"Name": name}
        config.delete_config_item(name)
        return safe_json(item)


def get_action(name, context=None, log_this_call=True):
    """
    Gets the details of the specified action
    :param name: Name of the action, raises an exception if the action does not exist
    :param context: Lambda context
    :param log_this_call: switch
    :return: Details of the specified action. This dictionary can contain the following actions:

    Constants used below can be found in actions/__init__.py

    -ACTION_SERVICE: Name of the service of the resources of this action

    -ACTION_RESOURCES: Name of the resources for this action

    -ACTION_AGGREGATION: Possible values are:
        ACTION_AGGREGATION_RESOURCE: resources are not aggregated, execution of the action for each individual resource.
        ACTION_AGGREGATION_ACCOUNT: resources are aggregated per account, execution of the action for the list of resources 
        in that account
        ACTION_AGGREGATION_TASK: resources are aggregated per task, single execution of the action for list of all resources 
        in all accounts

    -ACTION_SELECT_EXPRESSION: Optional JMES path to map/select attributes of and filtering of resources

    -ACTION_BATCH_SIZE: Optional batch size for aggregated resources.

    -ACTION_PERMISSIONS: Optional, permissions required for the action

    -ACTION_MEMORY: Optional memory requirement for lambda function to run action, default is size of the scheduler lambda function

    -ACTION_CROSS_ACCOUNT: Optional, cross account operations supported by action, default is True

    -ACTION_EVENT_FILTER: Optional, regex filter which type of source events are supported by the 
    action, default is None (all events)

    -ACTION_TITLE: Optional, title to be used in UI

    -ACTION_DESCRIPTION: Optional, description or url to be used in UI

    -ACTION_AUTHOR: Optional, author of the action

    -ACTION_VERSION: Optional, implementation version of the action

    -ACTION_MULTI_REGION: Optional, True if the action can execute in multiple regions (default)

    -ACTION_INTERNAL: Optional, True if the service can only be used in internal tasks

    -ACTION_PARAM_STACK_RESOURCES: Optional, cloudformation snippet of resources owen and used by action implementation

    -ACTION_STACK_RESOURCES_PERMISSIONS: Optional, list of permissions for action stack resources

    -ACTION_PARAMETERS: Parameters for the action:

        -PARAM_ALLOWED_VALUES: allowed valued for a parameter (optional)

        -PARAM_DEFAULT: default value for a parameter (optional)

        -PARAM_MAX_LEN: max length for a string parameter (optional)

        -PARAM_MAX_VALUE: max value for a numeric parameter (optional)

        -PARAM_MIN_LEN: min length for a string parameter (optional)

        -PARAM_MIN_VALUE: # min value for a numeric parameter (optional)

        -PARAM_PATTERN: allowed pattern for a string parameter (optional)

        -PARAM_REQUIRED: true if parameter is required (default=False)

        -PARAM_TYPE:  (Python) type name of a parameter

        -PARAM_DESCRIBE_PARAMETER: name of a parameter if it must be used as a parameter in the describe method for a resource

        -PARAM_DESCRIPTION: user readable description for parameter

        -PARAM_LABEL: label for parameter

    """
    with _get_logger(context=context) as logger:
        if log_this_call:
            logger.info("get_action")
        all_actions = actions.all_actions()
        if name not in all_actions:
            raise ValueError(ERR_ACTION_DOES_NOT_EXIST.format(name, ",".join(all_actions)))
        return safe_json(actions.get_action_properties(name))


def get_actions(context=None):
    """
    Returns  a dictionary with all available actions, see get_action for details on returned items
    :param context: Lambda context
    :return: all available action
    """
    with _get_logger(context=context) as logger:
        logger.info("get_actions")
        return safe_json({action_name: get_action(action_name, log_this_call=False) for action_name in actions.all_actions()})
