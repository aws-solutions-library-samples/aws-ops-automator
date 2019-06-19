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
import uuid
from datetime import datetime, timedelta

import actions
import configuration
import handlers
import pytz
from configuration.task_configuration import TaskConfiguration
from handlers.completion_handler import CompletionHandler
from handlers.schedule_handler import ScheduleHandler
from helpers import safe_json
from outputs import raise_value_error
from outputs.queued_logger import QueuedLogger
from scheduling.cron_expression import CronExpression

ERR_ACTION_DOES_NOT_EXIST = "not found: action {} does not exist, possible actions are {}"
ERR_NO_TASK_NAME = "error: task name attribute must be present and can not be empty"
ERR_TASK_DOES_ALREADY_EXIST = "error: task with name {} already exists"
ERR_TASK_DOES_NOT_EXIST = "not found: task with name {} does not exist"

LOG_STREAM = "TaskConfigAdminApi-{:0>4d}{:0>2d}{:0>2d}"


def _get_logger(context):
    dt = datetime.utcnow()
    logstream = LOG_STREAM.format(dt.year, dt.month, dt.day)
    return QueuedLogger(logstream=logstream, buffersize=10, context=context)


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
        return tasks


def get_task(name, context=None, exception_if_not_exists=True):
    """
    Returns item for specified task
    :param name: Name of the task
    :param context: Lambda context
    :param exception_if_not_exists: true if an exception should be raised if the item does not exist
    :return: Task item, raises exception if task with specified name does not exist
    """

    with _get_logger(context=context) as logger:
        logger.info("get_task")
        item = _get_task(name=name, context=context, logger=logger, exception_if_not_exists=exception_if_not_exists)
        return item


def _get_task(name, context=None, logger=None, exception_if_not_exists=True):
    item = TaskConfiguration(context=context, logger=logger).get_config_item(name)
    if item is None and exception_if_not_exists:
        raise ValueError("not found: task with name {} does not exist".format(name))
    return item


def get_next_task_execution(name, context=None, days=None, hours=None, minutes=None, include_disabled=False):
    with _get_logger(context=context) as logger:
        logger.info("get_next_task_execution")
        return _get_next_task_execution(name=name, context=context, days=days, hours=hours, minutes=minutes,
                                        include_disabled=include_disabled)


def _get_next_task_execution(name, context=None, logger=None, days=None, hours=None, minutes=None, include_disabled=False):
    def get_period():
        period = 0
        if days is not None:
            period += days * 60 * 24
        if hours is not None:
            period += 60 * hours
        if minutes is not None:
            period += minutes

        return timedelta(minutes=period if period != 0 else 24 * 60)

    result = {"Name": name}

    task = _get_task(name, context=context, logger=logger)

    enabled = task[configuration.CONFIG_ENABLED]
    result["Enabled"] = enabled

    if not enabled and include_disabled:
        return safe_json(result)

    task_interval = task.get(configuration.CONFIG_INTERVAL, None)
    result["Interval"] = task_interval
    if task_interval is None:
        return safe_json(result)

    task_cron_expression = CronExpression(expression=task_interval)
    task_timezone = task.get(configuration.CONFIG_TIMEZONE, "UTC")
    result["Timezone"] = task_timezone

    now = datetime.now(tz=pytz.timezone(task_timezone))
    next_execution = task_cron_expression.first_within_next(get_period(), now)
    if next_execution is None:
        return safe_json(result)

    result["NextExecution"] = next_execution

    return result


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

    -CONFIG_ACCOUNTS: List of accounts to execute task for

    -CONFIG_CROSS_ACCOUNT_ROLE_NAME: Name of alternative cross account role to use instead of default role in external accounts

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

    -CONFIG_TASK_METRICS: Task metrics parameter passed to the executed action (optional, default False, boolean)

    -CONFIG_EVENTS: List of resource events that trigger the task to be executed  (optional, default, List<string>)

    -CONFIG_DRYRUN: Dryrun parameter passed to the executed action (optional, default False, boolean)

    -CONFIG_EVENTS: Resource events that trigger the task (optional, default None, dict)

    -CONFIG_EVENT_SOURCE_TAG_FILTER: Filter for tags of source resource of events (optional, default None, string)

    -CONFIG_EVENT_SCOPES: Scope to select resource events for that trigger the task (optional, default None, dict)

    -CONFIG_SELECT_Size: Size to use for selecting resources (option, default is "Standard")

    -CONFIG_EXECUTE_SIZE: Size to use for executing task action(option, default is "Standard")

    -CONFIG_COMPLETION_SIZE: Size to use for executing task completion logic (option, default is "Standard")

    -CONFIG_TASK_METRICS: Flag to indicate if metrics should be generated for the task

    :return: Item created in the task configuration
    """

    with _get_logger(context=context) as logger:
        logger.info("create_task")
        config = TaskConfiguration(context=context, logger=logger)
        name = kwargs.get(configuration.CONFIG_TASK_NAME)
        if name is None or len(name) == 0:
            raise_value_error(ERR_NO_TASK_NAME)

        item = config.get_config_item(name)
        if item is not None:
            raise_value_error(ERR_TASK_DOES_ALREADY_EXIST, name)

        new_item = config.put_config_item(**kwargs)
        return new_item


def update_task(name, context=None, **kwargs):
    """
    Updates the specified task. An exception is raised when the action does not exist.
    :param name: Name of the task. This name overwrites the name in kwargs if it is used there
    :param kwargs: Task parameters dictionary, see create_task for details.
    :param context: Lambda context
    :return: Updated task item
    """
    with _get_logger(context=context) as logger:
        config = TaskConfiguration(context=context, logger=logger)
        if name is None or len(name) == 0:
            raise_value_error(ERR_NO_TASK_NAME)
        item = config.get_config_item(name)
        if item is None:
            raise_value_error(ERR_TASK_DOES_NOT_EXIST, name)

        # copy to avoid side effects when modifying arguments
        args = copy.deepcopy(kwargs)
        args[configuration.CONFIG_TASK_NAME] = name
        stack_id = item.get(configuration.CONFIG_STACK_ID)
        if stack_id is not None:
            args[configuration.CONFIG_STACK_ID] = stack_id
        item = config.put_config_item(**args)
        return item


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
                raise_value_error(ERR_TASK_DOES_NOT_EXIST, name)
        else:
            item = {"Name": name}
        config.delete_config_item(name)
        return item


def start_task(name, context=None, task_group=None):
    with _get_logger(context=context) as logger:
        logger.info("execute_task")
        if task_group is None:
            task_group = str(uuid.uuid4())

        event = {
            handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_EVENT_SCHEDULER_EXECUTE_TASK,
            handlers.HANDLER_EVENT_TASK_NAME: name,
            handlers.HANDLER_EVENT_TASK_GROUP: task_group
        }

        result = ScheduleHandler(event=event, context=context).handle_request()
        logger.info("Execute task result is \n{}", safe_json(result, indent=3))
        return result


def start_completion_handler(context=None):
    with _get_logger(context=context) as logger:
        logger.info("execute_completion_handler")

        if not handlers.running_local(context):
            raise Exception("This action is only allowed if handler is running in local mode")

        result = CompletionHandler(event={}, context=context).handle_request()
        logger.info("Execute completion handler result is \n{}", safe_json(result, indent=3))
        return result


def get_action(name, context=None, log_this_call=True):
    """
    Gets the details of the specified action
    :param name: Name of the action, raises an exception if the action does not exist
    :param context: Lambda context
    :param log_this_call: switch
    :return: Details of the specified action.

    """
    with _get_logger(context=context) as logger:
        if log_this_call:
            logger.info("get_action")
        all_actions = actions.all_actions()
        if name not in all_actions:
            raise_value_error(ERR_ACTION_DOES_NOT_EXIST, name, ",".join(all_actions))
        return actions.get_action_properties(name)


def get_actions(context=None):
    """
    Returns  a dictionary with all available actions, see get_action for details on returned items
    :param context: Lambda context
    :return: all available action
    """
    with _get_logger(context=context) as logger:
        logger.info("get_actions")
        return {action_name: get_action(action_name, log_this_call=False) for action_name in actions.all_actions()}
