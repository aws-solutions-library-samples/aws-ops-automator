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
######################################################################################################################
BOOLEAN_FALSE_VALUES = [
    "false",
    "no",
    "disabled",
    "off",
    "0"
]

BOOLEAN_TRUE_VALUES = [
    "true",
    "yes",
    "enabled",
    "on",
    "1"
]

# name of environment variable that holds the name of the configuration table
ENV_CONFIG_TABLE = "CONFIG_TABLE"
ENV_CONFIG_BUCKET = "CONFIG_BUCKET"

TASKS_OBJECTS = "TaskConfigurationObjects"

# names of attributes in configuration
# name of the action
CONFIG_ACTION_NAME = "Action"
# debug parameter
CONFIG_DEBUG = "Debug"
# notifications for started/ended tasks
CONFIG_TASK_NOTIFICATIONS = "TaskNotifications"
# list of cross account roles
CONFIG_ACCOUNTS = "Accounts"
# name of alternative cross account role
CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME = "CrossAccountRole"
# description
CONFIG_DESCRIPTION = "Description"
# Switch to enable/disable  task
CONFIG_ENABLED = "Enabled"
# tag filter for tags of source resource of an event
CONFIG_EVENT_SOURCE_TAG_FILTER = "SourceEventTagFilter"
# cron expression interval for time/date based tasks
CONFIG_INTERVAL = "Interval"
# internal task
CONFIG_INTERNAL = "Internal"
# name of the task
CONFIG_TASK_NAME = "Name"
# parameters of a task
CONFIG_PARAMETERS = "Parameters"
# switch to indicate if resource in the account of the scheduler should be processed
CONFIG_THIS_ACCOUNT = "ThisAccount"
# timezone for time/date scheduled task
CONFIG_TIMEZONE = "Timezone"
# tag filter to select resources processed by the task
CONFIG_TAG_FILTER = "TagFilter"
# regions where to select/process resources
CONFIG_REGIONS = "Regions"
# dryrun switch, passed to the tasks action
CONFIG_DRYRUN = "Dryrun"
# events that trigger the task
CONFIG_EVENTS = "Events"
# event scopes
CONFIG_EVENT_SCOPES = "EventScopes"
# stack id if created from cloudformation stack
CONFIG_STACK_ID = "StackId"
# action timeout
CONFIG_TASK_TIMEOUT = "TaskTimeout"
# action select memory
CONFIG_TASK_SELECT_SIZE = "SelectSize"
# action select memory
CONFIG_TASK_EXECUTE_SIZE = "ExecuteSize"
# action completion memory
CONFIG_TASK_COMPLETION_SIZE = "CompletionSize"
# action completion memory when running in ECS
CONFIG_ECS_COMPLETION_MEMORY = "CompletionEcsMemoryValue"
# action select memory when running in ECS
CONFIG_ECS_SELECT_MEMORY = "SelectEcsMemoryValueValue"
# action select memory when running in ECS
CONFIG_ECS_EXECUTE_MEMORY = "ExecuteEcsMemoryValue"

# Task metrics
CONFIG_TASK_METRICS = "TaskMetrics"
