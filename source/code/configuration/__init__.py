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

BOOLEAN_FALSE_VALUES = ["false", "no", "disabled", "off", "0"]
BOOLEAN_TRUE_VALUES = ["true", "yes", "enabled", "on", "1"]

# name of environment variable that holds the name of the configuration table
ENV_CONFIG_TABLE = "CONFIG_TABLE"

ENV_CONFIG_BUCKET = "CONFIG_BUCKET"

TASK_ROLES_FOLDER = "TaskRoles/"

# names of attributes in configuration
# name of the action
CONFIG_ACTION_NAME = "Action"
# debug parameter
CONFIG_DEBUG = "Debug"
# list of cross account roles
CONFIG_CROSS_ACCOUNT_ROLES = "CrossAccountRoles"
# description
CONFIG_DESCRIPTION = "Description"
# Switch to enable/disable  task
CONFIG_ENABLED = "Enabled"
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
# stack id if created from cloudformation stack
CONFIG_STACK_ID = "StackId"
# action timeout
CONFIG_TASK_TIMEOUT = "TaskTimeout"



