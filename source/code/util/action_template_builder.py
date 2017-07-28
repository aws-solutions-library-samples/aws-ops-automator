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

import os
from collections import OrderedDict
from decimal import Decimal

import boto3

import actions
import configuration
import handlers
import pytz
import services
from configuration.task_configuration import TaskConfiguration

PARAM_OPTIONS = [actions.PARAM_MIN_LEN,
                 actions.PARAM_MAX_LEN,
                 actions.PARAM_MIN_VALUE,
                 actions.PARAM_MAX_VALUE,
                 actions.PARAM_PATTERN,
                 actions.PARAM_ALLOWED_VALUES,
                 actions.PARAM_DESCRIPTION,
                 actions.PARAM_DEFAULT]

GROUP_LABEL_TASK_SETTINGS = "Task settings for task {}, version {} in stack {}"

PARAM_LABEL_CROSS_ACCOUNT = "Cross account roles"
PARAM_LABEL_DEBUG = "Enable debugging"
PARAM_LABEL_ENABLED = "Task enabled"
PARAM_LABEL_FILTER = "Tag filter"
PARAM_LABEL_INTERVAL = "Task interval"
PARAM_LABEL_REGIONS = "Regions"
PARAM_LABEL_TASK_DESCRIPTION = "Task description"
PARAM_LABEL_THIS_ACCOUNT = "This account"
PARAM_LABEL_TIMEZONE = "Timezone"
PARAM_LABEL_TIMEOUT = "Timeout"

PARAM_DESCRIPTION_CROSS_ACCOUNT = "List of cross account roles to run task with."
PARAM_DESCRIPTION_DEBUG = "Enable of disable logging of detailed debug information."
PARAM_DESCRIPTION_ENABLE = "Enable or disable this task."
PARAM_DESCRIPTION_INTERVAL = "Cron expression that specified when this task is executed."
PARAM_DESCRIPTION_TIMEZONE = "Timezone for scheduling the task."
PARAM_DESCRIPTION_REGIONS = "List of regions to run task. Use \"\" for region \"{}\", or * for all regions"
PARAM_DESCRIPTION_TAG_FILTER = "Optional tag filter for selecting resources instead of adding taskname to list of values in the " \
                               "tag named \"{}\"."
PARAM_DESCRIPTION_TAG_FILER_NO_WILDCARD = " Wildcard values \"*\" and \"**\" are not allowed for the name of the tag"
PARAM_DESCRIPTION_THIS_ACCOUNT = "Run task in this account."
PARAM_DESCRIPTION_TASK_DESCRIPTION = "Description for this task."
PARAM_DESCRIPTION_TIMEOUT = "Timeout in minutes for task to complete."

YES = "Yes"
NO = "No"
YES_NO = [YES, NO]

TEMPLATE_DESCRIPTION = "Task configuration for action {} version {} in stack \"{}\". This stack will be automatically deleted " \
                       "with that stack."


class ActionTemplateBuilder:
    """
    Class to build CloudFormation templates for actions
    """

    def __init__(self, context, service_token_arn=None):

        if context is not None:
            self.service_token = context.invoked_function_arn
        else:
            self.service_token = service_token_arn

        self.scheduler_stack_name = os.getenv(handlers.ENV_STACK_NAME)
        self.scheduler_tag_name = os.getenv(handlers.ENV_SCHEDULER_TAG_NAME)
        self.region = boto3.Session().region_name

        self._template = None
        self._template_parameters = None
        self._parameter_labels = None
        self._parameter_groups = None
        self._resources = None

        self.action_name = None
        self.action_properties = None
        self.action_class = None

        self.has_timeout_parameter = None
        self.has_regions_parameter = None
        self.has_cross_account_parameter = None
        self.is_regional_service = None
        self._resource_supports_tags = None

    def build_template(self, action_name):
        """
        Build a cloudformation template for the action to create tasks for that action
        :param action_name: name of the action
        :return: template as dictionary
        """
        self.action_name = action_name
        self.action_properties = actions.get_action_properties(self.action_name)
        self.action_class = actions.get_action_class(self.action_name)

        self.has_timeout_parameter = getattr(self.action_class, handlers.COMPLETION_METHOD, None)
        self.has_cross_account_parameter = self.action_properties.get(actions.ACTION_CROSS_ACCOUNT, True)
        self.is_regional_service = services.get_service_class(self.action_properties[actions.ACTION_SERVICE]).is_regional()
        self.has_regions_parameter = self.is_regional_service and self.action_properties.get(actions.ACTION_MULTI_REGION, True)

        self._setup_template()
        self._setup_common_parameters()
        self._setup_action_parameters()
        self._setup_resources()

        return self._template

    def _setup_template(self):
        """
        Initial setup of template
        :return: 
        """
        self._template = OrderedDict()
        self._template["AWSTemplateFormatVersion"] = "2010-09-09"
        self._template["Description"] = TEMPLATE_DESCRIPTION.format(self.action_properties[actions.ACTION_TITLE],
                                                                    self.action_properties[actions.ACTION_VERSION],
                                                                    self.scheduler_stack_name)
        self._template_parameters = OrderedDict()
        self._template["Parameters"] = self._template_parameters
        self._parameter_labels = OrderedDict()
        self._parameter_groups = []
        self._template["Metadata"] = {
            "AWS::CloudFormation::Interface": {
                "ParameterGroups": self._parameter_groups,
                "ParameterLabels": self._parameter_labels
            }
        }
        self._resources = OrderedDict()
        self._template["Resources"] = self._resources

    def _setup_common_parameters(self):
        """
        Setup non action specific parameters
        :return: 
        """

        def setup_region_parameter():
            """
            Create the region parameter
            :return: 
            """
            if self.has_regions_parameter:
                self._template_parameters[configuration.CONFIG_REGIONS] = {
                    "Type": "CommaDelimitedList",
                    "Default": self.region,
                    "Description": PARAM_DESCRIPTION_REGIONS.format(self.region)
                }
                self._parameter_labels[configuration.CONFIG_REGIONS] = {"default": PARAM_LABEL_REGIONS}
                self._parameter_groups[0]["Parameters"].insert(2, configuration.CONFIG_REGIONS)

        def setup_cross_account_parameters():
            """
            Creates cross account parameter
            :return: 
            """

            if self.action_properties.get(actions.ACTION_CROSS_ACCOUNT, True):

                self._template_parameters[configuration.CONFIG_THIS_ACCOUNT] = {
                    "Type": "String",
                    "AllowedValues": YES_NO,
                    "Default": YES,
                    "Description": PARAM_DESCRIPTION_THIS_ACCOUNT
                }

                order = 3 if self.is_regional_service else 2
                self._parameter_labels[configuration.CONFIG_THIS_ACCOUNT] = {"default": PARAM_LABEL_THIS_ACCOUNT}
                self._parameter_groups[0]["Parameters"].insert(order, configuration.CONFIG_THIS_ACCOUNT)

                self._template_parameters[configuration.CONFIG_CROSS_ACCOUNT_ROLES] = {
                    "Type": "CommaDelimitedList",
                    "Description": PARAM_DESCRIPTION_CROSS_ACCOUNT
                }
                self._parameter_labels[configuration.CONFIG_CROSS_ACCOUNT_ROLES] = {"default": PARAM_LABEL_CROSS_ACCOUNT}
                self._parameter_groups[0]["Parameters"].insert(order + 1, configuration.CONFIG_CROSS_ACCOUNT_ROLES)

        def setup_timeout_parameter():
            """
            Creates a timeout parameter if the task has a completion check method
            :return: 
            """

            if not self.has_timeout_parameter:
                return

            timeout = self.action_properties.get(actions.ACTION_COMPLETION_TIMEOUT_MINUTES,
                                                 actions.DEFAULT_COMPLETION_TIMEOUT_MINUTES_DEFAULT)

            self._template_parameters[configuration.CONFIG_TASK_TIMEOUT] = {
                "Type": "Number",
                "MinValue": 1,
                "Default": str(timeout),
                "Description": PARAM_DESCRIPTION_TIMEOUT
            }

            self._parameter_labels[configuration.CONFIG_TASK_TIMEOUT] = {"default": PARAM_LABEL_TIMEOUT}
            self._parameter_groups[0]["Parameters"].insert(2, configuration.CONFIG_TASK_TIMEOUT)

        def setup_tag_filter_parameter():

            # test if the resource support tags
            action_resources = self.action_properties.get(actions.ACTION_RESOURCES)
            service = self.action_properties[actions.ACTION_SERVICE]
            self._resource_supports_tags = action_resources and action_resources in services.create_service(
                service).resources_with_tags

            if not self._resource_supports_tags:
                return

            # task tag filter
            self._template_parameters[configuration.CONFIG_TAG_FILTER] = {
                "Type": "String",
                "Description": PARAM_DESCRIPTION_TAG_FILTER.format(self.scheduler_tag_name)
            }

            if not (self.action_properties.get(actions.ACTION_ALLOW_TAGFILTER_WILDCARD, True)):
                self._template_parameters[configuration.CONFIG_TAG_FILTER]["Description"] += PARAM_DESCRIPTION_TAG_FILER_NO_WILDCARD

            self._parameter_labels[configuration.CONFIG_TAG_FILTER] = {"default": PARAM_LABEL_FILTER}
            self._parameter_groups[0]["Parameters"].insert(2, configuration.CONFIG_TAG_FILTER)

        self._template_parameters.update(
            {
                # task interval
                configuration.CONFIG_INTERVAL: {
                    "Type": "String",
                    "MinLength": "9",
                    "Description": PARAM_DESCRIPTION_INTERVAL
                },
                # task timezone
                configuration.CONFIG_TIMEZONE: {
                    "Type": "String",
                    "Default": "UTC",
                    "AllowedValues": pytz.all_timezones,
                    "Description": PARAM_DESCRIPTION_TIMEZONE
                },

                # task enabled parameter
                configuration.CONFIG_ENABLED: {
                    "Type": "String",
                    "Default": YES,
                    "AllowedValues": YES_NO,
                    "Description": PARAM_DESCRIPTION_ENABLE

                },
                # task debug switch parameter
                configuration.CONFIG_DEBUG: {
                    "Type": "String",
                    "Default": NO,
                    "AllowedValues": YES_NO,
                    "Description": PARAM_DESCRIPTION_DEBUG
                },
                configuration.CONFIG_DESCRIPTION: {
                    "Type": "String",
                    "Description": PARAM_DESCRIPTION_TASK_DESCRIPTION
                }
            })

        self._parameter_labels.update(
            {
                # parameter labels
                configuration.CONFIG_INTERVAL: {"default": PARAM_LABEL_INTERVAL},
                configuration.CONFIG_TIMEZONE: {"default": PARAM_LABEL_TIMEZONE},
                configuration.CONFIG_ENABLED: {"default": PARAM_LABEL_ENABLED},
                configuration.CONFIG_DEBUG: {"default": PARAM_LABEL_DEBUG},
                configuration.CONFIG_DESCRIPTION: {"default": PARAM_LABEL_TASK_DESCRIPTION}
            })

        self._parameter_groups.append(
            {
                # parameter groups
                "Label": {
                    "default": GROUP_LABEL_TASK_SETTINGS.format(self.action_properties[actions.ACTION_TITLE],
                                                                self.action_properties[actions.ACTION_VERSION],
                                                                os.getenv(handlers.ENV_STACK_NAME, "")),
                },
                "Parameters": [configuration.CONFIG_DESCRIPTION,
                               configuration.CONFIG_INTERVAL,
                               configuration.CONFIG_TIMEZONE,
                               configuration.CONFIG_ENABLED,
                               configuration.CONFIG_DEBUG]
            })

        setup_region_parameter()
        setup_cross_account_parameters()
        setup_tag_filter_parameter()
        setup_timeout_parameter()

    def _setup_action_parameters(self):
        """
        Creates the action specific parameters from its metadata
        :return: 
        """

        def setup_action_parameter_groups():
            """
            Action parameter groups
            :return: 
            """
            for group in self.action_properties.get(actions.ACTION_PARAMETER_GROUPS, []):
                self._parameter_groups.append({
                    "Label": {
                        "default": group.get(actions.ACTION_PARAMETER_GROUP_TITLE, "")
                    },
                    "Parameters": group.get(actions.ACTION_PARAMETER_GROUP_LIST)
                })

        def setup_action_parameter(name, action_parameter):
            # single action parameter setup
            parameter_template = {}

            # parameter type
            parameter_type = action_parameter[actions.PARAM_TYPE]
            if parameter_type in [int, long, float, Decimal]:
                parameter_template["Type"] = "Number"
            elif isinstance([], parameter_type):
                parameter_template["Type"] = "CommaDelimitedList"
            else:
                parameter_template["Type"] = "String"
                if action_parameter.get(actions.PARAM_REQUIRED, False) and actions.PARAM_MIN_LEN not in action_parameter:
                    parameter_template[actions.PARAM_MIN_LEN] = 1
                # default allowed values for booleans
                if parameter_type == bool:
                    parameter_template["AllowedValues"] = YES_NO

            # for every parameter option...
            for p in PARAM_OPTIONS:
                if p in action_parameter:
                    if p == actions.PARAM_DEFAULT:
                        if parameter_type in [bool]:
                            value = TaskConfiguration.as_boolean(action_parameter[actions.PARAM_DEFAULT])
                            parameter_template[p] = YES if value else NO
                        continue

                    if isinstance(action_parameter[p], type([])):
                        parameter_template[p] = action_parameter[p]
                    else:
                        parameter_template[p] = str(action_parameter[p])

            # add parameter to template
            self._template_parameters[name] = parameter_template

            # add label
            if actions.PARAM_LABEL in action_parameter:
                self._parameter_labels[name] = {"default": action_parameter[actions.PARAM_LABEL]}

        # setup all parameters for an action
        for parameter_name, parameter in self.action_properties.get(actions.ACTION_PARAMETERS, {}).iteritems():
            setup_action_parameter(parameter_name, parameter)

        setup_action_parameter_groups()

    def _setup_resources(self):
        """
        Setup action custom resource that creates the task
        :return: 
        """

        task_resource = {
            "Type": "Custom::TaskConfig",

            "Properties": {
                "Name": {"Ref": "AWS::StackName"},
                "ServiceToken": self.service_token,
                # This is the timeout in seconds for the custom resource to complete
                "Timeout": str(60),
                "Action": self.action_name,
                configuration.CONFIG_INTERVAL: {"Ref": configuration.CONFIG_INTERVAL},
                configuration.CONFIG_TIMEZONE: {"Ref": configuration.CONFIG_TIMEZONE},
                configuration.CONFIG_ENABLED: {"Ref": configuration.CONFIG_ENABLED},
                configuration.CONFIG_DEBUG: {"Ref": configuration.CONFIG_DEBUG},
                configuration.CONFIG_DESCRIPTION: {"Ref": configuration.CONFIG_DESCRIPTION},
                configuration.CONFIG_THIS_ACCOUNT: {"Ref": configuration.CONFIG_THIS_ACCOUNT},
                configuration.CONFIG_PARAMETERS: {j: {"Ref": j} for j in self.action_properties[actions.ACTION_PARAMETERS]},
                configuration.CONFIG_STACK_ID: {"Ref": "AWS::StackId"},
            }
        }

        # this is the configured timeout in minutes for the task to complete
        if self.has_timeout_parameter:
            task_resource["Properties"][configuration.CONFIG_TASK_TIMEOUT] = {
                "Ref": configuration.CONFIG_TASK_TIMEOUT
            }
        if self.has_regions_parameter:
            task_resource["Properties"][configuration.CONFIG_REGIONS] = {
                "Ref": configuration.CONFIG_REGIONS
            }
        if self.has_cross_account_parameter:
            task_resource["Properties"][configuration.CONFIG_CROSS_ACCOUNT_ROLES] = {
                "Ref": configuration.CONFIG_CROSS_ACCOUNT_ROLES
            }

        if self._resource_supports_tags:
            task_resource["Properties"][configuration.CONFIG_TAG_FILTER] = {
                "Ref": configuration.CONFIG_TAG_FILTER
            }

        self._resources["Task"] = task_resource
