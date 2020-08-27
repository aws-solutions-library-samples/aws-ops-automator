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
from collections import OrderedDict
from decimal import Decimal

import actions
import configuration
import handlers.ebs_snapshot_event_handler
import handlers.ec2_state_event_handler
import handlers.ec2_tag_event_handler
import handlers.rds_event_handler
import handlers.rds_tag_event_handler
import handlers.s3_event_handler
import pytz
from handlers.event_handler_base import *

PARAM_OPTIONS = [
    actions.PARAM_MIN_LEN,
    actions.PARAM_MAX_LEN,
    actions.PARAM_MIN_VALUE,
    actions.PARAM_MAX_VALUE,
    actions.PARAM_PATTERN,
    actions.PARAM_ALLOWED_VALUES,
    actions.PARAM_DESCRIPTION,
    actions.PARAM_DEFAULT
]

GROUP_LABEL_TASK_SETTINGS = "Task settings for task {}, version {} in stack {}"
PARAM_GROUP_EVENT_FILTERING = "Event source filtering"

PARAM_LABEL_CROSS_ACCOUNT_ROLE_NAME = "Cross account role name"
PARAM_LABEL_ACCOUNTS = "Accounts"
PARAM_LABEL_DEBUG = "Enable debugging"
PARAM_LABEL_ENABLED = "Task enabled"
PARAM_LABEL_EVENT_SOURCE_RESOURCE_TAG_FILTER = "Event source tag filter"
PARAM_LABEL_FILTER = "Tag filter"
PARAM_LABEL_INTERVAL = "Task interval"
PARAM_LABEL_METRICS = "Collect Metrics"
PARAM_LABEL_REGIONS = "Regions"
PARAM_LABEL_TASK_DESCRIPTION = "Task description"
PARAM_LABEL_THIS_ACCOUNT = "This account"
PARAM_LABEL_TIMEZONE = "Timezone"
PARAM_LABEL_TIMEOUT = "Timeout"
PARAM_LABEL_COMPLETION_MEMORY = "Completion test memory"
PARAM_LABEL_EXECUTION_MEMORY = "Execution memory"
PARAM_LABEL_SELECT_MEMORY = "Resource selection memory"
PARAM_LABEL_ECS_SELECT_MEMORY = "Selection reserved memory"
PARAM_LABEL_ECS_EXEC_MEMORY = "Execution reserved memory"
PARAM_LABEL_SCOPE = "Resource selection scope for {} event"

PARAM_DESCRIPTION_EVENT_SCOPE = \
    "Resource selection scope when task is executed for the \"{}\" event. When set to \"resource\" only the resource that is the" \
    " source of the event, is used for selecting resources for this task. If set to \"region\" all resources in the region " \
    "of the source resource type are used in the selection."
PARAM_DESCRIPTION_NOTIFICATIONS = \
    "Send SNS notification for started/ended tasks."
PARAM_DESCRIPTION_CROSS_ACCOUNT_ROLE_NAME = \
    "Name of across-account role to use for accounts to execute this task, leave blank to use default name \"{}\". If the " \
    "default role does not exist, and the task is running for this account ({}), then the Ops Automator role \"{}\" from " \
    "stack \"{}\" is used."
PARAM_DESCRIPTION_ACCOUNTS = \
    "Comma separated list of accounts to run this task for."
PARAM_DESCRIPTION_DEBUG = \
    "Enable of disable logging of detailed debug information."
PARAM_DESCRIPTION_ENABLE = \
    "Enable or disable this task."
PARAM_DESCRIPTION_EVENT_SOURCE_RESOURCE_TAG_FILTER = \
    "Tag filter used to filter events that can trigger this task. The filter is applied on the tags of the resource that is " \
    "the source resource the event. Use of this filter is recommended when the scope of any event used to trigger this task is " \
    "set to  \"region\", or an event is used from a different service than {}."
PARAM_DESCRIPTION_INTERVAL = \
    "Cron expression that specifies the interval for this task being executed."
PARAM_DESC_INTERVAL_MIN = \
    "{} (Minimum interval between task executions for this action must be at least {} minutes)."
PARAM_DESCRIPTION_METRICS = \
    "Collect detailed CloudWatch metrics for this task."
PARAM_DESCRIPTION_TIMEZONE = \
    "Timezone for scheduling the task."
PARAM_DESCRIPTION_REGIONS = \
    "List of regions to run task in."
PARAM_DESCRIPTION_TAG_FILTER = \
    "Optional tag filter for selecting resources instead of adding taskname to list of values in the " \
    "tag named \"{}\"."
PARAM_DESCRIPTION_TAG_FILER_NO_WILDCARD = \
    "Wildcard values \"*\" and \"**\" are not allowed for the name of the tag."
PARAM_DESCRIPTION_THIS_ACCOUNT = \
    "Run task in this account ({})."
PARAM_DESCRIPTION_TASK_DESCRIPTION = \
    "Description for this task."
PARAM_DESCRIPTION_TIMEOUT = \
    "Timeout in minutes for task to complete."
PARAM_DESCRIPTION_COMPLETION_SIZE = \
    "Lambda memory size for executing task completion logic. {}"
PARAM_DESCRIPTION_EXECUTE_SIZE = \
    "Lambda memory size for executing the task action {}."
PARAM_DESCRIPTION_SELECT_SIZE = \
    " Lambda memory size for selecting task resources.{}."
PARAM_DESCRIPTION_ECS_SELECT_MEMORY = \
    "Reserved memory (MB) for container to select resources in ECS task. This value is only used if ECS is selected as value for " \
    "{} parameter.".format(PARAM_LABEL_SELECT_MEMORY.lower())
PARAM_DESCRIPTION_ECS_EXECUTE_MEMORY = \
    "Reserved memory (MB) for container to execute action in ECS task. This value is only used if ECS is selected as value for " \
    "{} parameter.".format(PARAM_LABEL_EXECUTION_MEMORY.lower())
PARAM_DESCRIPTION_ECS_COMPLETION_MEMORY = \
    "Reserved memory (MB) in MB for container to execute task completion logic in ECS task. This value is only used if ECS is " \
    "selected as the value for {} parameter.".format(PARAM_LABEL_COMPLETION_MEMORY.lower())

YES = "Yes"
NO = "No"
YES_NO = [YES, NO]

TEMPLATE_DESCRIPTION = "Task configuration for action {} version {} in stack \"{}\". This stack will be automatically deleted " \
                       "when that stack is deleted.(SO0029-{})"

EVENTS = [
    (handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_NOTIFICATION, handlers.ebs_snapshot_event_handler.HANDLED_EVENTS),
    (handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION, handlers.ec2_state_event_handler.HANDLED_EVENTS),
    (handlers.rds_event_handler.RDS_AWS_API_CALL, handlers.rds_event_handler.HANDLED_EVENTS),
    (handlers.s3_event_handler.S3_OBJECT_CREATED, handlers.s3_event_handler.HANDLED_EVENTS),
    (handlers.TAG_CHANGE_EVENT, handlers.ec2_tag_event_handler.HANDLED_EVENTS),
    (handlers.TAG_CHANGE_EVENT, handlers.rds_tag_event_handler.HANDLED_EVENTS)
]

EVENTS_HANDLER = "EventHandler"
OUTPUT_DESC_EVENT_HANDLER = "Lambda function for events that trigger tasks"


class ActionTemplateBuilder(object):
    """
    Class to build CloudFormation templates for actions
    """

    def __init__(self, context, service_token_arn=None, ops_automator_role=None, use_ecs=False):

        self._context = context,

        if context is not None:
            self.service_token = context.invoked_function_arn
        else:
            self.service_token = service_token_arn

        self.automator_stack_name = os.getenv(handlers.ENV_STACK_NAME)
        self.automator_tag_name = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME)
        self.config_bucket = os.getenv(handlers.ENV_CONFIG_BUCKET)
        self.region = services.get_session().region_name
        self.aws_account = services.get_aws_account()
        self.ops_automator_role = ops_automator_role
        self.use_ecs = use_ecs

        self._template = None
        self._template_parameters = None
        self._parameter_labels = None
        self._parameter_groups = None
        self._resources = None
        self._ops_automator_stack_template = None

        self.action_name = None
        self.action_properties = None
        self.action_class = None

        self.has_completion_logic = None
        self.has_regions_parameter = None
        self.has_cross_account_parameter = None
        self.is_regional_service = None
        self._action_events = {}
        self._action_scopes = {}
        self._resource_supports_tags = None
        self.use_events = None
        self.use_intervals = None
        self.interval_min = 0

    @classmethod
    def _format_event_name(cls, event_name):
        return event_name[0].upper() + event_name[1:]

    @property
    def ops_automator_stack_template(self):
        if self._ops_automator_stack_template is None:
            cfn = get_client_with_retries("cloudformation", methods=["get_template"], context=self._context)
            ops_automator_name = os.getenv(handlers.ENV_STACK_NAME, "")
            self._ops_automator_stack_template = cfn.get_template_with_retries(StackName=ops_automator_name).get("TemplateBody", {})
        return self._ops_automator_stack_template

    def build_template(self, action_name):
        """
        Build a cloudformation template for the action to create tasks for that action
        :param action_name: name of the action
        :return: template as dictionary
        """
        self.action_name = action_name
        self.action_properties = actions.get_action_properties(self.action_name)
        self.action_class = actions.get_action_class(self.action_name)

        self.has_completion_logic = getattr(self.action_class, handlers.COMPLETION_METHOD, None)
        self.has_cross_account_parameter = self.action_properties.get(actions.ACTION_CROSS_ACCOUNT, True)
        self.is_regional_service = services.get_service_class(self.action_properties[actions.ACTION_SERVICE]).is_regional()
        self.has_regions_parameter = self.is_regional_service and self.action_properties.get(actions.ACTION_MULTI_REGION, True)
        self.use_intervals = actions.ACTION_TRIGGER_INTERVAL[0] in self.action_properties.get(actions.ACTION_TRIGGERS,
                                                                                              actions.ACTION_TRIGGER_BOTH)
        self.interval_min = self.action_properties.get(actions.ACTION_MIN_INTERVAL_MIN, 0)
        self.use_events = actions.ACTION_TRIGGER_EVENTS[0] in self.action_properties.get(actions.ACTION_TRIGGERS,
                                                                                         actions.ACTION_TRIGGER_BOTH)

        self._setup_template()
        self._setup_common_parameters()
        self._setup_action_parameters()
        self._setup_resources()
        self._setup_outputs()

        for p in self._template_parameters:
            description = self._template_parameters[p]["Description"]
            if description not in ["", None] and not description.endswith("."):
                self._template_parameters[p]["Description"] += "."

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
                                                                    self.automator_stack_name,
                                                                    self.action_name)
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
        self._outputs = OrderedDict()

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
                    "Description": PARAM_DESCRIPTION_THIS_ACCOUNT.format(services.get_aws_account())
                }

                order = 3 if self.is_regional_service else 2
                self._parameter_labels[configuration.CONFIG_THIS_ACCOUNT] = {"default": PARAM_LABEL_THIS_ACCOUNT}
                self._parameter_groups[0]["Parameters"].insert(order, configuration.CONFIG_THIS_ACCOUNT)

                self._template_parameters[configuration.CONFIG_ACCOUNTS] = {
                    "Type": "CommaDelimitedList",
                    "Description": PARAM_DESCRIPTION_ACCOUNTS
                }
                self._parameter_labels[configuration.CONFIG_ACCOUNTS] = {"default": PARAM_LABEL_ACCOUNTS}
                self._parameter_groups[0]["Parameters"].insert(order + 1, configuration.CONFIG_ACCOUNTS)

                self._template_parameters[configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME] = {
                    "Type": "String",
                    "Description": PARAM_DESCRIPTION_CROSS_ACCOUNT_ROLE_NAME.format(handlers.default_rolename_for_stack(),
                                                                                    self.aws_account,
                                                                                    self.ops_automator_role.split("/")[-1],
                                                                                    self.automator_stack_name)
                }
                self._parameter_labels[configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME] = {
                    "default": PARAM_LABEL_CROSS_ACCOUNT_ROLE_NAME
                }
                self._parameter_groups[0]["Parameters"].insert(order + 2, configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME)

        def setup_timeout_parameter():
            """
            Creates a timeout parameter if the task has a completion check method
            :return: 
            """

            if not self.has_completion_logic:
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

            if self.action_properties.get(actions.ACTION_NO_TAG_SELECT, False):
                return

            # test if the resource/service supports tags
            action_resources = self.action_properties.get(actions.ACTION_RESOURCES, "")
            service = self.action_properties[actions.ACTION_SERVICE]
            service_resource_with_tags = services.create_service(service).resources_with_tags

            if action_resources == "":
                self._resource_supports_tags = len(service_resource_with_tags) != 0
            else:
                self._resource_supports_tags = action_resources.lower() in [r.lower() for r in service_resource_with_tags]

            if not self._resource_supports_tags:
                return

            # task tag filter
            self._template_parameters[configuration.CONFIG_TAG_FILTER] = {
                "Type": "String",
                "Description": PARAM_DESCRIPTION_TAG_FILTER.format(self.automator_tag_name)
            }

            if not (self.action_properties.get(actions.ACTION_ALLOW_TAGFILTER_WILDCARD, True)):
                self._template_parameters[configuration.CONFIG_TAG_FILTER]["Description"] += PARAM_DESCRIPTION_TAG_FILER_NO_WILDCARD

            self._parameter_labels[configuration.CONFIG_TAG_FILTER] = {"default": PARAM_LABEL_FILTER}
            self._parameter_groups[0]["Parameters"].insert(2, configuration.CONFIG_TAG_FILTER)

        def build_memory_parameter(size_group, lambda_size, lambda_size_param, config_ecs_memory_param, description, label,
                                   ecs_memory_label, ecs_description):

            memory_settings = self.ops_automator_stack_template["Mappings"]["Settings"]["ActionMemory"]
            memory_options = self.action_properties.get(lambda_size, [])

            if not self.use_ecs and actions.ACTION_USE_ECS in memory_options:
                del memory_options[memory_options.index(actions.ACTION_USE_ECS)]

            if len(memory_options) > 1:
                self._template_parameters[lambda_size_param] = {
                    "Type": "String",
                    "AllowedValues": memory_options,
                    "Default": memory_options[0],
                    "Description": description.format(", ".join(
                        "{} {}{} {}".format(m,
                                            "(" if memory_settings[m] != "" else "", memory_settings[m],
                                            "MB)" if memory_settings[m] != "" else "") for
                        m in memory_options))
                }

                self._parameter_labels[lambda_size_param] = {
                    "default": label
                }
                size_group["Parameters"].append(lambda_size_param)

            if actions.ACTION_USE_ECS in memory_options:
                self._template_parameters[config_ecs_memory_param] = {
                    "Type": "Number",
                    "MinValue": 8,
                    "Default": 128,
                    "Description": ecs_description
                }

                self._parameter_labels[config_ecs_memory_param] = {
                    "default": ecs_memory_label
                }

                size_group["Parameters"].append(config_ecs_memory_param)

        def setup_memory_parameters():

            memory_group = {
                "Label": {
                    "default": "Task memory allocation settings",
                },
                "Parameters": []
            }

            build_memory_parameter(size_group=memory_group,
                                   lambda_size=actions.ACTION_SELECT_SIZE,
                                   lambda_size_param=configuration.CONFIG_TASK_SELECT_SIZE,
                                   config_ecs_memory_param=configuration.CONFIG_ECS_SELECT_MEMORY,
                                   description=PARAM_DESCRIPTION_SELECT_SIZE,
                                   label=PARAM_LABEL_SELECT_MEMORY,
                                   ecs_memory_label=PARAM_LABEL_ECS_SELECT_MEMORY,
                                   ecs_description=PARAM_DESCRIPTION_ECS_SELECT_MEMORY)

            build_memory_parameter(size_group=memory_group,
                                   lambda_size=actions.ACTION_EXECUTE_SIZE,
                                   lambda_size_param=configuration.CONFIG_TASK_EXECUTE_SIZE,
                                   config_ecs_memory_param=configuration.CONFIG_ECS_EXECUTE_MEMORY,
                                   description=PARAM_DESCRIPTION_EXECUTE_SIZE,
                                   label=PARAM_LABEL_EXECUTION_MEMORY,
                                   ecs_memory_label=PARAM_LABEL_ECS_EXEC_MEMORY,
                                   ecs_description=PARAM_DESCRIPTION_ECS_EXECUTE_MEMORY)

            if self.has_completion_logic:
                build_memory_parameter(size_group=memory_group,
                                       lambda_size=actions.ACTION_COMPLETION_SIZE,
                                       lambda_size_param=configuration.CONFIG_TASK_COMPLETION_SIZE,
                                       config_ecs_memory_param=configuration.CONFIG_ECS_COMPLETION_MEMORY,
                                       description=PARAM_DESCRIPTION_COMPLETION_SIZE,
                                       label=PARAM_LABEL_COMPLETION_MEMORY,
                                       ecs_memory_label=PARAM_LABEL_COMPLETION_MEMORY,
                                       ecs_description=PARAM_DESCRIPTION_ECS_COMPLETION_MEMORY)

            if len(memory_group["Parameters"]) > 0:
                self._parameter_groups.append(memory_group)

        self._template_parameters.update(
            {
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
                # task debug switch parameter
                configuration.CONFIG_TASK_NOTIFICATIONS: {
                    "Type": "String",
                    "Default": NO,
                    "AllowedValues": YES_NO,
                    "Description": PARAM_DESCRIPTION_NOTIFICATIONS
                },
                configuration.CONFIG_TASK_METRICS: {
                    "Type": "String",
                    "Default": NO,
                    "AllowedValues": YES_NO,
                    "Description": PARAM_DESCRIPTION_METRICS

                },
                configuration.CONFIG_DESCRIPTION: {
                    "Type": "String",
                    "Description": PARAM_DESCRIPTION_TASK_DESCRIPTION
                }
            })

        if self.use_intervals:
            desc = PARAM_DESCRIPTION_INTERVAL if self.interval_min == 0 else PARAM_DESC_INTERVAL_MIN.format(
                PARAM_DESCRIPTION_INTERVAL, self.interval_min)
            self._template_parameters.update({
                # task interval
                configuration.CONFIG_INTERVAL: {
                    "Type": "String",
                    "Description": desc
                },
                # task timezone
                configuration.CONFIG_TIMEZONE: {
                    "Type": "String",
                    "Default": "UTC",
                    "AllowedValues": pytz.all_timezones,
                    "Description": PARAM_DESCRIPTION_TIMEZONE
                }
            })

            self._parameter_labels.update({
                configuration.CONFIG_INTERVAL: {"default": PARAM_LABEL_INTERVAL},
                configuration.CONFIG_TIMEZONE: {"default": PARAM_LABEL_TIMEZONE}
            })

        self._parameter_labels.update(
            {
                # parameter labels
                configuration.CONFIG_ENABLED: {"default": PARAM_LABEL_ENABLED},
                configuration.CONFIG_DEBUG: {"default": PARAM_LABEL_DEBUG},
                configuration.CONFIG_TASK_METRICS: {"default": PARAM_LABEL_METRICS},
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
                "Parameters": [
                    configuration.CONFIG_DESCRIPTION,
                    configuration.CONFIG_INTERVAL,
                    configuration.CONFIG_TIMEZONE,
                    configuration.CONFIG_ENABLED,
                    configuration.CONFIG_TASK_METRICS,
                    configuration.CONFIG_TASK_NOTIFICATIONS,
                    configuration.CONFIG_DEBUG
                ] if self.use_intervals else
                [
                    configuration.CONFIG_DESCRIPTION,
                    configuration.CONFIG_ENABLED,
                    configuration.CONFIG_TASK_METRICS,
                    configuration.CONFIG_TASK_NOTIFICATIONS,
                    configuration.CONFIG_DEBUG
                ]
            })

        def setup_event_parameters():

            self._action_events = {}
            self._action_scopes = {}

            if not self.use_events:
                return

            action_events = self.action_properties.get(actions.ACTION_EVENTS, {})
            action_event_scopes = self.action_properties.get(actions.ACTION_EVENT_SCOPES, {})

            # interval is required if there no events
            if len(action_events) == 0:
                interval_parameter = self._template["Parameters"][configuration.CONFIG_INTERVAL]
                interval_parameter["MinLength"] = 9

            for e in EVENTS:
                event_detail_type = e[0]
                handled_events = e[1]

                source = handled_events[EVENT_SOURCE]
                action_handled_details_types = action_events.get(source, {})
                action_handled_events = action_handled_details_types.get(event_detail_type, [])

                event_detail_scopes = action_event_scopes.get(source, {}).get(event_detail_type, {})

                if len(action_handled_events) > 0:

                    parameter_group = {
                        "Label": {
                            "default": handled_events["title"]
                        },
                        "Parameters": []
                    }

                    for event_name in sorted(action_handled_events):
                        # if not event_name in handled_events:
                        # continue
                        event_cloudformation_parameter = {

                            "Type": "String",
                            "Default": NO,
                            "AllowedValues": YES_NO,
                            "Description": handled_events["events"][event_name][EVENT_DESCRIPTION]
                        }
                        param_name = handled_events[EVENT_PARAMETER].format(self._format_event_name(event_name))

                        self._template_parameters[param_name] = event_cloudformation_parameter
                        parameter_group["Parameters"].append(param_name)

                        event_param_label = handled_events[EVENT_EVENTS][event_name][EVENT_LABEL]
                        self._parameter_labels[param_name] = {
                            "default": event_param_label
                        }

                        if source not in self._action_events:
                            self._action_events[source] = {}
                        if event_detail_type not in self._action_events[source]:
                            self._action_events[source][event_detail_type] = []
                        self._action_events[source][event_detail_type].append(event_name)

                        event_scope = event_detail_scopes.get(event_name, handlers.EVENT_SCOPE_RESOURCE)
                        if event_scope == handlers.EVENT_SCOPE_REGION:
                            event_source_cloudformation_parameter = {
                                "Type": "String",
                                "Default": handlers.EVENT_SCOPE_RESOURCE,
                                "AllowedValues": [handlers.EVENT_SCOPE_RESOURCE,
                                                  handlers.EVENT_SCOPE_REGION],
                                "Description": PARAM_DESCRIPTION_EVENT_SCOPE.format(event_name)
                            }
                            param_name = handled_events[EVENT_SCOPE_PARAMETER].format(self._format_event_name(event_name))

                            self._template_parameters[param_name] = event_source_cloudformation_parameter

                            parameter_group["Parameters"].append(param_name)

                            self._parameter_labels[param_name] = {
                                "default": PARAM_LABEL_SCOPE.format(event_param_label.lower())
                            }

                            if source not in self._action_scopes:
                                self._action_scopes[source] = {}
                            if event_detail_type not in self._action_scopes[source]:
                                self._action_scopes[source][event_detail_type] = []
                            self._action_scopes[source][event_detail_type].append(event_name)

                    if len(parameter_group["Parameters"]) > 0:
                        self._parameter_groups.append(parameter_group)

            # actions that can react to events with a wider scope than the resource that cause the event can filter on
            # the tags of the source resource that cause the event
            if self.action_properties.get(actions.ACTION_PARAM_EVENT_SOURCE_TAG_FILTER, False):
                self._template_parameters[configuration.CONFIG_EVENT_SOURCE_TAG_FILTER] = {
                    "Type": "String",
                    "Description": PARAM_DESCRIPTION_EVENT_SOURCE_RESOURCE_TAG_FILTER.format(
                        self.action_properties[actions.ACTION_SERVICE].upper())
                }
                self._parameter_groups.append(
                    {
                        "Label": {
                            "default": PARAM_GROUP_EVENT_FILTERING
                        },
                        "Parameters": [configuration.CONFIG_EVENT_SOURCE_TAG_FILTER]
                    })
                self._parameter_labels[configuration.CONFIG_EVENT_SOURCE_TAG_FILTER] = {
                    "default": PARAM_LABEL_EVENT_SOURCE_RESOURCE_TAG_FILTER
                }

        setup_region_parameter()
        setup_cross_account_parameters()
        setup_tag_filter_parameter()
        setup_timeout_parameter()
        setup_event_parameters()
        setup_memory_parameters()

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

            if action_parameter.get(actions.PARAM_TYPE_AWS, None):
                parameter_template["Type"] = action_parameter[actions.PARAM_TYPE_AWS]
            else:
                if parameter_type in [int, int, float, Decimal]:
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

                    if p == actions.PARAM_ALLOWED_VALUES and action_parameter[p] in [[], None, ""]:
                        continue

                    if p == actions.PARAM_DEFAULT and parameter_type in [bool]:
                        value = TaskConfiguration.as_boolean(action_parameter[actions.PARAM_DEFAULT])
                        parameter_template[p] = YES if value else NO
                    else:
                        if isinstance(action_parameter[p], type([])):
                            parameter_template[p] = action_parameter[p]
                        else:
                            parameter_template[p] = str(action_parameter[p])

                    if p == actions.PARAM_DESCRIPTION:
                        parameter_template[p] = parameter_template[p] \
                            .replace("{ops-automator-role}", self.ops_automator_role) \
                            .replace("{region}", self.region) \
                            .replace("{account}", self.aws_account) \
                            .replace("{config-bucket}", self.config_bucket)

            # add parameter to template
            self._template_parameters[name] = parameter_template

            # add label
            if actions.PARAM_LABEL in action_parameter:
                self._parameter_labels[name] = {"default": action_parameter[actions.PARAM_LABEL]}

        # setup all parameters for an action
        for parameter_name, parameter in self.action_properties.get(actions.ACTION_PARAMETERS, {}).items():
            # Parameters cab be marked as hidden, no UI is generated
            if parameter.get(actions.PARAM_HIDDEN, False):
                continue
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
                "Timeout": str(180),
                "Action": self.action_name,
                configuration.CONFIG_ENABLED: {"Ref": configuration.CONFIG_ENABLED},
                configuration.CONFIG_DEBUG: {"Ref": configuration.CONFIG_DEBUG},
                configuration.CONFIG_TASK_NOTIFICATIONS: {"Ref": configuration.CONFIG_TASK_NOTIFICATIONS},
                configuration.CONFIG_TASK_METRICS: {"Ref": configuration.CONFIG_TASK_METRICS},
                configuration.CONFIG_DESCRIPTION: {"Ref": configuration.CONFIG_DESCRIPTION},
                configuration.CONFIG_THIS_ACCOUNT: {"Ref": configuration.CONFIG_THIS_ACCOUNT},
                configuration.CONFIG_PARAMETERS: self._build_resource_parameters(),
                configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME: {"Ref": configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME},
                configuration.CONFIG_STACK_ID: {"Ref": "AWS::StackId"},
            }
        }

        if self.use_intervals:
            task_resource["Properties"].update(
                {
                    configuration.CONFIG_INTERVAL: {"Ref": configuration.CONFIG_INTERVAL},
                    configuration.CONFIG_TIMEZONE: {"Ref": configuration.CONFIG_TIMEZONE}
                }

            )

        # this is the configured timeout in minutes for the task to complete
        if self.has_completion_logic:
            task_resource["Properties"][configuration.CONFIG_TASK_TIMEOUT] = {
                "Ref": configuration.CONFIG_TASK_TIMEOUT
            }
        if self.has_regions_parameter:
            task_resource["Properties"][configuration.CONFIG_REGIONS] = {
                "Ref": configuration.CONFIG_REGIONS
            }
        if self.has_cross_account_parameter:
            task_resource["Properties"][configuration.CONFIG_ACCOUNTS] = {
                "Ref": configuration.CONFIG_ACCOUNTS
            }

        if self._resource_supports_tags:
            task_resource["Properties"][configuration.CONFIG_TAG_FILTER] = {
                "Ref": configuration.CONFIG_TAG_FILTER
            }

        self._setup_event_properties(task_resource)

        self._setup_event_scopes_properties(task_resource)

        # make references to memory parameters
        for memory_param in [
            configuration.CONFIG_TASK_SELECT_SIZE,
            configuration.CONFIG_ECS_SELECT_MEMORY,
            configuration.CONFIG_TASK_EXECUTE_SIZE,
            configuration.CONFIG_ECS_EXECUTE_MEMORY,
            configuration.CONFIG_TASK_COMPLETION_SIZE,
            configuration.CONFIG_ECS_COMPLETION_MEMORY
        ]:
            if memory_param in self._template_parameters:
                task_resource["Properties"][memory_param] = {
                    "Ref": memory_param
                }
            else:
                # or just the memory value if there was only one option
                memory_options = self.action_properties.get(memory_param, [])
                if len(memory_options) == 1:
                    task_resource["Properties"][memory_param] = memory_options[0]

        self._resources["Task"] = task_resource

    def _setup_event_properties(self, task_resource):
        if (sum([len(d) for d in self._action_events])) > 0:
            task_resource["Properties"][configuration.CONFIG_EVENTS] = {}
            resource_events = {}
            for e in EVENTS:
                detail_type = e[0]
                handler_events = e[1]

                action_events_for_source = self._action_events.get(handler_events[EVENT_SOURCE], {})

                if len(action_events_for_source.get(detail_type, [])) > 0:
                    source = handler_events[EVENT_SOURCE]
                    if source not in resource_events:
                        resource_events[source] = {}

                    event_names = self._action_events.get(source, {}).get(detail_type, [])
                    resource_events[source][detail_type] = {
                        event_name: {
                            "Ref": handler_events[EVENT_PARAMETER].format(self._format_event_name(event_name))
                        } for event_name in
                        event_names
                    }

            task_resource["Properties"][configuration.CONFIG_EVENTS] = resource_events

    def _setup_event_scopes_properties(self, task_resource):

        if len(self._action_scopes) > 0:
            task_resource["Properties"][configuration.CONFIG_EVENT_SCOPES] = {}
            resource_scopes = {}

            for source in self._action_scopes:
                if source not in resource_scopes:
                    resource_scopes[source] = {}

                for detail_type in self._action_scopes[source]:
                    if detail_type not in resource_scopes[source]:
                        resource_scopes[source][detail_type] = {}

                    for event_name in self._action_scopes[source][detail_type]:
                        scope_handled_events = [
                            e[1] for e in EVENTS if e[1][EVENT_SOURCE] == source and e[0] == detail_type][0]

                        resource_scopes[source][detail_type][event_name] = {
                            "Ref": scope_handled_events[EVENT_SCOPE_PARAMETER].format(
                                self._format_event_name(event_name))
                        }

            task_resource["Properties"][configuration.CONFIG_EVENT_SCOPES] = resource_scopes

    def _build_resource_parameters(self):
        # parameters that are not hidden in the template
        params = {
            j: {"Ref": j} for j in self.action_properties[actions.ACTION_PARAMETERS] if
            not self.action_properties[actions.ACTION_PARAMETERS][j].get(actions.PARAM_HIDDEN, False)
        }

        # if hidden parameters have a default, use that default
        for p in self.action_properties[actions.ACTION_PARAMETERS]:
            if not self.action_properties[actions.ACTION_PARAMETERS][p].get(actions.PARAM_HIDDEN, False):
                params[p] = {"Ref": p}
            else:
                default = self.action_properties[actions.ACTION_PARAMETERS][p].get(actions.PARAM_DEFAULT, None)
                if default is not None:
                    params[p] = default
        return params

    def _setup_outputs(self):
        if len(self._outputs) != 0:
            self._template["Outputs"] = self._outputs
