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
import re as regex
import time

from botocore.exceptions import ClientError

import handlers.ec2_tag_event_handler
import services.ec2_service
import services.elb_service
import services.elbv2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from actions.action_ec2_events_base import ActionEc2EventBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from handlers import TASK_PARAMETERS
from helpers import safe_json
from helpers.timer import Timer
from outputs import raise_exception, raise_value_error
from tagging import tag_key_value_list
from tagging.tag_filter_expression import TagFilterExpression
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_NEW_INSTANCE_TYPE = "new-instance-type"
TAG_PLACEHOLDER_ORG_INSTANCE_TYPE = "org-instance-type"
TAG_PLACEHOLDER_ORG_INSTANCE_ID = "org-instance-id"

REPLACE_BY_SPECIFIED_TYPE = "ReplaceByType"
REPLACE_BY_STEP = "ReplaceByStep"

PARAM_ASSUMED_TYPE = "AssumedType"
PARAM_REPLACE_MODE = "ReplaceMode"
PARAM_SCALING_RANGE = "ScalingRange"
PARAM_TAGFILTER_SCALE_DOWN = "TagFilterScaleDown"
PARAM_TAGFILTER_SCALE_UP = "TagFilterScaleUp"
PARAM_TRY_NEXT_IN_RANGE = "TryNextInRange"
PARAM_NEW_INSTANCE_TAGS = "NewInstanceTags"
PARAM_INSTANCE_TYPES = "InstanceTypes"
PARAM_TEST_UNAVAILABLE_TYPES = "NotAvailableTypes"
PARAM_REPLACE_IF_SAME_TYPE = "ReplaceIfSameType"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"

PARAM_DESC_ASSUMED_TYPE = \
    "The assumed instance type if the current type of the replaced instance is not in the range of instance types."
PARAM_DESC_REPLACE_MODE = \
    "Set to {} to replace instance with a specified instance type (or an alternative if the type is not available) in " \
    "parameter {}, Set to {} to set to an instance type lower or higher in the list of instances in parameter {} " \
    "using the {} or {} tag filter to determine if the instance is scaled up or down".format(REPLACE_BY_SPECIFIED_TYPE,
                                                                                             PARAM_INSTANCE_TYPES,
                                                                                             REPLACE_BY_STEP,
                                                                                             PARAM_SCALING_RANGE,
                                                                                             PARAM_TAGFILTER_SCALE_UP,
                                                                                             PARAM_TAGFILTER_SCALE_DOWN)
PARAM_DESC_SCALING_RANGE = \
    "Comma separated list of unique instances types in which the instance can be scaled vertically. The list must " \
    "contain at least 2 instance types, and must be ordered by instance types, starting with the smallest " \
    "instance type in the range."
PARAM_DESC_TAGFILTER_SCALE_DOWN = \
    "Tag filter expression that when it matches the instance tags, will make the task replace the instance with the next " \
    "instance type down in the range list of types"
PARAM_DESC_TAGFILTER_SCALE_UP = \
    "Tag filter expression that when it matches the instance tags, will make the task replace the instance with the next " \
    "instance type up in the range list of types"
PARAM_DESC_TRY_NEXT_IN_RANGE = \
    "Try next instance type up or down in range if an instance type is not available. If this parameter is set to False " \
    "the instance will keeps it's size if the next type up or down in the scaling range is not available."
PARAM_DESC_NEW_INSTANCE_TAGS = "List of tags, in name=value format, for newly created instance. Do not use tags that will " \
                               "re-trigger a new execution of this task if tag events are used to start this task."
PARAM_DESC_INSTANCE_TYPES = "New instance type, use a list of types to provide alternatives in case an instance type is not " \
                            "available"
PARAM_DESC_REPLACE_IF_SAME_TYPE = "Replace instance even if new type is the same as current type"
PARAM_DESC_COPIED_INSTANCE_TAGS = \
    "Tag filter expression to copy tags from the instance to the replacement instance. Do not include tags that will re-trigger " \
    "a new  execution of this task, when tagging events are used to start the task. If no value is used for this parameter all " \
    "tags are copied, except for the tags that are part of the scale-up or scale-down tag filter expressions if tag events used " \
    "to trigger the execution the task."

PARAM_LABEL_ASSUMED_TYPE = "Assumed type"
PARAM_LABEL_REPLACE_MODE = "Replacement mode"
PARAM_LABEL_SCALING_RANGE = "Scaling range"
PARAM_LABEL_TAGFILTER_SCALE_DOWN = "Scale down tag filter"
PARAM_LABEL_TAGFILTER_SCALE_UP = "Scale up tag filter"
PARAM_LABEL_TRY_NEXT_IN_RANGE = "Try next in range"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied instance tags"
PARAM_LABEL_INSTANCE_TYPES = "New instance types(s)"
PARAM_LABEL_REPLACE_IF_SAME_TYPE = "Replace if same type"
PARAM_LABEL_NEW_INSTANCE_TAGS = "Instance tags"

INSUFFICIENT_CAPACITY = "InsufficientInstanceCapacity"
INSTANCES_TYPES_NOT_SUPPORTING_EBS_OPTIMIZATION = ["g2.8xlarge", "i2.8xlarge"]

EC2_STATE_PENDING = 0
EC2_STATE_RUNNING = 16
EC2_STATE_STOPPED = 80
EC2_STATE_SHUTTING_DOWN = 32
EC2_STATE_STOPPING = 64
EC2_STATE_TERMINATED = 48

EC2_STOPPING_STATES = {EC2_STATE_SHUTTING_DOWN, EC2_STATE_STOPPING, EC2_STATE_STOPPED}

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options (For replacing instances with encrypted volumes make sure to " \
                               "grant kms:CreateGrant permission for the used kms key to the Ops Automator role)"
GROUP_TITLE_REPLACE_BY_STEP = "Step replacement mode options"
GROUP_TITLE_REPLACE_BY_TYPE = "Specified type replacement mode options"

ERR_CREATE_REPLACEMENT_INSTANCE = "Error creating replacement instance for instance {}, {}"
ERR_INVALID_INSTANCE_TYPE = "{} is not a valid instance type"
ERR_SET_TAGS = "Can not set tags to new instance {}, {}"
ERR_STARTING_NEW_INSTANCE = "New instance {} is not starting, \n{}"
ERR_STOPPING_NEW_INSTANCE = "Error stopping new instance {}, {}"
ERR_TERMINATING_INSTANCE = "Error terminating instance {}, {}"
ERR_TIMEOUT_START_NEW_INSTANCE = "Timeout waiting for instance to stop instance {}, last status is {}"
ERR_TIMEOUT_STOPPING_NEW_INSTANCE = "Timeout waiting for new instance {} to stop"
ERR_ASSUMED_NOT_IN_SCALING_RANGE = "Value of {} parameter  must be in the list of types specified in the {} parameters"
ERR_AT_LEAST_TWO_TYPES = "Parameter {} must contain a list with at least 2 instance types"
ERR_BOTH_SCALING_FILTERS_EMPTY = "Parameter {} and {} can not bot be empty in {} mode"
ERR_NO_TYPE_IN_SPECIFIED_MODE = "At least one instance type must be specified  in parameters {} for replacement mode {}"
ERR_NOT_IN_RANGE_NO_ASSUMED_TYPE = "No assumed type defined and current type {} is not in scaling range {}"
ERR_NOT_LONGER_AVAILABLE = "Instance {} to be replaced is not longer available"
ERR_GETTING_SOURCE_TAGS = "Could not get tags from source volumes of instance {}"
ERR_GETTING_NEW_INST_VOLUMES = "Timeout getting volumes from new instance {} to set new tags, tags are not set"

INF_COPY_TAGS_PER_VOLUME = "Copying tags {} to volume {} of instance {}"
INF_COPY_TAGS_SAME = "Copying tags {} to volume(s) {} of instance {}"
INF_CREATED_NEW_INSTANCE = "Created new instance {} to replace instance {}"
INF_DEREGISTER_INSTANCE_LOADBALANCER = "Deregister instance {} from loadbalancer {}"
INF_DEREGISTER_INSTANCE_TARGET_GROUP = "Deregister instance {} from target group {}"
INF_INSTANCE_NOT_REPLACED = "Instance type of instance {} is already {} instance is not replaced"
INF_INSTANCE_REPLACE_ACTION = "Replacing EC2 instance {} of type {} with new instance of type {} for task {}"
INF_INSTANCE_RUNNING = "New provisioned instance {} is running"
INF_NO_TAG_MATCH_NO_REPLACE = "Scale up tags {} and scale down tags {} do not match instance tags {}, instance {} with type {} " \
                              "will not be replaced"
INF_NO_VOLUME_TAGS_TO_COPY = "No tags to copy for volumes of instance {}"
INF_NOT_IN_SCALING_RANGE = "Type {} is not in scaling range {}"
INF_REGISTER_NEW = "Register instance {} to {}"
INF_RETRY_CREATE = "Retry to provision instance with alternative instance type {}"
INF_STOP_NEW = "Stopping stopping new instance {} as original instance was not running"
INF_TERMINATING_INSTANCE = "Terminating instance {}"
INF_USE_ASSUMED_TYPE = "Assuming specified type {}"
INF_WAIT_DEREGISTER_LOAD_BALANCER = "Waiting for instance {} to deregister from {}"
INF_WAIT_FOR_NEW_TO_START = "Waiting for instance {} to get in running state before registering to loadbalancing"
INF_WAIT_REGISTER_LOAD_BALANCER = "Waiting for instance {} to register to loadbalancer"
INF_MAX_SIZE = "Instance {}, {} at max of range {}, instance will not be replaced"
INF_MIN_SIZE = "Instance {}, {} at min of range {}, instance will not be replaced"

WARN_NO_TYPE_CAPACITY = "No capacity available for type {}"
WARN_BOTH_UP_DOWN = "Both scale up tag filter \"{}\" and scale down tag filter \"{}\" do match instance tags {}, " \
                    "instance {} will not be replaced"
INF_TAGS_NOT_SET_STEP = "Tag {} is part of the scale-up and/or scale-down filter, it will not be set"
INF_TAGS_NOT_SET_TYPE = "Tag {} is not set for instance {} as the event will re-trigger the execution of this task"


class Ec2ReplaceInstanceAction(ActionEc2EventBase):
    properties = {
        ACTION_TITLE: "EC2 Replace Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Replaces EC2 instance with new instance of a different instance type",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "5f44c20d-68fb-4dd0-b9dd-1cf6511bbf00",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "Reservations[*].Instances[]" +
            "|[?contains(['running','stopped'],State.Name)]",

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_NEW_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_NEW_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_NEW_INSTANCE_TAGS
            },
            PARAM_INSTANCE_TYPES: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TYPES,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TYPES
            },
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_DEFAULT: "*",
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
            },
            PARAM_REPLACE_IF_SAME_TYPE: {
                PARAM_DESCRIPTION: PARAM_DESC_REPLACE_IF_SAME_TYPE,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_REPLACE_IF_SAME_TYPE
            },
            PARAM_REPLACE_MODE: {
                PARAM_DESCRIPTION: PARAM_DESC_REPLACE_MODE.format(REPLACE_BY_SPECIFIED_TYPE, REPLACE_BY_STEP),
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: REPLACE_BY_SPECIFIED_TYPE,
                PARAM_ALLOWED_VALUES: [REPLACE_BY_SPECIFIED_TYPE, REPLACE_BY_STEP],
                PARAM_LABEL: PARAM_LABEL_REPLACE_MODE
            },
            PARAM_ASSUMED_TYPE: {
                PARAM_DESCRIPTION: PARAM_DESC_ASSUMED_TYPE,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_ALLOWED_VALUES: services.ec2_service.Ec2Service.valid_instance_types(),
                PARAM_LABEL: PARAM_LABEL_ASSUMED_TYPE
            },
            PARAM_TRY_NEXT_IN_RANGE: {
                PARAM_DESCRIPTION: PARAM_DESC_TRY_NEXT_IN_RANGE,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_TRY_NEXT_IN_RANGE
            },
            PARAM_SCALING_RANGE: {
                PARAM_DESCRIPTION: PARAM_DESC_SCALING_RANGE,
                PARAM_TYPE: list,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SCALING_RANGE
            },
            PARAM_TAGFILTER_SCALE_UP: {
                PARAM_DESCRIPTION: PARAM_DESC_TAGFILTER_SCALE_UP,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_TAGFILTER_SCALE_UP
            },
            PARAM_TAGFILTER_SCALE_DOWN: {
                PARAM_DESCRIPTION: PARAM_DESC_TAGFILTER_SCALE_DOWN,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_TAGFILTER_SCALE_DOWN
            },
            PARAM_TEST_UNAVAILABLE_TYPES: {
                # This is a hidden test parameter and is used to simulate situations where instance types are not available
                PARAM_DESCRIPTION: "",
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: "",
                PARAM_HIDDEN: True
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_INSTANCE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_NEW_INSTANCE_TAGS,
                    PARAM_REPLACE_MODE
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_REPLACE_BY_TYPE,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_INSTANCE_TYPES,
                    PARAM_REPLACE_IF_SAME_TYPE
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_REPLACE_BY_STEP,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_SCALING_RANGE,
                    PARAM_ASSUMED_TYPE,
                    PARAM_TRY_NEXT_IN_RANGE,
                    PARAM_TAGFILTER_SCALE_UP,
                    PARAM_TAGFILTER_SCALE_DOWN

                ],
            },
        ],

        ACTION_PERMISSIONS: [
            "ec2:RunInstances",
            "ec2:StopInstances",
            "ec2:TerminateInstances",
            "ec2:DescribeTags",
            "ec2:DescribeVolumes",
            "ec2:DescribeInstanceAttribute",
            "ec2:CreateTags",
            "ec2:DeleteTags",
            "ec2:DescribeInstanceCreditSpecifications",
            "ec2:ModifyInstanceCreditSpecification",
            "elasticloadbalancing:DescribeLoadBalancers",
            "elasticloadbalancing:DescribeTargetGroupAttributes",
            "elasticloadbalancing:DescribeTags",
            "elasticloadbalancing:RegisterTargets",
            "elasticloadbalancing:DescribeTargetHealth",
            "elasticloadbalancing:DescribeTargetGroups",
            "elasticloadbalancing:DeregisterTargets",
            "elasticloadbalancing:DeregisterInstancesFromLoadBalancer",
            "elasticloadbalancing:RegisterInstancesWithLoadBalancer",
            "iam:GetRole",
            "iam:PassRole"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.instance = self._resources_

        self.new_instance = None
        self.new_instance_id = None

        self.instance_id = self.instance["InstanceId"]
        self._ec2_client = None
        self._ec2_service = None

        # instance type, list if alternatives must be retried if the type is not available
        self.new_instance_types = [s.strip() for s in self.get(PARAM_INSTANCE_TYPES, [])]

        self.replace_mode = self.get(PARAM_REPLACE_MODE)

        self._elb_service = None
        self._elb_client = None

        self._elbv2_service = None
        self._elbv2_client = None

        self._elb_data = None

        self.instance_type_index = -1
        self.elb_registrations = None

        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))

        self.scaling_range = [t.strip() for t in self.get(PARAM_SCALING_RANGE, [])]
        self.next_type_in_range = self.get(PARAM_TRY_NEXT_IN_RANGE, True) if self.replace_mode == REPLACE_BY_STEP else True

        self.replace_if_same_type = self.get(PARAM_DESC_REPLACE_IF_SAME_TYPE, False)

        self.scale_up_str = self.get(PARAM_TAGFILTER_SCALE_UP)
        self.scale_up_tagfilter = TagFilterExpression(self.scale_up_str) if self.scale_up_str is not None else None

        self.scale_down_str = self.get(PARAM_TAGFILTER_SCALE_DOWN)
        self.scale_down_tagfilter = TagFilterExpression(self.scale_down_str) if self.scale_down_str is not None else None

        self.assumed_instance_type = self.get(PARAM_ASSUMED_TYPE)
        self.scaling_range_index = None
        self.scale_up = None
        self.scale_down = None

        self.original_type = None

        self.new_instance_type = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instance": self.instance_id,
            "task": self._task_
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        instance = arguments[ACTION_PARAM_RESOURCES]
        name = instance.get("Tags", {}).get("Name", "").strip()
        if name != "":
            name = name + "-"
        instance_id = instance["InstanceId"]
        account = instance["AwsAccount"]
        region = instance["Region"]
        return "{}-{}-{}{}-{}".format(account, region, name, instance_id, log_stream_date())

    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        mode = parameters.get(PARAM_REPLACE_MODE)

        def check_types(types):
            valid_types = services.ec2_service.Ec2Service.valid_instance_types()
            if valid_types not in [None, []]:
                for inst_type in [e.strip() for e in types]:
                    if inst_type not in valid_types:
                        raise_value_error(ERR_INVALID_INSTANCE_TYPE.format(inst_type))

        if mode == REPLACE_BY_SPECIFIED_TYPE:

            instance_types = parameters.get(PARAM_INSTANCE_TYPES, [])

            if len(instance_types) == 0:
                raise_value_error(ERR_NO_TYPE_IN_SPECIFIED_MODE, PARAM_REPLACE_MODE.format(mode))
            check_types(instance_types)

        else:
            scaling_range = parameters.get(PARAM_SCALING_RANGE, [])
            if len(scaling_range) < 2:
                raise_value_error(ERR_AT_LEAST_TWO_TYPES.format(PARAM_SCALING_RANGE))

            check_types(scaling_range)

            assumed_type = parameters.get(PARAM_ASSUMED_TYPE)
            if assumed_type is not None:
                if assumed_type not in scaling_range:
                    raise_value_error(ERR_ASSUMED_NOT_IN_SCALING_RANGE, PARAM_ASSUMED_TYPE, PARAM_SCALING_RANGE)

            scale_up_filter = parameters.get(PARAM_TAGFILTER_SCALE_UP)
            scale_down_filter = parameters.get(PARAM_TAGFILTER_SCALE_DOWN)

            if scale_up_filter is None and scale_down_filter is None:
                raise_value_error(ERR_BOTH_SCALING_FILTERS_EMPTY, PARAM_TAGFILTER_SCALE_UP, PARAM_TAGFILTER_SCALE_DOWN, mode)

        ActionEc2EventBase.check_tag_filters_and_tags(parameters, task_settings, [PARAM_NEW_INSTANCE_TAGS], logger)

        return parameters

    # noinspection PyUnusedLocal,PyUnusedLocal,PyUnusedLocal,PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        parameters = task.get(TASK_PARAMETERS, {})
        if parameters.get(PARAM_REPLACE_MODE) == REPLACE_BY_SPECIFIED_TYPE:
            return resource

        tags = resource.get("Tags", {})

        scale_up_str = parameters.get(PARAM_TAGFILTER_SCALE_UP)
        scale_up_filter = TagFilterExpression(scale_up_str) if scale_up_str is not None else None

        if scale_up_filter is not None and scale_up_filter.is_match(tags):
            return resource

        scale_down_str = parameters.get(PARAM_TAGFILTER_SCALE_DOWN)
        scale_down_filter = TagFilterExpression(scale_down_str) if scale_down_str is not None else None
        if scale_down_filter is not None and scale_down_filter.is_match(tags):
            return resource

        logger.debug("Instance {} is not selected as tags {} do not match scale-up filter \"{}\" or scale-down filter \"{}\"",
                     resource["InstanceId"], tags, scale_up_str, scale_down_str)

        return None

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "run_instances",
                "terminate_instances",
                "create_tags",
                "delete_tags",
                "describe_instances",
                "modify_instance_credit_specification"
            ]

            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    @property
    def ec2_service(self):
        if self._ec2_service is None:
            self._ec2_service = services.create_service("ec2", session=self._session_,
                                                        service_retry_strategy=get_default_retry_strategy("ec2",
                                                                                                          context=self._context_))
        return self._ec2_service

    @property
    def elb_client(self):
        if self._elb_client is None:
            methods = ["register_instances_with_load_balancer",
                       "deregister_instances_from_load_balancer"]

            self._elb_client = get_client_with_retries("elb",
                                                       methods=methods,
                                                       region=self._region_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._elb_client

    @property
    def elb_service(self):
        if self._elb_service is None:
            self._elb_service = services.create_service("elb", session=self._session_,
                                                        service_retry_strategy=get_default_retry_strategy("ec2",
                                                                                                          context=self._context_))
        return self._elb_service

    @property
    def elbv2_client(self):
        if self._elbv2_client is None:
            methods = ["deregister_targets",
                       "register_targets"]
            self._elbv2_client = get_client_with_retries("elbv2",
                                                         methods=methods,
                                                         region=self._region_,
                                                         session=self._session_,
                                                         logger=self._logger_)
        return self._elbv2_client

    @property
    def elbv2_service(self):
        if self._elbv2_service is None:
            self._elbv2_service = services.create_service("elbv2", session=self._session_,
                                                          service_retry_strategy=get_default_retry_strategy("elbv2",
                                                                                                            context=self._context_))
        return self._elbv2_service

    @property
    def elb_data(self):
        if self._elb_data is None:
            self._elb_data = self._get_elb_data({})
            self._elb_data = self._get_elbv2_data(self._elb_data)
        return self._elb_data

    @classmethod
    def is_in_stopping_or_stopped_state(cls, state):
        return (state & 0xFF) in EC2_STOPPING_STATES

    @classmethod
    def insufficient_capacity(cls, ex):
        return type(ex).__name__ == "ClientError" and ex.response.get("Error", {}).get("Code", None) == INSUFFICIENT_CAPACITY

    @classmethod
    def load_balancing_str(cls, registrations):
        names = []
        for r in registrations:
            if r["Version"] == 1:
                names.append("Loadbalancer {}".format(r["LoadBalancerName"]))
            if r["Version"] == 2:
                names.append("TargetGroup {}".format(r["TargetGroupName"]))
        s = ", ".join(names)
        return s

    def _get_instance(self, instance_id):

        return self.ec2_service.get(services.ec2_service.INSTANCES,
                                    InstanceIds=[instance_id],
                                    region=self._region_,
                                    select="Reservations[*].Instances[]")

    def _set_new_instance_type(self):

        if self.replace_mode == REPLACE_BY_SPECIFIED_TYPE:
            self.instance_type_index += 1
            if self.instance_type_index >= len(self.new_instance_types):
                self.new_instance_types = [self.original_type]
                self.instance_type_index = 0
            self.new_instance_type = self.new_instance_types[self.instance_type_index]
            return

        current_type = self.instance["InstanceType"]
        instance_tags = self.instance.get("Tags", {})

        if self.scaling_range_index is None:

            if current_type not in self.scaling_range:

                self._logger_.info(INF_NOT_IN_SCALING_RANGE, current_type, ", ".join(self.scaling_range))
                if self.assumed_instance_type is None:
                    self._logger_.error(ERR_NOT_IN_RANGE_NO_ASSUMED_TYPE, current_type, ", ".join(self.scaling_range))
                    self.new_instance_type = current_type
                    return

                current_type = self.assumed_instance_type
                self._logger_.info(INF_USE_ASSUMED_TYPE, self.assumed_instance_type)

            instance_tags = self.instance.get("Tags", {})
            self.scale_up = self.scale_up_tagfilter is not None and self.scale_up_tagfilter.is_match(instance_tags)
            self.scale_down = self.scale_down_tagfilter is not None and self.scale_down_tagfilter.is_match(instance_tags)

            if self.scale_up and self.scale_down:
                self._logger_.warning(WARN_BOTH_UP_DOWN, self.scale_up_str, self.scale_down_str, instance_tags, self.instance_id)
                self.new_instance_type = current_type
                return

            self.scaling_range_index = self.scaling_range.index(current_type)
        elif not self.next_type_in_range:
            self.new_instance_type = current_type
            return

        if self.scale_up:
            self.scaling_range_index += 1
            if self.scaling_range_index >= len(self.scaling_range):
                self._logger_.info(INF_MAX_SIZE, self.instance_id, current_type, ", ".join(self.scaling_range))
                self.new_instance_type = current_type
            else:
                self.new_instance_type = self.scaling_range[self.scaling_range_index]
            return

        if self.scale_down:
            self.scaling_range_index -= 1
            if self.scaling_range_index < 0:
                self._logger_.info(INF_MIN_SIZE, self.instance_id, current_type, ", ".join(self.scaling_range))
                self.new_instance_type = current_type
            else:
                self.new_instance_type = self.scaling_range[self.scaling_range_index]
            return

        self._logger_.info(INF_NO_TAG_MATCH_NO_REPLACE, self.scale_up_str, self.scale_down_str, instance_tags, self.instance_id,
                           current_type)
        self.new_instance_type = current_type

    def _create_replacement_instance(self):

        def copy_volume_tags():
            source_volume_ids = [m["Ebs"]["VolumeId"] for m in self.instance["BlockDeviceMappings"]]

            with Timer(timeout_seconds=60) as t:
                while True:
                    source_volumes = list(self.ec2_service.describe(services.ec2_service.VOLUMES,
                                                                    VolumeIds=source_volume_ids,
                                                                    region=self._region_,
                                                                    tags=True))
                    if all(len(v.get("Attachments", [])) > 0 for v in source_volumes):
                        source_device_tags = {v["Attachments"][0]["Device"]: v.get("Tags", {}) for v in source_volumes}
                        break

                    if t.timeout:
                        self._logger_.error(ERR_GETTING_SOURCE_TAGS, self.instance_id)
                        source_device_tags = {}
                        break

                    time.sleep(10)

            for t in source_device_tags.values():
                if len(t) > 0:
                    break
            else:
                self._logger_.info(INF_NO_VOLUME_TAGS_TO_COPY, self.new_instance_id)
                return

            all_volume_tags = source_device_tags.values()
            for t in all_volume_tags:
                if t != all_volume_tags[0]:
                    all_same_tags = False
                    break
            else:
                all_same_tags = True
            with Timer(60) as t:
                while True:
                    volumes = list(self.ec2_service.describe(services.ec2_service.VOLUMES,
                                                             region=self._region_,
                                                             Filters=[
                                                                 {
                                                                     "Name": "attachment.instance-id",
                                                                     "Values": [self.new_instance_id]
                                                                 }]))
                    if len(volumes) > 0:
                        break

                    if t.timeout:
                        self._logger_.error(ERR_GETTING_NEW_INST_VOLUMES, self.new_instance_id)
                        return

                    time.sleep(10)

            if all_same_tags:
                volume_ids = [v["VolumeId"] for v in volumes]
                tags = source_device_tags.values()[0]
                self._logger_.info(INF_COPY_TAGS_SAME, tags, ", ".join(volume_ids),
                                   self.new_instance_id)
                self.ec2_client.create_tags_with_retries(Resources=volume_ids, Tags=tag_key_value_list(tags))
            else:
                devices = {v["Attachments"][0]["Device"]: v["VolumeId"] for v in volumes}
                for v in devices:
                    tags = source_device_tags.get(v, {})
                    volume_id = [devices[v]]
                    if len(tags) > 0:
                        self._logger_.info(INF_COPY_TAGS_PER_VOLUME, tags, volume_id[0], self.new_instance_id)
                        self.ec2_client.create_tags_with_retries(Resources=volume_id, Tags=tag_key_value_list(tags))

        def get_user_data():
            return self.ec2_service.get(services.ec2_service.INSTANCE_ATTRIBUTE,
                                        InstanceId=self.instance_id,
                                        region=self._region_,
                                        Attribute="userData").get("UserData", None)

        def get_termination_protection():
            return self.ec2_service.get(services.ec2_service.INSTANCE_ATTRIBUTE,
                                        InstanceId=self.instance_id,
                                        region=self._region_,
                                        Attribute="disableApiTermination").get("DisableApiTermination")

        def get_kernel():
            return self.ec2_service.get(services.ec2_service.INSTANCE_ATTRIBUTE,
                                        InstanceId=self.instance_id,
                                        region=self._region_,
                                        Attribute="kernel").get("Kernel")

        def get_ramdisk():
            return self.ec2_service.get(services.ec2_service.INSTANCE_ATTRIBUTE,
                                        InstanceId=self.instance_id,
                                        region=self._region_,
                                        Attribute="ramdisk").get("RamdiskId")

        def get_shutdown_behavior():
            return self.ec2_service.get(services.ec2_service.INSTANCE_ATTRIBUTE,
                                        InstanceId=self.instance_id,
                                        region=self._region_,
                                        Attribute="instanceInitiatedShutdownBehavior").get("InstanceInitiatedShutdownBehavior")

        def get_ebs_optimized():
            if self.new_instance_type.startswith(
                    "t2.") or self.new_instance_type in INSTANCES_TYPES_NOT_SUPPORTING_EBS_OPTIMIZATION:
                return False
            return self.instance["EbsOptimized"]

        def get_network_interfaces():
            result = [
                {
                    "AssociatePublicIpAddress": network_interface.get("Association", {}).get("PublicIp") is not None,
                    "DeleteOnTermination": network_interface["Attachment"]["DeleteOnTermination"],
                    "Description": network_interface.get("Description", ""),
                    "DeviceIndex": int(network_interface["Attachment"]["DeviceIndex"]),
                    "Groups":
                        [g["GroupId"] for g in network_interface["Groups"]
                         ],
                    "Ipv6AddressCount": len(network_interface["Ipv6Addresses"]),

                    "SecondaryPrivateIpAddressCount": max(len(network_interface["PrivateIpAddresses"]) - 1, 0),
                    "SubnetId": network_interface["SubnetId"]
                } for network_interface in self.instance["NetworkInterfaces"]
            ]
            for i in result:
                if i["SecondaryPrivateIpAddressCount"] == 0:
                    del i["SecondaryPrivateIpAddressCount"]
            return result

        args = {
            "ImageId": self.instance["ImageId"],
            "InstanceType": self.new_instance_type,
            "MaxCount": 1,
            "MinCount": 1,
            "Monitoring": {"Enabled": self.instance["Monitoring"]["State"] == "enabled"},
            "DisableApiTermination": get_termination_protection(),
            "InstanceInitiatedShutdownBehavior": get_shutdown_behavior(),
            "EbsOptimized": get_ebs_optimized(),
            "NetworkInterfaces": get_network_interfaces()
        }
        if self.instance.get("KeyName") is not None:
            args["KeyName"] = self.instance["KeyName"]

        if self.instance.get("Placement") is not None:
            args["Placement"] = self.instance["Placement"]

        if self.instance.get("HibernationOptions") is not None:
            args["HibernationOptions"] = self.instance["HibernationOptions"]

        if self.instance.get("LicenseSpecifications") is not None:
            args["LicenseConfigurationArn"] = [l["LicenseConfigurationArn"] for l in self.instance.get("Licenses", [])],

        kernel_id = get_kernel()
        if kernel_id is not None:
            args["KernelId"] = kernel_id

        ramdisk_id = get_ramdisk()
        if kernel_id is not None:
            args["RamdiskId"] = ramdisk_id

        userdata = get_user_data()
        if userdata not in ["", None, {}]:
            args["UserData"] = base64.b64decode(userdata)

        instance_profile = self.instance.get("IamInstanceProfile")
        if instance_profile not in [None, {}]:
            args["IamInstanceProfile"] = {"Arn": instance_profile["Arn"]}

        capacity_reservation_specification = self.instance.get("CapacityReservationSpecification")
        if capacity_reservation_specification not in [None, {}]:
            args["CapacityReservationSpecification"] = capacity_reservation_specification

        self._test_simulate_insufficient_instance_capacity()
        self.new_instance = self.ec2_client.run_instances_with_retries(**args)["Instances"][0]
        self.new_instance_id = self.new_instance["InstanceId"]

        cpu_credits = self.ec2_service.get(services.ec2_service.INSTANCE_CREDIT_SPECIFICATIONS,
                                           region=self._region_,
                                           InstanceIds=[self.instance_id]).get("CpuCredits")

        if cpu_credits == "unlimited" and regex.match(r"^t\d.", self.new_instance_type) is not None:
            self.ec2_client.modify_instance_credit_specification_with_retries(InstanceCreditSpecifications=[
                {
                    'InstanceId': self.new_instance_id,
                    'CpuCredits': "unlimited"
                },
            ])

        copy_volume_tags()

    def _get_elb_data(self, data, load_balancers=None):

        result = copy.deepcopy(data)

        args = {
            "service_resource": services.elb_service.LOAD_BALANCERS,
            "region": self._region_,
        }
        if load_balancers is not None:
            args["LoadBalancerNames"] = load_balancers

        for lb in self.elb_service.describe(**args):
            for inst in lb.get("Instances", []):
                instance_id = inst["InstanceId"]
                if instance_id not in result:
                    result[instance_id] = []
                result[instance_id].append({
                    "Version": 1,
                    "LoadBalancerName": lb["LoadBalancerName"]
                })

        return result

    def _get_elbv2_data(self, data, target_group_arns=None):

        result = copy.deepcopy(data)

        args = {
            "service_resource": services.elbv2_service.TARGET_GROUPS,
            "region": self._region_,

        }

        if target_group_arns is not None:
            args["TargetGroupArns"] = target_group_arns

        target_groups = list(self.elbv2_service.describe(**args))
        for target_group in target_groups:
            target_group_healths = list(self.elbv2_service.describe(services.elbv2_service.TARGET_HEALTH,
                                                                    region=self._region_,
                                                                    TargetGroupArn=target_group["TargetGroupArn"]))
            for target_group_health in target_group_healths:
                target = target_group_health["Target"]
                if not target["Id"].startswith("i-"):
                    continue
                instance_id = target["Id"]
                if instance_id not in result:
                    result[instance_id] = []
                result[instance_id].append({
                    "Version": 2,
                    "TargetGroupName": target_group.get("TargetGroupName"),
                    "TargetGroupArn": target_group.get("TargetGroupArn"),
                    "Port": target.get("Port"),
                    "AvailabilityZone": target.get("AvailabilityZone")

                })

        return result

    def _test_simulate_insufficient_instance_capacity(self):

        if self.new_instance_type in self.get(PARAM_TEST_UNAVAILABLE_TYPES, []):
            raise ClientError(
                {
                    "Error": {
                        "Code": INSUFFICIENT_CAPACITY,
                        "Message": "Simulated {} Exception".format(INSUFFICIENT_CAPACITY)
                    }
                }, operation_name="start_instances")

    def _register_new_instance_to_elb_v1(self):

        def register_instance_v1(reg):

            self.elb_client.register_instances_with_load_balancer_with_retries(
                LoadBalancerName=reg["LoadBalancerName"],
                Instances=[
                    {
                        "InstanceId": self.new_instance_id
                    },
                ]
            )

        def register_instance_v2(reg):

            target = {
                "Id": self.new_instance_id
            }
            if reg.get("Port") is not None:
                target["Port"] = reg["Port"]

            if reg.get("AvailabilityZone") is not None:
                target["AvailabilityZone"] = reg["AvailabilityZone"]

            self.elbv2_client.register_targets_with_retries(TargetGroupArn=reg["TargetGroupArn"], Targets=[target])

        if self.elb_registrations is None:
            return

        for registration in self.elb_registrations:
            if registration["Version"] == 1:
                register_instance_v1(registration)
            elif registration["Version"] == 2:
                register_instance_v2(registration)

    def _deregister_source_instance(self):

        def deregister_instance_v1(reg):

            self.elb_client.deregister_instances_from_load_balancer_with_retries(
                LoadBalancerName=reg["LoadBalancerName"],
                Instances=[{
                    "InstanceId": self.instance_id
                }])

        def deregister_instance_v2(reg):

            target = {
                "Id": self.instance_id
            }

            port = reg.get("Port")
            if port is not None:
                target["Port"] = port

            availability_zone = reg.get("AvailabilityZone")
            if availability_zone is not None:
                target["AvailabilityZone"] = availability_zone

            self.elbv2_client.deregister_targets_with_retries(
                TargetGroupArn=reg["TargetGroupArn"],
                Targets=[target])

        for registration in self.elb_registrations:
            if registration["Version"] == 1:
                self._logger_.info(INF_DEREGISTER_INSTANCE_LOADBALANCER, self.instance_id, registration["LoadBalancerName"])
                deregister_instance_v1(registration)
            elif registration["Version"] == 2:
                self._logger_.info(INF_DEREGISTER_INSTANCE_TARGET_GROUP, self.instance_id, registration["TargetGroupName"])
                deregister_instance_v2(registration)

    def _wait_for_new_instance_running(self, timeout=600):
        with Timer(timeout) as timer:

            while True:
                state = self._get_instance(self.new_instance_id)["State"]["Name"]
                if state == "running":
                    return

                elif timer.timeout:
                    raise Exception(ERR_TIMEOUT_START_NEW_INSTANCE.format(self.new_instance_id, state))

                time.sleep(15)

    def is_completed(self, start_data):

        def task_is_triggered_by_tag_event():
            task_change_events = self._events_.get(handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE, {}).get(
                handlers.TAG_CHANGE_EVENT, [])

            return handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT in task_change_events

        def set_tags_on_new_instance(new_instance_type, original_type):

            # tags copied from replaced instance
            copied_tags_filter_str = self.get(PARAM_COPIED_INSTANCE_TAGS, "*")
            copied_tags_filter = TagFilterSet(copied_tags_filter_str)
            tags = copied_tags_filter.pairs_matching_any_filter(start_data.get("source-tags", {}))

            # tags set by action
            tags.update(self.build_tags_from_template(parameter_name=PARAM_NEW_INSTANCE_TAGS,
                                                      tag_variables={
                                                          TAG_PLACEHOLDER_NEW_INSTANCE_TYPE: new_instance_type,
                                                          TAG_PLACEHOLDER_ORG_INSTANCE_TYPE: original_type,
                                                          TAG_PLACEHOLDER_ORG_INSTANCE_ID: self.instance_id
                                                      }))

            try:
                # if task is triggered by tagging event
                if task_is_triggered_by_tag_event():
                    # up or down tags filters should not match new tags as it would re-trigger execution of the task
                    if self.replace_mode == REPLACE_BY_STEP:
                        for t in tags.keys():
                            # remove tags that match up or down tag filters
                            if (self.scale_up_tagfilter and t in self.scale_up_tagfilter.get_filter_keys()) or \
                                    (self.scale_down_tagfilter and t in self.scale_down_tagfilter.get_filter_keys()):
                                self._logger_.info(INF_TAGS_NOT_SET_STEP.format({t: tags[t]}, self.instance_id))
                                del tags[t]

                    else:
                        # new tags should not match the tag filter or task name should not be in task list
                        if self._tagfilter_ is not None:
                            # check again tag filter if any
                            check_filter = TagFilterExpression(self._tagfilter_)
                            for t in tags.keys():
                                if t in check_filter.get_filter_keys():
                                    self._logger_.info(INF_TAGS_NOT_SET_TYPE.format({t: tags[t]}, self.instance_id))
                                    del tags[t]
                        else:
                            # check if name of the task is not in the new task list
                            tag_list_tag_name = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME)
                            for t in tags.keys():
                                if t == tag_list_tag_name and self._task_ in tagging.split_task_list(tags[t]):
                                    self._logger_.info(INF_TAGS_NOT_SET_TYPE.format({t: tags[t]}, self.instance_id))
                                    del tags[t]

                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     resource_ids=[self.new_instance_id],
                                     logger=self._logger_,
                                     tags=tags)

            except Exception as tag_ex:
                raise_exception(ERR_SET_TAGS, self.new_instance_id, tag_ex)

        def get_scoped_elb_data(registrations):

            result = {}
            if registrations not in [{}, None]:
                result = self._get_elb_data({}, [e["LoadBalancerName"] for e in registrations if e["Version"] == 1])
                result = self._get_elbv2_data(result, [e["TargetGroupArn"] for e in registrations if e["Version"] == 2])
            return result

        elb_registrations = start_data.get("elb-registrations")
        current_elb_data = get_scoped_elb_data(elb_registrations)

        if start_data.get("not-replaced", False):
            return self.result

        # test if there any registrations left for replaced instance
        instance_registrations = current_elb_data.get(self.instance_id)
        if instance_registrations is not None:
            self._logger_.info(INF_WAIT_DEREGISTER_LOAD_BALANCER, self.instance_id, self.load_balancing_str(instance_registrations))
            return None

        # get current state of instance
        self.new_instance_id = start_data["new-instance"]
        instance = self._get_instance(self.new_instance_id)
        self._logger_.debug("Instance data is {}", safe_json(instance, indent=3))

        state_code = instance["State"]["Code"] & 0xFF

        # new instance is running, done...
        if state_code == EC2_STATE_RUNNING:
            # instance is running
            self._logger_.info(INF_INSTANCE_RUNNING, self.new_instance_id)

            if elb_registrations is not None and len(elb_registrations) != len(current_elb_data.get(self.new_instance_id, [])):
                self._logger_.info(INF_WAIT_REGISTER_LOAD_BALANCER, self.new_instance_id)
                return None

            set_tags_on_new_instance(instance["InstanceType"], start_data.get("org-instance-type", ""))
            self._logger_.info(INF_TERMINATING_INSTANCE, self.instance_id)
            try:
                self.ec2_client.terminate_instances_with_retries(InstanceIds=[self.instance_id])
            except Exception as ex:
                self._logger_.error(ERR_TERMINATING_INSTANCE, self.instance_id, ex)

            for s in ["source-tags", "not-replaced"]:
                if s in self.result:
                    del self.result[s]
            return self.result

        # in pending state, wait for next completion check
        if state_code == EC2_STATE_PENDING:
            return None

        raise_exception(ERR_STARTING_NEW_INSTANCE, self.new_instance_id, safe_json(instance, indent=3))

    def execute(self):

        def should_replace_instance():
            if self.original_type == self.new_instance_type and not self.replace_if_same_type:
                self._logger_.info(INF_INSTANCE_NOT_REPLACED, self.instance_id, self.original_type)
                self.result["not-replaced"] = True
                self.result[METRICS_DATA] = build_action_metrics(
                    action=self,
                    ReplacedInstances=0
                )
                return False
            return True

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._set_new_instance_type()

        # get instance in it's current state
        instance = self._get_instance(self.instance_id)
        if instance is None:
            raise_exception(ERR_NOT_LONGER_AVAILABLE, self.instance_id)

        self.original_type = instance["InstanceType"]
        self.result["org-instance-type"] = self.original_type

        if not should_replace_instance():
            return self.result

        instance_running = not self.is_in_stopping_or_stopped_state(instance["State"]["Code"])
        self.result["instance-running"] = instance_running

        self.result["source-tags"] = self.copied_instance_tagfilter.pairs_matching_any_filter(instance.get("Tags", {}))

        self.elb_registrations = self.elb_data.get(self.instance_id)
        if self.elb_registrations is not None:
            self.result["elb-registrations"] = self.elb_registrations

        while True:

            try:
                self._logger_.info(INF_INSTANCE_REPLACE_ACTION, self.instance_id, self.original_type, self.new_instance_type,
                                   self._task_)

                self._create_replacement_instance()
                self.result["new-instance"] = self.new_instance_id
                self.result["instance-type"] = self.new_instance_type

                self._logger_.info(INF_CREATED_NEW_INSTANCE, self.new_instance_id, self.instance_id)

                if self.elb_registrations is not None:

                    try:
                        self._logger_.info(INF_WAIT_FOR_NEW_TO_START, self.new_instance_id)
                        self._wait_for_new_instance_running()
                        registrations_str = self.load_balancing_str(self.elb_registrations)
                        self._logger_.info(INF_REGISTER_NEW, self.new_instance_id, registrations_str)
                        self._register_new_instance_to_elb_v1()
                        self._deregister_source_instance()

                        self.result[METRICS_DATA] = build_action_metrics(
                            action=self,
                            ReplacedInstances=1,
                            OrgInstanceType=self.original_type,
                            NewInstanceType=self.new_instance_type
                        )

                    except Exception as ex:
                        self.ec2_client.terminate_instances_with_retries(InstanceIds=[self.new_instance_id])
                        raise_exception(ex)

                if not instance_running:
                    try:
                        self._logger_.info(INF_STOP_NEW, self.new_instance_id)
                        self.ec2_client.stop_instances_with_retries(InstanceIds=[self.new_instance_id])
                    except Exception as ex:
                        self._logger_.error(ERR_STOPPING_NEW_INSTANCE, self.new_instance_id, ex)

                break

            except ClientError as ex:

                # no capacity for this type
                if self.insufficient_capacity(ex):

                    # try to set alternative type
                    self._logger_.warning(WARN_NO_TYPE_CAPACITY, self.new_instance_type)

                    self._set_new_instance_type()
                    if not should_replace_instance():
                        return self.result

                    self._logger_.info(INF_RETRY_CREATE, self.new_instance_type)

                else:
                    raise_exception(ERR_CREATE_REPLACEMENT_INSTANCE, self.instance_id, str(ex))

        return self.result
