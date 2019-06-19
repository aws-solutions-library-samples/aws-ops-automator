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


import time

from botocore.exceptions import ClientError

import handlers.ec2_tag_event_handler
import services.ec2_service
import tagging
from actions import *
from actions.action_ec2_events_base import ActionEc2EventBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from handlers import TASK_PARAMETERS
from helpers import safe_json
from helpers.timer import Timer
from outputs import raise_exception, raise_value_error
from tagging.tag_filter_expression import TagFilterExpression

TAG_PLACEHOLDER_NEW_INSTANCE_TYPE = "new-instance-type"
TAG_PLACEHOLDER_ORG_INSTANCE_TYPE = "org-instance-type"

INSUFFICIENT_CAPACITY = "InsufficientInstanceCapacity"

EC2_STATE_PENDING = 0
EC2_STATE_RUNNING = 16
EC2_STATE_STOPPED = 80
EC2_STATE_SHUTTING_DOWN = 32
EC2_STATE_STOPPING = 64

EC2_STOPPING_STATES = {EC2_STATE_SHUTTING_DOWN, EC2_STATE_STOPPING, EC2_STATE_STOPPED}
EC2_STARTING_STATES = {EC2_STATE_PENDING, EC2_STATE_RUNNING}

RESIZE_BY_SPECIFIED_TYPE = "ReplaceByType"
RESIZE_BY_STEP = "ReplaceByStep"

PARAM_RESIZED_INSTANCE_TAGS = "ResizedInstanceTags"
PARAM_INSTANCE_TYPES = "InstanceTypes"
PARAM_RESIZE_MODE = "ReplaceMode"
PARAM_ASSUMED_TYPE = "AssumedType"
PARAM_TRY_NEXT_IN_RANGE = "TryNextInRange"
PARAM_SCALING_RANGE = "ScalingRange"
PARAM_TAGFILTER_SCALE_DOWN = "TagFilterScaleDown"
PARAM_TAGFILTER_SCALE_UP = "TagFilterScaleUp"
PARAM_TEST_UNAVAILABLE_TYPES = "NotAvailableTypes"

PARAM_DESC_RESIZED_INSTANCE_TAGS = "Tags to set on resized EC2 instance. Don't use tag updates combined with " \
                                   "tag filters that could re-trigger a new execution of this task."
PARAM_DESC_INSTANCE_TYPES = "New instance type, use a list of types to provide alternatives in case an instance type is " \
                            "not available"

PARAM_DESC_RESIZE_MODE = \
    "Set to {} to resize instance with a specified instance type (or an alternative if the type is not available) in " \
    "parameter {}, Set to {} to set to an instance type lower or higher in the list of instances in parameter {} " \
    "using the {} or {} tag filter to determine if the instance is scale up or down".format(RESIZE_BY_SPECIFIED_TYPE,
                                                                                            PARAM_INSTANCE_TYPES,
                                                                                            RESIZE_BY_STEP,
                                                                                            PARAM_SCALING_RANGE,
                                                                                            PARAM_TAGFILTER_SCALE_UP,
                                                                                            PARAM_TAGFILTER_SCALE_DOWN)
PARAM_DESC_ASSUMED_TYPE = \
    "The assumed instance type if the current instance type is not in the range of instance types."
PARAM_DESC_TRY_NEXT_IN_RANGE = \
    "Try next instance type up or down in range if an instance type is not available. If this parameter is set to False " \
    "the instance will keep its size if the next type up or down in the range is not available."
PARAM_DESC_SCALING_RANGE = \
    "Comma separated list of unique instances types in which the instance can be scaled vertically. The list must " \
    "contain at least 2 instance types, and must be ordered by instance types, starting with the smallest " \
    "instance type in the range."
PARAM_DESC_TAGFILTER_SCALE_DOWN = \
    "Tag filter expression that when it matches the instance tags, will make the task resize the instance with the next " \
    "instance type down in the range list of types. The task will remove the matching tags from the instance after " \
    "it has been executed."
PARAM_DESC_TAGFILTER_SCALE_UP = \
    "Tag filter expression that when it matches the instance tags, will make the task resize the instance with the next " \
    "instance type up in the range list of types. The task will remove the matching tags from the instance after " \
    "it has been executed."

PARAM_LABEL_RESIZED_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_INSTANCE_TYPES = "New instance sizes(s)"
PARAM_LABEL_RESIZE_MODE = "Resizing mode"
PARAM_LABEL_ASSUMED_TYPE = "Assumed type"
PARAM_LABEL_TRY_NEXT_IN_RANGE = "Try next in range"
PARAM_LABEL_SCALING_RANGE = "Scaling range"
PARAM_LABEL_TAGFILTER_SCALE_DOWN = "Scale down tag filter"
PARAM_LABEL_TAGFILTER_SCALE_UP = "Scale up tag filter"

GROUP_TITLE_INSTANCE_OPTIONS = "Instance options (For resizing instances with encrypted volumes make sure to " \
                               "grant kms:CreateGrant permission for the used kms key to the Ops Automator role)"
GROUP_TITLE_STEP_RESIZING_OPTIONS = "Step resize mode options"
GROUP_TITLE_RESIZE_BY_SPECIFIED_TYPE_OPTIONS = "Specified type resizing mode options"

INF_INSTANCE_NOT_RESIZED = "Instance type of instance {} is already {} instance is not resized"
INF_REMOVE_TAG = "Removing tag {} from instance {} as it is part of the scale up or down filter"
INF_INSTANCE_RESIZE_ACTION = "Resizing EC2 instance {} from type {} to {} for task {}"
INF_INSTANCE_RUNNING = "Resized instance {} is running"
INF_NOT_IN_SCALING_RANGE = "Type {} is not in scaling range {}"
INF_STOPPED_INSTANCE = "Instance {} not started as it was in a stopped state"
INF_RETRY_START = "Retry to start instance {} with alternative type {}"
INF_SET_ALT_TYPE = "Setting instance type for instance {} to {}"
INF_STOPPING = "Stopping instance {} for resizing"
INF_USE_ASSUMED_TYPE = "Assuming specified type {}"
INF_TAGS_NOT_SET_STEP = "Tag {} is part of the scale-up and/or scale-down filter, as setting this tag can re-trigger this task " \
                        "these tags are removed from the resized instance {}"
INF_NO_TAG_MATCH_NO_REPLACE = "Scale up tags {} and scale down tags {} do not match instance tags {}, instance {} with type {} " \
                              "will not be replaced"

ERR_ASSUMED_NOT_IN_SCALING_RANGE = "Value of {} parameter  must be in the list of types specified in the {} parameters"
ERR_AT_LEAST_TWO_TYPES = "Parameter {} must contain a list with at least 2 instance types"
ERR_BOTH_SCALING_FILTERS_EMPTY = "Parameter {} and {} can not bot be empty in {} mode"
ERR_INSTANCE_NOT_IN_STARTING_STATE = "Instance {} is not in a starting state, state is {}"
ERR_INSTANCE_RESIZING = "Error resizing instance {} to type {}, {}"
ERR_INSTANCE_STOP_TIMEOUT = "Instance {} not stopped for resize in time"
ERR_INVALID_INSTANCE_TYPE = "{} is not a valid instance type"
ERR_NO_TYPE_IN_SPECIFIED_MODE = "At least one instance type must be specified  in parameters {} for resizing mode {}"
ERR_NOT_IN_RANGE_NO_ASSUMED_TYPE = "No assumed type defined and current type {} is not in scaling range {}"
ERR_NOT_LONGER_AVAILABLE = "Instance {} to be resized is not longer available"
ERR_SET_TAGS = "Can not set tags to resized instance {}, {}"
ERR_STARTING = "Error starting instance {}, {}"
ERR_STOP_RESIZING = "Error stopping instance {} for resizing, {}"

WARM_MAX_SIZE = "Instance {}, {} at max of range {}, instance will not be replaced"
WARM_MIN_SIZE = "Instance {}, {} at min of range {}, instance will not be replaced"
WARN_TAGS_NOT_SET_TYPE = "Tag {} is removed or not set for instance {} as it is part of the tag filter used to scale up or down"

WARN_NO_TYPE_CAPACITY = "Not enough capacity for type {}"
WARN_BOTH_UP_DOWN = "Both scale up tag filter \"{}\" and scale down tag filter \"{}\" do match instance tags {}, " \
                    "instance {} will not be replaced"


class Ec2ResizeInstanceAction(ActionEc2EventBase):
    properties = {
        ACTION_TITLE: "EC2 Resize Instance",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Re-sizes EC2 instances",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "cd198dac-d7b6-4992-b748-6f2a95a1a041",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_SELECT_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 15,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION:
            "Reservations[*].Instances[]." +
            "{State:State.Name,InstanceId:InstanceId, CurrentState:CurrentState,InstanceType:InstanceType, Tags:Tags}" +
            "|[?contains(['running','stopped'],State)]",

        ACTION_EVENTS: {
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_PARAMETERS: {

            PARAM_RESIZED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_RESIZED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_RESIZED_INSTANCE_TAGS
            },
            PARAM_INSTANCE_TYPES: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TYPES,
                PARAM_TYPE: type([]),
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TYPES
            },
            PARAM_RESIZE_MODE: {
                PARAM_DESCRIPTION: PARAM_DESC_RESIZE_MODE.format(RESIZE_BY_SPECIFIED_TYPE, RESIZE_BY_STEP),
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: RESIZE_BY_SPECIFIED_TYPE,
                PARAM_ALLOWED_VALUES: [RESIZE_BY_SPECIFIED_TYPE, RESIZE_BY_STEP],
                PARAM_LABEL: PARAM_LABEL_RESIZE_MODE
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
                    PARAM_RESIZED_INSTANCE_TAGS,
                    PARAM_RESIZE_MODE
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_RESIZE_BY_SPECIFIED_TYPE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_INSTANCE_TYPES,
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_STEP_RESIZING_OPTIONS,
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
            "ec2:StartInstances",
            "ec2:StopInstances",
            "ec2:DescribeTags",
            "ec2:ModifyInstanceAttribute",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],

    }

    def __init__(self, action_arguments, action_parameters):

        ActionEc2EventBase.__init__(self, action_arguments, action_parameters)

        self.instance = self._resources_

        self.instance_id = self.instance["InstanceId"]
        self._ec2_client = None
        self._ec2_service = None

        self.instance_type_index = -1

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instance": self.instance_id,
            "task": self._task_
        }

        # instance type, list if alternatives must be retried if the type is not available
        self.new_instance_types = [s.strip() for s in self.get(PARAM_INSTANCE_TYPES, [])]

        self.resize_mode = self.get(PARAM_RESIZE_MODE)

        self.instance_type_index = -1

        self.scaling_range = [t.strip() for t in self.get(PARAM_SCALING_RANGE, [])]
        self.next_type_in_range = self.get(PARAM_TRY_NEXT_IN_RANGE, True) if self.resize_mode == RESIZE_BY_STEP else True

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
        instance_id = instance["InstanceId"]
        account = instance["AwsAccount"]
        region = instance["Region"]
        return "{}-{}-{}-{}".format(account, region, instance_id, log_stream_date())

    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        mode = parameters.get(PARAM_RESIZE_MODE)

        if mode == RESIZE_BY_SPECIFIED_TYPE:

            instance_types = parameters.get(PARAM_INSTANCE_TYPES, [])

            if len(instance_types) == 0:
                raise_value_error(ERR_NO_TYPE_IN_SPECIFIED_MODE, PARAM_RESIZE_MODE.format(mode))

            valid_types = services.ec2_service.Ec2Service.valid_instance_types()
            if valid_types not in [None, []]:
                for inst_type in [e.strip() for e in instance_types]:
                    if inst_type not in valid_types:
                        raise_value_error(ERR_INVALID_INSTANCE_TYPE.format(inst_type))
        else:
            scaling_range = parameters.get(PARAM_SCALING_RANGE, [])
            if len(scaling_range) < 2:
                raise_value_error(ERR_AT_LEAST_TWO_TYPES.format(PARAM_SCALING_RANGE))

            assumed_type = parameters.get(PARAM_ASSUMED_TYPE)
            if assumed_type is not None:
                if assumed_type not in scaling_range:
                    raise_value_error(ERR_ASSUMED_NOT_IN_SCALING_RANGE, PARAM_ASSUMED_TYPE, PARAM_SCALING_RANGE)

            scale_up_filter = parameters.get(PARAM_TAGFILTER_SCALE_UP)
            scale_down_filter = parameters.get(PARAM_TAGFILTER_SCALE_DOWN)

            if scale_up_filter is None and scale_down_filter is None:
                raise_value_error(ERR_BOTH_SCALING_FILTERS_EMPTY, PARAM_TAGFILTER_SCALE_UP, PARAM_TAGFILTER_SCALE_DOWN, mode)

        ActionEc2EventBase.check_tag_filters_and_tags(parameters, task_settings, [PARAM_RESIZED_INSTANCE_TAGS], logger)

        return parameters

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        parameters = task.get(TASK_PARAMETERS, {})
        if parameters.get(PARAM_RESIZE_MODE) == RESIZE_BY_SPECIFIED_TYPE:
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

    def _get_instance(self):

        return self.ec2_service.get(services.ec2_service.INSTANCES,
                                    InstanceIds=[self.instance_id],
                                    region=self._region_,
                                    select="Reservations[*].Instances[].{"
                                           "Tags:Tags,"
                                           "StateName:State.Name,"
                                           "StateCode:State.Code,"
                                           "StateStateReasonMessage:StateReason.Message,"
                                           "InstanceType:InstanceType,"
                                           "InstanceId:InstanceId}")

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = [
                "start_instances",
                "stop_instances",
                "create_tags",
                "delete_tags",
                "describe_instances",
                "modify_instance_attribute"
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

    def is_completed(self, start_data):

        def task_is_triggered_by_tag_event():
            task_change_events = self._events_.get(handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE, {}).get(
                handlers.TAG_CHANGE_EVENT, [])

            return handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT in task_change_events

        def tags_to_delete():
            tags = {}
            tags_on_instance = self.instance.get("Tags", {})
            for t in tags_on_instance.keys():
                if (self.scale_up_tagfilter and t in self.scale_up_tagfilter.get_filter_keys()) or \
                        (self.scale_down_tagfilter and t in self.scale_down_tagfilter.get_filter_keys()):
                    self._logger_.info(INF_REMOVE_TAG.format({t: tags_on_instance[t]}, self.instance_id))
                    tags[t] = tagging.TAG_DELETE
            return tags

        def delete_up_down_filter_tags():
            tags = tags_to_delete()
            if len(tags) > 0:
                tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                     tags=tags,
                                     can_delete=True,
                                     logger=self._logger_,
                                     resource_ids=[self.instance_id])

        def set_tags_on_resized_instance(new_instance_type, original_type):

            # tags set by action
            tags = self.build_tags_from_template(parameter_name=PARAM_RESIZED_INSTANCE_TAGS,
                                                 tag_variables={
                                                     TAG_PLACEHOLDER_NEW_INSTANCE_TYPE: new_instance_type,
                                                     TAG_PLACEHOLDER_ORG_INSTANCE_TYPE: original_type
                                                 })

            try:
                # if task is triggered by tagging event
                if task_is_triggered_by_tag_event():
                    # up or down tags filters should not match new tags as it would re-trigger execution of the task
                    if self.resize_mode == RESIZE_BY_STEP:

                        for t in tags.keys():
                            # remove tags that match up or down tag filters
                            if (self.scale_up_tagfilter and t in self.scale_up_tagfilter.get_filter_keys()) or \
                                    (self.scale_down_tagfilter and t in self.scale_down_tagfilter.get_filter_keys()):
                                self._logger_.info(INF_TAGS_NOT_SET_STEP.format({t: tags[t]}, self.instance_id))
                                del tags[t]

                tags.update(tags_to_delete())

                self.set_ec2_instance_tags_with_event_loop_check(client=self.ec2_client,
                                                                 instance_ids=[self.instance_id],
                                                                 tags_to_set=tags)

            except Exception as tag_ex:
                raise_exception(ERR_SET_TAGS, self.instance_id, tag_ex)

        resized = not start_data.get("not-resized", False)
        need_start = start_data.get("instance-running", True)

        if not resized and not need_start:
            delete_up_down_filter_tags()
            self._logger_.info(INF_STOPPED_INSTANCE, self.instance_id)
            return self.result

        if not need_start and resized:
            set_tags_on_resized_instance(start_data["new-instance-type"], start_data.get("org-instance-type", ""))
            return self.result

        # get current state of instance
        instance = self._get_instance()
        self._logger_.debug("Instance data is {}", safe_json(instance, indent=3))

        state_code = instance["StateCode"] & 0xFF

        # resized instance is running, done...
        if state_code == EC2_STATE_RUNNING:
            # instance is running
            self._logger_.info(INF_INSTANCE_RUNNING, self.instance_id)
            if resized:
                set_tags_on_resized_instance(instance["InstanceType"], start_data.get("org-instance-type", ""))
            else:
                delete_up_down_filter_tags()
            return self.result

        # in pending state, wait for next completion check
        if state_code == EC2_STATE_PENDING:
            return None

        raise_exception(ERR_INSTANCE_NOT_IN_STARTING_STATE, self.instance_id, instance)

    @classmethod
    def is_in_starting_or_running_state(cls, state):
        return (state & 0xFF) in EC2_STARTING_STATES if state is not None else False

    @classmethod
    def is_in_stopping_or_stopped_state(cls, state):
        return (state & 0xFF) in EC2_STOPPING_STATES

    @classmethod
    def insufficient_capacity(cls, ex):
        return type(ex).__name__ == "ClientError" and ex.response.get("Error", {}).get("Code", None) == INSUFFICIENT_CAPACITY

    def _set_new_instance_type(self):

        if self.resize_mode == RESIZE_BY_SPECIFIED_TYPE:
            self.instance_type_index += 1
            if self.instance_type_index >= len(self.new_instance_types):
                self.new_instance_type = self.original_type
                return
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
                self._logger_.warning(WARM_MAX_SIZE, self.instance_id, current_type, ", ".join(self.scaling_range))
                self.new_instance_type = current_type
            else:
                self.new_instance_type = self.scaling_range[self.scaling_range_index]
            return

        if self.scale_down:
            self.scaling_range_index -= 1
            if self.scaling_range_index < 0:
                self._logger_.warning(WARM_MIN_SIZE, self.instance_id, current_type, ", ".join(self.scaling_range))
                self.new_instance_type = current_type
            else:
                self.new_instance_type = self.scaling_range[self.scaling_range_index]
            return

        self._logger_.info(INF_NO_TAG_MATCH_NO_REPLACE, self.scale_up_str, self.scale_down_str, instance_tags, self.instance_id,
                           current_type)

        self.new_instance_type = current_type

    def _resize_instance(self):
        if self._get_instance()["InstanceType"] != self.new_instance_type:
            self._logger_.info("Setting instance size of instance {} to {}", self.instance_id, self.new_instance_type)
            try:
                self.ec2_client.modify_instance_attribute_with_retries(InstanceId=self.instance_id,
                                                                       InstanceType={"Value": self.new_instance_type})
            except Exception as ex:
                self._logger_.error(ERR_INSTANCE_RESIZING, self.instance_id, self.new_instance_type, ex)

    def _stop_instance(self):
        try:
            self._logger_.info(INF_STOPPING, self.instance_id)
            self.ec2_client.stop_instances_with_retries(InstanceIds=[self.instance_id])
        except Exception as ex:
            raise_exception(ERR_STOP_RESIZING, self.instance_id, ex)
        # wait for instance to stop, or until is signaled it is about to timeout
        while not self.time_out():
            time.sleep(10)
            state = self._get_instance()["StateCode"] & 0xFF
            if state == EC2_STATE_STOPPED:
                break
        if self.time_out():
            raise_exception(ERR_INSTANCE_STOP_TIMEOUT, self.instance_id)

    def _restart_instance(self):
        # for testing the parameter PARAM_TEST_UNAVAILABLE_TYPES can be used to simulate a InsufficientInstanceCapacity
        self._test_simulate_insufficient_instance_capacity()
        self.ec2_client.start_instances_with_retries(InstanceIds=[self.instance_id])
        with Timer(timeout_seconds=60, start=True) as t:
            started_instance = self._get_instance()

            # get state of started instance
            current_state = started_instance["StateCode"]

            if self.is_in_starting_or_running_state(current_state):
                # instance is starting
                return
            else:
                if t.timeout:
                    self._logger_.info(ERR_INSTANCE_NOT_IN_STARTING_STATE, self.instance_id, current_state)
                    raise_exception(ERR_INSTANCE_NOT_IN_STARTING_STATE, self.instance_id, current_state)

    def _test_simulate_insufficient_instance_capacity(self):

        if self.new_instance_type in self.get(PARAM_TEST_UNAVAILABLE_TYPES, []):
            raise ClientError(
                {
                    "Error": {
                        "Code": INSUFFICIENT_CAPACITY,
                        "Message": "Simulated {} Exception".format(INSUFFICIENT_CAPACITY)
                    }
                }, operation_name="start_instances")

    def execute(self):

        def should_resize_instance():
            if self.original_type == self.new_instance_type:
                self._logger_.info(INF_INSTANCE_NOT_RESIZED, self.instance_id, self.original_type)
                self.result["not-resized"] = True
                self.result[METRICS_DATA] = build_action_metrics(
                    action=self,
                    ReplacedInstances=0
                )
                return False
            return True

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        # get instance in it's current state
        instance = self._get_instance()
        if instance is None:
            raise_exception(ERR_NOT_LONGER_AVAILABLE, self.instance_id)
        instance_running = not self.is_in_stopping_or_stopped_state(instance["StateCode"])
        self.result["instance-running"] = instance_running

        self.original_type = instance["InstanceType"]
        self.result["org-instance-type"] = self.original_type

        self._set_new_instance_type()
        if not should_resize_instance():
            self.result["new-instance-type"] = self.new_instance_type
            return self.result

        self._logger_.info(INF_INSTANCE_RESIZE_ACTION, self.instance_id, self.original_type, self.new_instance_type, self._task_)

        # instance is running, stop it first so it can be resized
        if instance_running:
            self._stop_instance()

        self._resize_instance()

        if instance_running:

            while True:

                try:
                    self._restart_instance()
                    break

                except ClientError as ex:

                    # no capacity for this type
                    if self.insufficient_capacity(ex):
                        # try to set alternative type
                        self._logger_.warning(WARN_NO_TYPE_CAPACITY, self.new_instance_type)

                        self._set_new_instance_type()
                        if not should_resize_instance():
                            # resize to original type
                            self._resize_instance()
                            self._restart_instance()
                            self.result["new-instance-type"] = self.new_instance_type
                            return self.result

                        self._resize_instance()

                        self._logger_.info(INF_RETRY_START, self.instance_id, self.new_instance_type)

                except Exception as ex:
                    self.new_instance_type = self.original_type
                    self._resize_instance()
                    self._restart_instance()
                    raise_exception(ERR_STARTING, self.instance_id, str(ex))

            self.result[METRICS_DATA] = build_action_metrics(
                action=self,
                ResizedInstances=1,
                OrgInstanceSize=self.original_type,
                NewInstanceSize=self.new_instance_type
            )

        self.result["new-instance-type"] = self.new_instance_type

        return self.result
