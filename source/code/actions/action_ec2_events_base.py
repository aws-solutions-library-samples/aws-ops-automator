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
import copy
import os

import handlers
import handlers.ec2_tag_event_handler
import services
import services.ec2_service
import tagging
from actions import ACTION_PARAM_EVENTS, ACTION_PARAM_TAG_FILTER
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from tagging.tag_filter_expression import TagFilterExpression

ERR_SET_TAGS = "Can not set tags to EC2 instances {}, {}"

WARN_LOOP_TAG = \
    __file__ + "Setting tags {} will trigger task to execute from TaskList tag \"{}={}\", action for instance {} executed " \
               "but tags not set"
WARN_LOOP_TAG_TAGFILTER = \
    "Setting tags {} will trigger task from matching TagFilter \"{}\", action for instance {} executed but tags not set"
WARN_TAG_FILER_TAG_COMBINATION = \
    "Tag updates in parameter \"{}\":\"{}\" combined with tag filter \"{}\" and EC2 tag change event {} could potentially " \
    "trigger execution loop of this task. The new tag values set by this task will be checked before changing the actual tags " \
    "on the resource If the values will trigger a new execution of this task, the task will be executed, but the tags " \
    "will not be set."


class ActionEc2EventBase(ActionBase):

    @staticmethod
    def check_tag_filters_and_tags(parameters, task_settings, tag_param_names, logger):
        """
        Check for tags and tags and tags.

        Args:
            parameters: (dict): write your description
            task_settings: (dict): write your description
            tag_param_names: (str): write your description
            logger: (todo): write your description
        """

        # check if tag events triggering is used
        task_events = task_settings.get(ACTION_PARAM_EVENTS, {})
        task_change_events = task_events.get(handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE, {}).get(
            handlers.TAG_CHANGE_EVENT, [])
        if handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT in task_change_events not in task_change_events:
            return

        # test for task filter
        tag_filter_str = task_settings.get(handlers.TASK_TR_TAGFILTER, None)
        if tag_filter_str in ["", None]:
            return

            # using any tag parameters
        for p in tag_param_names:
            tags = parameters.get(p, None)
            if tags in [None, ""]:
                continue

            logger.debug(WARN_TAG_FILER_TAG_COMBINATION,
                         p, parameters[p],
                         tag_filter_str,
                         handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT)

        return

    def __init__(self, action_arguments, action_parameters):
        """
        Initializes the arguments object.

        Args:
            self: (todo): write your description
            action_arguments: (str): write your description
            action_parameters: (todo): write your description
        """
        ActionBase.__init__(self, action_arguments, action_parameters)

    def set_ec2_instance_tags_with_event_loop_check(self, instance_ids, tags_to_set, client=None, region=None):
        """
        Set ec2 ec2 ec2 instance

        Args:
            self: (dict): write your description
            instance_ids: (int): write your description
            tags_to_set: (str): write your description
            client: (todo): write your description
            region: (str): write your description
        """

        def get_instances():
            """
            Returns a list. ec2 instances.

            Args:
            """
            ec2 = services.create_service("ec2", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

            return list(ec2.describe(services.ec2_service.INSTANCES,
                                     InstanceIds=instance_ids,
                                     region=region if region is not None else self._region_,
                                     tags=True,
                                     select="Reservations[*].Instances[].{Tags:Tags,InstanceId:InstanceId}"))

        def get_ec2_client():
            """
            Return ec2 ec2 ec2 client

            Args:
            """
            if client is not None:
                return client

            methods = ["create_tags",
                       "delete_tags"]

            return get_client_with_retries("ec2",
                                           methods=methods,
                                           region=region,
                                           session=self._session_,
                                           logger=self._logger_)

        try:
            if len(tags_to_set) > 0:
                tagged_instances = instance_ids[:]
                # before setting the tags check if these tags won't trigger a new execution of the task causing a loop
                task_events = self.get(ACTION_PARAM_EVENTS, {})
                task_change_events = task_events.get(handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE, {}).get(
                    handlers.TAG_CHANGE_EVENT, [])

                if handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT in task_change_events:

                    tag_name = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME)
                    tag_filter_str = self.get(ACTION_PARAM_TAG_FILTER, None)
                    tag_filter = TagFilterExpression(tag_filter_str) if tag_filter_str not in ["", None, "None"] else None

                    for instance in get_instances():

                        # tags currently on instance
                        instance_tags = instance.get("Tags", {})
                        # tags that have updated values when setting the tags

                        deleted_tags = {t: tags_to_set[t] for t in tags_to_set if
                                        tags_to_set[t] == tagging.TAG_DELETE and t in instance_tags}
                        new_tags = {t: tags_to_set[t] for t in tags_to_set if
                                    t not in instance_tags and tags_to_set[t] != tagging.TAG_DELETE}
                        updated_tags = {t: tags_to_set[t] for t in tags_to_set if
                                        tags_to_set[t] != tagging.TAG_DELETE and t in instance_tags and instance_tags[t] !=
                                        tags_to_set[t]}

                        updated_tags.update(new_tags)

                        # if there are updates
                        if any([len(t) > 0 for t in [new_tags, updated_tags, deleted_tags]]):

                            # this will be the new set of tags for the instance
                            updated_instance_tags = copy.deepcopy(instance_tags)
                            for t in deleted_tags:
                                del updated_instance_tags[t]
                            for t in updated_tags:
                                updated_instance_tags[t] = updated_tags[t]

                            # test if we have a tag filter and if the filter matches the new tags
                            if tag_filter is not None:

                                updated_tags_used_in_filter = set(updated_tags).intersection(tag_filter.get_filter_keys())
                                # tags updated that are in the tag filter
                                if len(updated_tags_used_in_filter) > 0:
                                    # test if updated tags trigger the task
                                    if tag_filter.is_match(updated_instance_tags):
                                        self._logger_.warning(WARN_LOOP_TAG_TAGFILTER,
                                                              tags_to_set,
                                                              tag_filter_str,
                                                              instance["InstanceId"])
                                        tagged_instances.remove(instance["InstanceId"])

                            # if no tag filter then check if the tag with the Ops Automator tasks does contain the name of the task
                            else:
                                task_list = updated_instance_tags.get(tag_name, "")
                                if tag_name in updated_tags and self._task_ in tagging.split_task_list(task_list):
                                    self._logger_.warning(WARN_LOOP_TAG, tags_to_set, task_list, tag_name, instance["InstanceId"])
                                    tagged_instances.remove(instance["InstanceId"])

                if len(tagged_instances) > 0:
                    tagging.set_ec2_tags(ec2_client=get_ec2_client(),
                                         resource_ids=tagged_instances,
                                         tags=tags_to_set)

        except Exception as ex:
            self._logger_.error(ERR_SET_TAGS, ','.join(instance_ids), str(ex))
