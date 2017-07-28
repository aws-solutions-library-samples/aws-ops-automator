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
from datetime import datetime

import boto3

import actions
import handlers
import handlers.task_tracking_table as tracking
import services
from boto_retry import get_default_retry_strategy
from handlers.task_tracking_table import TaskTrackingTable
from services.aws_service import AwsService
from util import safe_dict, safe_json
from util.logger import Logger
from util.tag_filter_set import TagFilterSet

WARN_REGION_NOT_IN_TASK_CONFIGURATION = "Region from event {} is not configured in the list of regions for this task"

DEBUG_SELECT_PARAMETERS = "Selecting resources of type {} for service {} with parameters {}"

DEBUG_ACTION = "Action properties {}"
DEBUG_EVENT = "Handling event {}"
DEBUG_RESOURCE_NO_TAGS = "Resource {} does not support tags, resource is selected"
DEBUG_RESOURCE_NOT_SELECTED = "Resource {} not selected for task {}"
DEBUG_SELECT_ALL_RESOURCES = "* used as tag_filter all resources of type {} are selected"
DEBUG_SELECT_BY_TASK_NAME = "Resources of type {} that have tag name {} and have {} in its list of values are selected"
DEBUG_SELECTED_BY_TAG_FILTER = "Resource {} is selected because tag {} matches the filter {} set for task {}"
DEBUG_SELECTED_BY_TASK_NAME_IN_TAG_VALUE = "Resources {} selected because it has tag named {} and taskname {} is in list of values"
DEBUG_SELECTED_WILDCARD_TAG_FILTER = "Resource {} selected because the tagfilter set for task {} is set to *"
DEBUG_TAG_FILTER_USED_TO_SELECT_RESOURCES = "Tag-filter is used to select resources of type {}"

INFO_ACCOUNT = "Account is {}"
INFO_ACCOUNT_AGGREGATED = "Added action item {} for {} aggregated resources for account of type \"{}\" for task \"{}\""
INFO_ADDED_ITEMS = "Added {} action items for task {}"
INFO_AGGR_LEVEL = "Aggregation level for action is \"{}\" level"
INFO_ASSUMED_ROLE = "Assume role to select resources is {}"
INFO_IN_REGION = "{} in region {}"
INFO_RESOURCE = "Added action item {} for resource of type \"{}\" for task \"{}\""
INFO_RESOURCES_FOUND = "{} resources found"
INFO_RESOURCES_SELECTED = "{} resources selected"
INFO_RESULT = "Selecting resources took {:>.3f} seconds"
INFO_SELECTED_RESOURCES = "Selecting resources of type \"{}\" from service \"{}\" for task \"{}\""
INFO_TASK_AGGREGATED = "Added action item {} for {} aggregated resources of type {} for task {}"
INFO_USE_TAGS_TO_SELECT = "{}esource tags are used to select resources"

ERR_CAN_NOT_EXECUTE_WITH_THESE_RESOURSES = "Can not execute action \"{}\" for task \"{}\", reason {}"

MSG_NO_CROSS_ACCOUNT_ROLE = "No cross account role configured for task {} for account {} to select resources"

LOG_STREAM = "{}-{}-{:0>4d}{:0>2d}{:0>2d}"


class SelectResourcesHandler:
    """
    Class that handles the selection of AWS service resources for a task to perform its action on.
    """

    def __init__(self, event, context):

        self._context = context
        self._event = event
        self.task = event[handlers.HANDLER_EVENT_TASK]

        # setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, self.task[handlers.TASK_NAME], dt.year, dt.month, dt.day)
        debug = event[handlers.HANDLER_EVENT_TASK].get(handlers.TASK_DEBUG, False)
        self._logger = Logger(logstream=logstream, context=context, buffersize=40 if debug else 20, debug=debug)

        self._sts = None
        self._dynamodb = boto3.client("dynamodb")

        self.select_args = event.get(handlers.HANDLER_SELECT_ARGUMENTS, {})
        self.task_dt = event[handlers.HANDLER_EVENT_TASK_DT]

        self.action_properties = actions.get_action_properties(self.task[handlers.TASK_ACTION])
        self.action_class = actions.get_action_class(self.task[handlers.TASK_ACTION])
        self.task_parameters = self.task.get(handlers.TASK_PARAMETERS, {})
        self.aggregation_level = self.action_properties.get(actions.ACTION_AGGREGATION, actions.ACTION_AGGREGATION_RESOURCE)
        self.service = self.action_properties[actions.ACTION_SERVICE]
        self.resource_name = self.action_properties[actions.ACTION_RESOURCES]
        self.keep_tags = self.action_properties.get(actions.ACTION_KEEP_RESOURCE_TAGS, True)

        self.source = self._event.get(handlers.HANDLER_EVENT_SOURCE, handlers.UNKNOWN_SOURCE)

    @staticmethod
    def is_handling_request(event):
        """
        Tests if this handler handles the event.
        :param event: The event tyo test
        :return: True if the event is handled by this handler
        """
        return event.get(handlers.HANDLER_EVENT_ACTION, "") == handlers.HANDLER_ACTION_SELECT_RESOURCES

    @property
    def _task_tag(self):
        """
        Returns the name of the tag that contains the list of actions for a resource.
        :return: The name of the tag that contains the list of actions for a resource
        """
        name = os.environ.get(handlers.ENV_SCHEDULER_TAG_NAME)
        if name is None:
            name = handlers.DFLT_SCHEDULER_TAG
        return name

    @property
    def sts(self):
        if self._sts is None:
            self._sts = boto3.client("sts")
        return self._sts

    def _check_can_execute(self, selected_resources):
        """
        Checks if the action for the task can be executed with the selected resources
        :param selected_resources: 
        :return: 
        """
        check_method = getattr(self.action_class, actions.CHECK_CAN_EXECUTE, None)
        if check_method:
            try:
                check_method(selected_resources, self.task_parameters)
                return True
            except ValueError as ex:
                self._logger.error(ERR_CAN_NOT_EXECUTE_WITH_THESE_RESOURSES, self.task[handlers.TASK_ACTION],
                                   self.task[handlers.TASK_NAME], str(ex))
                return False
        return True

    def _account_service_sessions(self, service_name):
        """
        Returns a list of service instances for each handled account/role
        :return:
        """

        account = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_ACCOUNT)
        retry_strategy = get_default_retry_strategy(service=service_name, context=self._context)
        if account is not None:

            if account == AwsService.get_aws_account():
                yield services.create_service(service_name=service_name)
            else:
                for role in self.task[actions.ACTION_CROSS_ACCOUNT]:
                    if AwsService.account_from_role_arn(role) == account:
                        yield services.create_service(service_name=service_name, role_arn=role,
                                                      service_retry_strategy=retry_strategy)
                else:
                    self._logger.error(MSG_NO_CROSS_ACCOUNT_ROLE, self.task[handlers.TASK_NAME], account)
        else:

            if self.task.get(handlers.TASK_THIS_ACCOUNT, True):
                yield services.create_service(service_name=service_name, service_retry_strategy=retry_strategy)

            for role in self.task.get(handlers.TASK_CROSS_ACCOUNT_ROLES, []):
                yield services.create_service(service_name=service_name, role_arn=role, service_retry_strategy=retry_strategy)

    @property
    def _regions(self):
        """
        Returns the regions in where resources are selected
        :return:
        """
        regions = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_REGIONS)
        if regions is None:
            regions = self.task.get(handlers.TASK_REGIONS, [None])
        else:
            # check if the regions in the event are in the task configurations regions
            checked_regions = [r for r in regions if r in self.task[handlers.TASK_REGIONS]]
            if len(checked_regions) != len(regions):
                self._logger.warning(WARN_REGION_NOT_IN_TASK_CONFIGURATION, self._event)
                return checked_regions
        return regions if len(regions) > 0 else [None]

    def handle_request(self):
        """
        Handles the select resources request. Creates new actions for resources found for a task
        :return: Results of handling the request
        """

        def is_selected_resource(resource, taskname, tags_filter, does_resource_supports_tags):
            """
            Tests if item is a selected resource for this task
            :param resource: The tested resource
            :param taskname: Name of the task
            :param tags_filter: Tag filter
            :param does_resource_supports_tags: Trie if the resource supports tags
            :return: True if resource is selected
            """

            # No tags then always selected
            if not does_resource_supports_tags:
                self._logger.debug(DEBUG_RESOURCE_NO_TAGS, resource)
                return True

            tags = resource.get("Tags", {})

            # name of the tag that holds the list of tasks for this resource
            tagname = self._task_tag

            if tags_filter is None:
                # test if name of the task is in list of tasks in tag value
                if tagname in tags and taskname in tags[tagname].split(","):
                    self._logger.debug(DEBUG_SELECTED_BY_TASK_NAME_IN_TAG_VALUE, safe_json(resource, indent=2),
                                       tagname, taskname)
                    return True
            else:
                # using a tag filter, * means any tag
                if tags_filter == "*":
                    self._logger.debug(DEBUG_SELECTED_WILDCARD_TAG_FILTER, safe_json(resource, indent=2),
                                       taskname)
                    return True

                # test if there are any tags matching the tag filter
                matched_tags = TagFilterSet(tags_filter).pairs_matching_any_filter(tags)
                if len(matched_tags) != 0:
                    self._logger.debug(DEBUG_SELECTED_BY_TAG_FILTER, safe_json(resource, indent=2),
                                       matched_tags, tag_filter, taskname)
                    return True

            self._logger.debug(DEBUG_RESOURCE_NOT_SELECTED, safe_json(resource, indent=2), taskname)
            return False

        def resource_batches(resources):
            """
            Returns resources as chunks of size items. If the class has an optional custom aggregation function then the 
            reousrces are aggregated first using this function before applying the batch size
            :param resources: resources to process
            :return: Generator for blocks of resource items
            """

            aggregate_func = getattr(self.action_class, actions.CUSTOM_AGGREGATE_METHOD, None)
            batch_size = self.action_properties.get(actions.ACTION_BATCH_SIZE)

            for i in aggregate_func(resources, self.task_parameters) if aggregate_func is not None else [resources]:
                if batch_size is None:
                    yield i
                else:
                    first = 0
                    while first < len(i):
                        yield i[first:first + batch_size]
                        first += batch_size

        try:
            items = []
            start = datetime.now()

            self._logger.info("Handler {}", self.__class__.__name__)
            self._logger.debug(DEBUG_EVENT, safe_json(self._event, indent=2))
            self._logger.debug(DEBUG_ACTION, safe_json(self.action_properties, indent=2))

            self._logger.info(INFO_SELECTED_RESOURCES, self.resource_name, self.service, self.task[handlers.TASK_NAME])
            self._logger.info(INFO_AGGR_LEVEL, self.aggregation_level)

            task_level_aggregated_resources = []
            args = self._build_describe_argument()

            supports_tags = self.action_properties.get(actions.ACTION_RESOURCES) in services.create_service(
                self.service).resources_with_tags
            args["tags"] = supports_tags
            self._logger.info(INFO_USE_TAGS_TO_SELECT, "R" if supports_tags else "No r")

            task_name = self.task[handlers.TASK_NAME]

            # get optional tag filter
            tag_filter = self.task.get(handlers.TASK_TAG_FILTER)
            if tag_filter is None:
                self._logger.debug(DEBUG_SELECT_BY_TASK_NAME, self.resource_name, self._task_tag, task_name)
            elif tag_filter == "*":
                self._logger.debug(DEBUG_SELECT_ALL_RESOURCES, self.resource_name)
            else:
                self._logger.debug(DEBUG_TAG_FILTER_USED_TO_SELECT_RESOURCES, self.resource_name)

            with TaskTrackingTable(self._context) as actions_tracking:

                for service in self._account_service_sessions(self.service):
                    assumed_role = service.assumed_role

                    self._logger.info(INFO_ACCOUNT, service.aws_account)
                    if assumed_role is not None:
                        self._logger.info(INFO_ASSUMED_ROLE, assumed_role)

                    for region in self._regions:

                        if region is not None:
                            args["region"] = region
                        else:
                            if "region" in args:
                                del args["region"]

                        self._logger.debug(DEBUG_SELECT_PARAMETERS, self.resource_name, self.service, args)
                        # selecting a list of all resources in this account/region
                        all_resources = list(service.describe(self.resource_name, **args))

                        logstr = INFO_RESOURCES_FOUND.format(len(all_resources))
                        if region is not None:
                            logstr = INFO_IN_REGION.format(logstr, region)
                        self._logger.info(logstr)

                        # select resources that are processed by the task
                        selected = list([sr for sr in all_resources if is_selected_resource(sr, task_name, tag_filter,
                                                                                            supports_tags)])

                        if len(all_resources) > 0:
                            self._logger.info(INFO_RESOURCES_SELECTED, len(selected))
                        if len(selected) == 0:
                            continue

                        if not self.keep_tags:
                            for res in selected:
                                if "Tags" in res:
                                    del res["Tags"]

                        if self.aggregation_level == actions.ACTION_AGGREGATION_TASK:
                            task_level_aggregated_resources += selected
                        elif self.aggregation_level == actions.ACTION_AGGREGATION_ACCOUNT:

                            if self._check_can_execute(selected):
                                # create tasks action for account aggregated resources , optionally split in batch size chunks
                                for r in resource_batches(selected):
                                    action_item = actions_tracking.add_task_action(
                                        task=self.task,
                                        assumed_role=assumed_role,
                                        action_resources=r,
                                        task_datetime=self.task_dt,
                                        source=self.source)

                                    items.append(action_item)
                                    self._logger.info(
                                        INFO_ACCOUNT_AGGREGATED, action_item[tracking.TASK_TR_ID], len(r), self.resource_name,
                                        self.task[
                                            handlers.TASK_NAME])
                        else:
                            for res in selected:
                                # task action for each selected resource
                                action_item = actions_tracking.add_task_action(
                                    task=self.task,
                                    assumed_role=assumed_role,
                                    action_resources=res,
                                    task_datetime=self.task_dt,
                                    source=self.source)

                                items.append(action_item)
                                self._logger.info(INFO_RESOURCE, action_item[tracking.TASK_TR_ID], self.resource_name,
                                                  self.task[handlers.TASK_NAME])

                if self.aggregation_level == actions.ACTION_AGGREGATION_TASK and len(task_level_aggregated_resources) > 0:

                    if self._check_can_execute(task_level_aggregated_resources):
                        for r in resource_batches(task_level_aggregated_resources):
                            # create tasks action for task aggregated resources , optionally split in batch size chunks
                            action_item = actions_tracking.add_task_action(
                                task=self.task,
                                assumed_role=None,
                                action_resources=r,
                                task_datetime=self.task_dt,
                                source=self.source)

                            items.append(action_item)
                            self._logger.info(INFO_TASK_AGGREGATED, action_item[tracking.TASK_TR_ID], len(r), self.resource_name,
                                              self.task[handlers.TASK_NAME])

            self._logger.info(INFO_ADDED_ITEMS, len(items), self.task[handlers.TASK_NAME])

            running_time = float((datetime.now() - start).total_seconds())
            self._logger.info(INFO_RESULT, running_time)

            return safe_dict({
                "datetime": datetime.now().isoformat(),
                "running-time": running_time,
                "dispatched-tasks": items
            })

        finally:
            self._logger.flush()

    def _build_describe_argument(self):
        """
        Build the argument for the describe call that selects the resources
        :return: arguments for describe call
        """
        args = {}
        # get the mapping for parameters that should be used as parameters to the describe method call to select the resources
        action_parameters = self.action_properties.get(actions.ACTION_PARAMETERS, {})
        for p in [p for p in action_parameters if action_parameters[p].get(actions.PARAM_DESCRIBE_PARAMETER) is not None]:

            if self.task_parameters.get(p) is not None:
                args[action_parameters[p][actions.PARAM_DESCRIBE_PARAMETER]] = self.task_parameters[p]

        # also add describe method parameters specified as select parameters in the metadata of the action
        select_parameters = self.action_properties.get(actions.ACTION_SELECT_PARAMETERS, {})
        for p in select_parameters:
            args[p] = select_parameters[p]

        # region and account are separate describe parameters
        args.update({a: self.select_args[a] for a in self.select_args if a not in [handlers.HANDLER_EVENT_REGIONS,
                                                                                   handlers.HANDLER_EVENT_ACCOUNT]})
        # action specified select jmes-path expression for resources
        if actions.ACTION_SELECT_EXPRESSION in self.action_properties:
            # replace parameter placeholders with values. We cant use str.format here are the jmespath expression may contain {}
            # as well for projection of attributes, so the use placeholders for parameter names in format %paramname%
            jmes = self.action_properties[actions.ACTION_SELECT_EXPRESSION]
            for p in self.task_parameters:
                jmes = jmes.replace("%{}%".format(p), str(self.task_parameters[p]))
            args["select"] = jmes
        return args
