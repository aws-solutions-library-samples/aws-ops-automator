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

import os
import threading
import types
import uuid
from datetime import datetime

import boto3

import actions
import handlers
import services
import tagging
import tagging.tag_filter_set
from boto_retry import get_default_retry_strategy
from handlers.task_tracking_table import TaskTrackingTable
from helpers import safe_dict, safe_json
from metrics import put_task_select_data
from outputs import raise_exception
from outputs.queued_logger import QueuedLogger
from tagging.tag_filter_expression import TagFilterExpression


REMAINING_TIME_AFTER_STORE = 15
REMAINING_TIME_AFTER_DESCRIBE = 45

WARN_REGION_NOT_IN_TASK_CONFIGURATION = "Region from event {} is not configured in the list of regions for this task"

DEBUG_ACTION = "Action properties {}"
DEBUG_ADD_SINGLE_RESOURCE_TASK = "Created task item {} with 1 resource of type {} for task {}"
DEBUG_ADDED_AGGREGATED_RESOURCES_TASK = "Created task item {} with {} resources of type {} for task {}"
DEBUG_EVENT = "Handling event {}"
DEBUG_FILTER_METHOD = "Resource filtered out by {} {} method"
DEBUG_FILTERED_RESOURCE = "Returned resource by {} {} method is: {}"
DEBUG_RESOURCE_NO_TAGS = "Resource {}\n does not support tags, resource is selected"
DEBUG_RESOURCE_NOT_SELECTED = "Resource {} not selected for task {} because task not in tags {}"
DEBUG_RESOURCE_NOT_SELECTED_TAG_FILTER = "Resource {} not selected for task {}\n task filter does not match tags {}"
DEBUG_SELECT_ALL_RESOURCES = "* used as tag_filter all resources of type {} are selected"
DEBUG_SELECT_BY_TASK_NAME = "Resources of type {} that have tag name {} and have {} in its list of values are selected"
DEBUG_SELECT_PARAMETERS = "Selecting resources of type {} for service {} with parameters {}"
DEBUG_SELECTED_BY_TAG_FILTER = "Resource {}\n is selected because tags {} matches the filter {} set for task {}"
DEBUG_SELECTED_BY_TASK_NAME_IN_TAG_VALUE = "Resources {}\n selected because it has tag named {} and taskname {} is tag of value"
DEBUG_SELECTED_WILDCARD_TAG_FILTER = "Resource {}\n selected because the tagfilter set for task {} is set to *"
DEBUG_TAG_FILTER_USED_TO_SELECT_RESOURCES = "Tag-filter is used to select resources of type {}"

INFO_ACCOUNT = "Account is {}"
INFO_ADDED_ITEMS = "Added {} action items for task {}"
INFO_AGGR_LEVEL = "Aggregation level for action is \"{}\" level"
INFO_ASSUMED_ROLE = "Assumed role to select resources is {}"
INFO_REGION_AGGREGATED = "Added action item {} for {} aggregated resources for region {} of type \"{}\" for task \"{}\""
INFO_RESOURCES_FOUND = "{} resources found"
INFO_RESOURCES_SELECTED = "{} resources selected"
INFO_RESULT = "Selecting resources took {:>.3f} seconds"
INFO_SELECTED_RESOURCES = "Selecting resources of type \"{}\" from service \"{}\" for task \"{}\""
INFO_USE_TAGS_TO_SELECT = "{}esource tags are used to select resources"

ERR_CAN_NOT_EXECUTE_WITH_THESE_RESOURCES = "Can not execute action \"{}\" for task \"{}\", reason {}"
ERR_TIMEOUT_SELECT_OR_STORE = "Selection and storing of resources not completed after {} seconds, adjust select memory " \
                              "settings for task {}"
ERR_TIMEOUT_SELECTING_RESOURCES = "Timeout selecting {} resources from service {} for task {}"
ERR_CREATING_TASKS_FOR_SELECTED_RESOURCES = "Timeout creating tasks for selected resources in DynamoDB tracking table for task {}"
ERR_ACCOUNT_SKIPPED_NO_ROLE = "Account {} skipped because the required role could not be assumed or was not available"
ERR_SELECTING_TASK_RESOURCES = "Error selecting resources for task {}, {}"

LOG_STREAM = "{}-{}-{}{:0>4d}{:0>2d}{:0>2d}"


class SelectResourcesHandler(object):
    """
    Class that handles the selection of AWS service resources for a task to perform its action on.
    """

    def __init__(self, event, context, logger=None, tracking_store=None):
        """
        Initialize the event object.

        Args:
            self: (todo): write your description
            event: (dict): write your description
            context: (str): write your description
            logger: (todo): write your description
            tracking_store: (todo): write your description
        """

        def log_stream_name():
            """
            Get stream name.

            Args:
            """

            classname = self.__class__.__name__
            dt = datetime.utcnow()

            account = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_ACCOUNT, "")
            regions = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_REGIONS, [])

            if account is not None and len(regions) > 0:
                account_and_region = "-".join([account, regions[0]]) + "-"

            else:
                region = ""

                if self.sub_task is not None:
                    account = ""
                    if self._this_account:
                        if len(self._accounts) == 0:
                            account = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)
                    elif len(self._accounts) == 1:
                        account = self._accounts[0]

                    region = self._regions[0] if len(self._regions) == 1 else ""

                if account != "":
                    if region not in ["", None]:
                        account_and_region = "-".join([account, region]) + "-"
                    else:
                        account_and_region = account
                else:
                    account_and_region = ""

            return LOG_STREAM.format(classname, self.task[handlers.TASK_NAME], account_and_region, dt.year, dt.month, dt.day)

        self._context = context
        self._event = event
        self.task = event[handlers.HANDLER_EVENT_TASK]
        self.sub_task = event.get(handlers.HANDLER_EVENT_SUB_TASK, None)
        self.use_custom_select = event.get(handlers.HANDLER_EVENT_CUSTOM_SELECT, True)

        # the job id is used to correlate all generated tasks for the selected resources
        self.task_group = self._event.get(handlers.HANDLER_EVENT_TASK_GROUP, None)
        if self.task_group is None:
            self.task_group = str(uuid.uuid4())

        debug = event[handlers.HANDLER_EVENT_TASK].get(handlers.TASK_DEBUG, False)
        if logger is None:
            self._logger = QueuedLogger(logstream=log_stream_name(), context=context, buffersize=50 if debug else 20, debug=debug)
        else:
            self._logger = logger

        self._sts = None

        self.select_args = event.get(handlers.HANDLER_SELECT_ARGUMENTS, {})
        self.task_dt = event[handlers.HANDLER_EVENT_TASK_DT]

        self.action_properties = actions.get_action_properties(self.task[handlers.TASK_ACTION])
        self.action_class = actions.get_action_class(self.task[handlers.TASK_ACTION])
        self.task_parameters = self.task.get(handlers.TASK_PARAMETERS, {})
        self.metrics = self.task.get(handlers.TASK_METRICS, False)

        self.service = self.action_properties[actions.ACTION_SERVICE]
        self.keep_tags = self.action_properties.get(actions.ACTION_KEEP_RESOURCE_TAGS, True)

        self.source = self._event.get(handlers.HANDLER_EVENT_SOURCE, handlers.UNKNOWN_SOURCE)
        self.run_local = handlers.running_local(self._context)
        self._timer = None
        self._timeout_event = self._timeout_event = threading.Event()

        self.aggregation_level = self.action_properties.get(actions.ACTION_AGGREGATION, actions.ACTION_AGGREGATION_RESOURCE)
        if self.aggregation_level is not None and isinstance(self.aggregation_level, types.FunctionType):
            self.aggregation_level = self.aggregation_level(self.task_parameters)

        self.batch_size = self.action_properties.get(actions.ACTION_BATCH_SIZE)
        if self.batch_size is not None and isinstance(self.batch_size, types.FunctionType):
            self.batch_size = self.batch_size(self.task_parameters)

        self.actions_tracking = TaskTrackingTable(self._context, logger=self._logger) if tracking_store is None else tracking_store

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Tests if this handler handles the event.
        :param _:
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
        name = os.environ.get(handlers.ENV_AUTOMATOR_TAG_NAME)
        if name is None:
            name = handlers.DEFAULT_SCHEDULER_TAG
        return name

    @property
    def sts(self):
        """
        The client : class : ~plex.

        Args:
            self: (todo): write your description
        """
        if self._sts is None:
            self._sts = boto3.client("sts")
        return self._sts

    @property
    def _resource_name(self):
        """
        Get the resource name.

        Args:
            self: (todo): write your description
        """
        name = self.action_properties[actions.ACTION_RESOURCES]
        if name in [None, ""]:
            name = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_RESOURCE_NAME, "")
        return name

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
                self._logger.error(ERR_CAN_NOT_EXECUTE_WITH_THESE_RESOURCES, self.task[handlers.TASK_ACTION],
                                   self.task[handlers.TASK_NAME], str(ex))
                return False
        return True

    def _task_assumed_roles(self):
        """
        Returns a list of service instances for each handled account/role
        :return:
        """

        # account can optionally be passed in by events
        account = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_ACCOUNT)

        if account is not None:
            assumed_role = handlers.get_account_role(account=account, task=self.task, logger=self._logger)
            if assumed_role is None:
                if account != os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT):
                    self._logger.error(ERR_ACCOUNT_SKIPPED_NO_ROLE, account)
                yield None
            else:
                yield assumed_role

        else:
            # no role if processing scheduled task in own account
            if self._this_account:
                assumed_role = handlers.get_account_role(account=os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT),
                                                         task=self.task,
                                                         logger=self._logger)
                yield assumed_role

            for acct in self._accounts:
                # for external accounts
                assumed_role = handlers.get_account_role(account=acct, task=self.task, logger=self._logger)
                if assumed_role is not None:
                    yield assumed_role

    @property
    def _this_account(self):
        """
        Returns the account associated account.

        Args:
            self: (todo): write your description
        """
        if self.sub_task is not None:
            return self.sub_task[handlers.TASK_THIS_ACCOUNT]
        return self.task.get(handlers.TASK_THIS_ACCOUNT, True)

    @property
    def _accounts(self):
        """
        List of the list.

        Args:
            self: (todo): write your description
        """
        if self.sub_task is not None:
            return self.sub_task[handlers.TASK_ACCOUNTS]
        return self.task.get(handlers.TASK_ACCOUNTS, [])

    @property
    def _regions(self):
        """
        Returns the regions in where resources are selected
        :return:
        """
        regions = self._event.get(handlers.HANDLER_SELECT_ARGUMENTS, {}).get(handlers.HANDLER_EVENT_REGIONS)
        if regions is None:
            regions = self.sub_task[handlers.TASK_REGIONS] if self.sub_task is not None else self.task.get(
                handlers.TASK_REGIONS, [None])
        else:
            # check if the regions in the event are in the task configurations regions
            checked_regions = [r for r in regions if r in regions]
            if len(checked_regions) != len(regions):
                self._logger.warning(WARN_REGION_NOT_IN_TASK_CONFIGURATION, self._event)
                return checked_regions
        return regions if len(regions) > 0 else [None]

    def handle_request(self):
        """
        Handles the select resources request. Creates new actions for resources found for a task
        :return: Results of handling the request
        """

        def filter_by_action_filter(srv, used_role, r):
            """
            Filter the action filter for the given action.

            Args:
                srv: (array): write your description
                used_role: (bool): write your description
                r: (array): write your description
            """
            filter_method = getattr(self.action_class, actions.SELECT_AND_PROCESS_RESOURCE_METHOD, None)
            if filter_method is not None:
                r = filter_method(srv, self._logger, self._resource_name, r, self._context,
                                  self.task, used_role)
                if r is None:
                    self._logger.debug(DEBUG_FILTER_METHOD, self.action_class.__name__, actions.SELECT_AND_PROCESS_RESOURCE_METHOD)
                    return None
                else:
                    self._logger.debug(DEBUG_FILTERED_RESOURCE, self.action_class.__name__,
                                       actions.SELECT_AND_PROCESS_RESOURCE_METHOD, safe_json(r, indent=3))

            return r

        def is_selected_resource(aws_service, resource, used_role, taskname, tags_filter, does_resource_supports_tags):
            """
            Return true if the given service is selected.

            Args:
                aws_service: (todo): write your description
                resource: (dict): write your description
                used_role: (todo): write your description
                taskname: (str): write your description
                tags_filter: (str): write your description
                does_resource_supports_tags: (todo): write your description
            """

            # No tags then just use filter method if any
            if not does_resource_supports_tags:
                self._logger.debug(DEBUG_RESOURCE_NO_TAGS, resource)
                return filter_by_action_filter(srv=aws_service,
                                               used_role=used_role,
                                               r=resource)

            tags = resource.get("Tags", {})

            # name of the tag that holds the list of tasks for this resource
            tagname = self._task_tag

            if tags_filter is None:
                # test if name of the task is in list of tasks in tag value
                if (tagname not in tags) or (taskname not in tagging.split_task_list(tags[tagname])):
                    self._logger.debug(DEBUG_RESOURCE_NOT_SELECTED, safe_json(resource, indent=2), taskname,
                                       ','.join(["'{}'='{}'".format(t, tags[t]) for t in tags]))
                    return None
                self._logger.debug(DEBUG_SELECTED_BY_TASK_NAME_IN_TAG_VALUE, safe_json(resource, indent=2), tagname, taskname)
            else:
                # using a tag filter, * means any tag
                if tags_filter != tagging.tag_filter_set.WILDCARD_CHAR:
                    # test if there are any tags matching the tag filter
                    if not TagFilterExpression(tags_filter).is_match(tags):
                        self._logger.debug(DEBUG_RESOURCE_NOT_SELECTED_TAG_FILTER, safe_json(resource, indent=2), taskname,
                                           ','.join(["'{}'='{}'".format(t, tags[t]) for t in tags]))
                        return None
                    self._logger.debug(DEBUG_SELECTED_BY_TAG_FILTER, safe_json(resource, indent=2), tags, tag_filter_str, taskname)
                else:
                    self._logger.debug(DEBUG_SELECTED_WILDCARD_TAG_FILTER, safe_json(resource, indent=2), taskname)
                    return filter_by_action_filter(srv=aws_service,
                                                   used_role=used_role,
                                                   r=resource)

            return filter_by_action_filter(srv=aws_service,
                                           used_role=used_role,
                                           r=resource)

        def resource_batches(resources):
            """
            Returns resources as chunks of size items. If the class has an optional custom aggregation function then the 
            resources are aggregated first using this function before applying the batch size
            :param resources: resources to process
            :return: Generator for blocks of resource items
            """

            aggregate_func = getattr(self.action_class, actions.CUSTOM_AGGREGATE_METHOD, None)

            for i in aggregate_func(resources, self.task_parameters, self._logger) if aggregate_func is not None else [resources]:
                if self.batch_size is None:
                    yield i
                else:
                    first = 0
                    while first < len(i):
                        yield i[first:first + self.batch_size]
                        first += self.batch_size

        def setup_tag_filtering(t_name):
            """
            Setup a t_tag.

            Args:
                t_name: (str): write your description
            """
            # get optional tag filter
            no_select_by_tags = self.action_properties.get(actions.ACTION_NO_TAG_SELECT, False)
            if no_select_by_tags:
                tag_filter_string = tagging.tag_filter_set.WILDCARD_CHAR
            else:
                tag_filter_string = self.task.get(handlers.TASK_TAG_FILTER)

            # set if only a single task is required for selecting the resources, it is used to optimise the select
            select_tag = None
            if tag_filter_string is None:
                self._logger.debug(DEBUG_SELECT_BY_TASK_NAME, self._resource_name, self._task_tag, t_name)
                select_tag = self._task_tag
            elif tag_filter_string == tagging.tag_filter_set.WILDCARD_CHAR:
                self._logger.debug(DEBUG_SELECT_ALL_RESOURCES, self._resource_name)
            else:
                self._logger.debug(DEBUG_TAG_FILTER_USED_TO_SELECT_RESOURCES, self._resource_name)
                # build the tag expression that us used to filter the resources
                tag_filter_expression = TagFilterExpression(tag_filter_string)
                # the keys of the used tags
                tag_filter_expression_tag_keys = list(tag_filter_expression.get_filter_keys())
                # if there is only a single tag then we can optimize by just filtering on that specific tag
                if len(tag_filter_expression_tag_keys) == 1 and \
                        tagging.tag_filter_set.WILDCARD_CHAR not in tag_filter_expression_tag_keys[0]:
                    select_tag = tag_filter_expression_tag_keys[0]
            return select_tag, tag_filter_string

        def add_aggregated(aggregated_resources):
            """
            Add aggregated aggregated aggregated aggregations.

            Args:
                aggregated_resources: (todo): write your description
            """
            # create tasks action for aggregated resources , optionally split in batch size chunks
            for ra in resource_batches(aggregated_resources):
                if self._check_can_execute(ra):
                    action_item = self.actions_tracking.add_task_action(task=self.task,
                                                                        assumed_role=assumed_role,
                                                                        action_resources=ra,
                                                                        task_datetime=self.task_dt,
                                                                        source=self.source,
                                                                        task_group=self.task_group)

                    self._logger.debug(DEBUG_ADDED_AGGREGATED_RESOURCES_TASK, action_item[handlers.TASK_TR_ID], len(ra),
                                       self._resource_name, self.task[handlers.TASK_NAME])

                    self._logger.debug("Added item\n{}", safe_json(action_item, indent=3))

                    yield action_item

        def add_as_individual(resources):
            """
            Add an individual group to the group.

            Args:
                resources: (dict): write your description
            """
            for ri in resources:
                # task action for each selected resource
                if self._check_can_execute([ri]):
                    action_item = self.actions_tracking.add_task_action(task=self.task,
                                                                        assumed_role=assumed_role,
                                                                        action_resources=ri,
                                                                        task_datetime=self.task_dt,
                                                                        source=self.source,
                                                                        task_group=self.task_group)

                    self._logger.debug(DEBUG_ADD_SINGLE_RESOURCE_TASK, action_item[handlers.TASK_TR_ID], self._resource_name,
                                       self.task[handlers.TASK_NAME])
                    self._logger.debug("Added item\n{}", safe_json(action_item, indent=3))
                    yield action_item

        try:
            task_items = []
            start = datetime.now()

            self._logger.debug(DEBUG_EVENT, safe_json(self._event, indent=3))
            self._logger.debug(DEBUG_ACTION, safe_json(self.action_properties, indent=3))

            self._logger.info(INFO_SELECTED_RESOURCES, self._resource_name, self.service, self.task[handlers.TASK_NAME])
            self._logger.info(INFO_AGGR_LEVEL, self.aggregation_level)

            task_level_aggregated_resources = []

            args = self._build_describe_argument()

            service_resource_with_tags = services.create_service(self.service).resources_with_tags
            if self._resource_name == "":
                supports_tags = len(service_resource_with_tags) != 0
            else:
                supports_tags = self._resource_name.lower() in [r.lower() for r in service_resource_with_tags]

            args["tags"] = supports_tags
            self._logger.info(INFO_USE_TAGS_TO_SELECT, "R" if supports_tags else "No r")

            task_name = self.task[handlers.TASK_NAME]
            count_resource_items = 0
            selected_resource_items = 0

            select_on_tag, tag_filter_str = setup_tag_filtering(task_name)

            filter_func = getattr(self.action_class, actions.FILTER_RESOURCE_METHOD, None)

            # timer to guard selection time and log warning if getting close to lambda timeout
            if self._context is not None:
                self.start_timer(REMAINING_TIME_AFTER_DESCRIBE)
            try:

                for assumed_role in self._task_assumed_roles():
                    retry_strategy = get_default_retry_strategy(service=self.service, context=self._context)

                    service = services.create_service(service_name=self.service,
                                                      service_retry_strategy=retry_strategy, role_arn=assumed_role)

                    if self.is_timed_out():
                        break

                    # contains resources for account
                    account_level_aggregated_resources = []

                    self._logger.info(INFO_ACCOUNT, service.aws_account)
                    if assumed_role not in [None, ""]:
                        self._logger.info(INFO_ASSUMED_ROLE, assumed_role)

                    for region in self._regions:

                        # test for timeouts
                        if self.is_timed_out():
                            break

                        # handle region passed in the event
                        if region is not None:
                            args["region"] = region
                        else:
                            if "region" in args:
                                del args["region"]

                        # resources can be passed in the invent by event handlers
                        all_resources = self._event.get(handlers.HANDLER_SELECT_RESOURCES, None)

                        if all_resources is None:

                            # actions can have an optional method to select resources
                            action_custom_describe_function = getattr(self.action_class, "describe_resources", None)
                            if action_custom_describe_function is not None and self.use_custom_select:
                                all_resources = action_custom_describe_function(service, self.task, region)
                            else:
                                # select resources from the service
                                self._logger.debug(DEBUG_SELECT_PARAMETERS, self._resource_name, self.service, args)
                                # selecting a list of all resources in this account/region
                                all_resources = list(service.describe(self._resource_name,
                                                                      filter_func=filter_func,
                                                                      select_on_tag=select_on_tag,
                                                                      **args))
                            # test for timeout
                            if self.is_timed_out():
                                break

                            count_resource_items += len(all_resources)

                        self._logger.info(INFO_RESOURCES_FOUND, len(all_resources))

                        # select resources that are processed by the task
                        selected_resources = []
                        for sr in all_resources:
                            sel = is_selected_resource(aws_service=service,
                                                       resource=sr,
                                                       used_role=assumed_role,
                                                       taskname=task_name,
                                                       tags_filter=tag_filter_str,
                                                       does_resource_supports_tags=supports_tags)
                            if sel is not None:
                                selected_resources.append(sel)

                        selected_resource_items += len(selected_resources)

                        # display found and selected resources
                        if len(all_resources) > 0:
                            self._logger.info(INFO_RESOURCES_SELECTED, len(selected_resources))
                            if len(selected_resources) == 0:
                                continue

                        # delete tags if not needed by the action
                        if not self.keep_tags:
                            for res in selected_resources:
                                if "Tags" in res:
                                    del res["Tags"]

                        # add resources to total list of resources for this task
                        if self.aggregation_level == actions.ACTION_AGGREGATION_TASK:
                            task_level_aggregated_resources += selected_resources

                        # add resources to list of resources for this account
                        if self.aggregation_level == actions.ACTION_AGGREGATION_ACCOUNT:
                            account_level_aggregated_resources += selected_resources

                        # add batch(es) of resources for this region
                        if self.aggregation_level == actions.ACTION_AGGREGATION_REGION and len(selected_resources) > 0:
                            task_items += list(add_aggregated(selected_resources))

                        # no aggregation, add each individual resource
                        if self.aggregation_level == actions.ACTION_AGGREGATION_RESOURCE and len(selected_resources) > 0:
                            task_items += list(add_as_individual(selected_resources))

                    # at the end of the region loop, check if aggregated resources for account need to be added
                    if self.aggregation_level == actions.ACTION_AGGREGATION_ACCOUNT and len(account_level_aggregated_resources) > 0:
                        task_items += list(add_aggregated(account_level_aggregated_resources))

                # at the end of the accounts loop, check if aggregated resources for task need to be added
                if self.aggregation_level == actions.ACTION_AGGREGATION_TASK and len(task_level_aggregated_resources) > 0:
                    task_items += list(add_aggregated(task_level_aggregated_resources))
            except Exception as ex:
                raise_exception(ERR_SELECTING_TASK_RESOURCES, self.task[handlers.TASK_NAME], ex)

            finally:
                if self._timer is not None:
                    # cancel time used avoid timeouts when selecting resources
                    self._timer.cancel()
                    if self.is_timed_out():
                        raise_exception(ERR_TIMEOUT_SELECTING_RESOURCES, self._resource_name, self.service, task_name)

                    self.start_timer(REMAINING_TIME_AFTER_STORE)

                    self.actions_tracking.flush(self._timeout_event)
                    if self.is_timed_out():
                        raise_exception(ERR_CREATING_TASKS_FOR_SELECTED_RESOURCES, task_name)
                    self._timer.cancel()
                else:
                    self.actions_tracking.flush()

            self._logger.info(INFO_ADDED_ITEMS, len(task_items), self.task[handlers.TASK_NAME])

            running_time = float((datetime.now() - start).total_seconds())
            self._logger.info(INFO_RESULT, running_time)

            if self.metrics:
                put_task_select_data(task_name=task_name,
                                     items=count_resource_items,
                                     selected_items=selected_resource_items,
                                     logger=self._logger,
                                     selection_time=running_time)

            return safe_dict({
                "datetime": datetime.now().isoformat(),
                "running-time": running_time,
                "dispatched-tasks": task_items
            })

        finally:
            self._logger.flush()

    def select_timed_out(self):
        """
        Function is called when the handling of the request times out
        :return:
        """
        time_used = int(os.getenv(handlers.ENV_LAMBDA_TIMEOUT, 900)) - int((self._context.get_remaining_time_in_millis() / 1000))
        self._logger.error(ERR_TIMEOUT_SELECT_OR_STORE, time_used, self.task[handlers.TASK_NAME])
        self._timeout_event.set()
        self._logger.flush()
        self._timer.cancel()

    def start_timer(self, remaining):
        """
        Starts the timer.

        Args:
            self: (todo): write your description
            remaining: (todo): write your description
        """
        execution_time_left = (self._context.get_remaining_time_in_millis() / 1000.00) - remaining
        self._timer = threading.Timer(execution_time_left, self.select_timed_out)
        self._timer.start()

    def is_timed_out(self):
        """
        Return true if the event is set.

        Args:
            self: (todo): write your description
        """
        return self._timeout_event is not None and self._timeout_event.is_set()

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
        if types.FunctionType == type(select_parameters):
            select_parameters = select_parameters(self.task, self.task_parameters)
        for p in select_parameters:
            args[p] = select_parameters[p]

        # region and account are separate describe parameters
        args.update({a: self.select_args[a] for a in self.select_args if a not in [handlers.HANDLER_EVENT_REGIONS,
                                                                                   handlers.HANDLER_EVENT_ACCOUNT,
                                                                                   handlers.HANDLER_EVENT_RESOURCE_NAME]})
        # action specified select jmes-path expression for resources
        if actions.ACTION_SELECT_EXPRESSION in self.action_properties:
            # replace parameter placeholders with values. We cant use str.format here are the jmespath expression may contain {}
            # as well for projection of attributes, so the use placeholders for parameter names in format %paramname%
            jmes = self.action_properties[actions.ACTION_SELECT_EXPRESSION]
            for p in self.task_parameters:
                jmes = jmes.replace("%{}%".format(p), str(self.task_parameters[p]))
            args["select"] = jmes
        return args
