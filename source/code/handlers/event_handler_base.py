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


import json
import os
from datetime import datetime

import handlers
import services
import tagging
from boto_retry import get_client_with_retries
from configuration.task_configuration import TaskConfiguration
from helpers import safe_dict, safe_json
from main import lambda_handler
from outputs.queued_logger import QueuedLogger
from tagging.tag_filter_expression import TagFilterExpression

EVENT_DESCRIPTION = "description"
EVENT_LABEL = "label"
EVENT_EVENTS = "events"
EVENT_PARAMETER = "parameter"
EVENT_SCOPE_PARAMETER = "EventScopeParameter"
EVENT_SOURCE = "source"
EVENT_SOURCE_TITLE = "title"

DEBUG_EVENT_RESOURCES = "Selected resources by event handler are {}"
DEBUG_EVENT = "Scheduling task {} for  event  {} with parameters {}, account {} in region {}\nTask definition is {}"

ERR_HANDLING_EVENT_IN_BASE_HANDLER = "Error handling event in base state handler {}\n{}"
ERR_GETTING_EVENT_SOURCE_RESOURCE_TAGS = "Error retrieving tags for resource, {}"
ERR_NO_SESSION_FOR_GETTING_TAGS = "Can not use role {} to create session for getting tags from resource"

LOG_STREAM = "{}-{}{}{:0>4d}{:0>2d}{:0>2d}"


class EventHandlerBase(object):
    """
    Class that handles time based events from CloudWatch rules
    """

    def __init__(self, event, context, resource, handled_event_source, handled_event_detail_type, event_name_in_detail="event",
                 is_tag_change_event=False):

        self._context = context
        self._event = json.loads(event["Records"][0]["Sns"]["Message"])

        self._resource = resource
        self._handled_event_source = handled_event_source
        self._handled_detail_type = handled_event_detail_type
        self._role_executing_triggered_task = None
        self._is_tag_change_event = is_tag_change_event
        self.event_name_in_detail = event_name_in_detail
        self._this_account = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)

        # setup logging
        self._logger = EventHandlerBase._get_logger(self.__class__.__name__, account=self._event_account(),
                                                    region=self._event_region(),
                                                    context=self._context)

    @staticmethod
    def _get_logger(class_name, context, account=None, region=None):
        dt = datetime.utcnow()
        log_stream = LOG_STREAM.format(class_name,
                                       account + "-" if account is not None else "",
                                       region + "-" if region is not None else "", dt.year, dt.month, dt.day)
        return QueuedLogger(logstream=log_stream, buffersize=20, context=context)

    @staticmethod
    def is_subscribed_sns_message(event):
        record = event.get("Records", [{}])[0]
        events_topic_arn = os.getenv(handlers.ENV_EVENTS_TOPIC_ARN, None)
        return record.get("EventSource") == "aws:sns" and record.get("Sns", {}).get("TopicArn", "") == events_topic_arn

    @classmethod
    def is_handling_request(cls, event, context):
        with cls._get_logger(cls.__name__, account="invalid", context=context) as logger:
            if not EventHandlerBase.is_subscribed_sns_message(event):
                return False
            try:
                message_event = json.loads(event["Records"][0]["Sns"]["Message"])
                return cls.is_handling_event(message_event, logger=logger)
            except Exception as ex:
                message = "Event {} has unknown or invalid message".format(str(event), ex)
                logger.warning(message)
                raise Exception(message)

    @staticmethod
    def is_handling_event(event, logger):
        raise NotImplementedError("\"is_handling_event\"  method must be implemented for classes inherited from EventHandlerBase")

    def _event_triggers_task(self, task):

        task_name = task[handlers.TASK_NAME]

        if not task.get("enabled", True):
            self._logger.debug("Task {} is not enabled", task_name)
            return False

        if self._event_name() not in (
                task.get(handlers.TASK_EVENTS, {}).get(self._handled_event_source, {}).get(self._handled_detail_type, [])):
            self._logger.debug("Events {} for source {}, event detail type {}, is not handled by action {} in task {}",
                               self._event_name(),
                               self._handled_event_source,
                               self._handled_detail_type,
                               task[handlers.TASK_ACTION],
                               task_name)
            return False

        if self._event_region() is not None and self._event_region() not in task[handlers.TASK_REGIONS]:
            self._logger.debug("Region {} is not in the list of handled regions {} for task {}", self._event_region(),
                               ",".join(task[handlers.TASK_REGIONS]), task_name)
            return False

        # is the event for an instance in this account or does the account have a cross account rule in this task

        # events is not account specific (this is the case for S3 events)
        if self._event_account() is None:
            return True

        if task[handlers.TASK_THIS_ACCOUNT]:
            if self._event_account() == os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT):
                self._role_executing_triggered_task = handlers.get_account_role(self._this_account, task=task, logger=self._logger)
                return True
            else:
                self._logger.debug("This account {} is not handled by task {}, trying account list",
                                   self._event_account(),
                                   task_name)

        for acct in task.get(handlers.TASK_ACCOUNTS, []):

            acct = acct.strip()
            if self._event_account() == acct:
                self._role_executing_triggered_task = handlers.get_account_role(account=acct, task=task, logger=self._logger)
                return True
        else:
            self._logger.debug("Task {} has no  account  for account {} in region {}",
                               task_name,
                               self._event_account(),
                               self._event_region())

        if self._is_tag_change_event:
            return self._new_tags_triggers_task(task)

        return False

    def handle_request(self, use_custom_select=True):
        """
        Handled the cloudwatch rule timer event
        :return: Started tasks, if any, information
        """
        try:

            self._logger.info("Handling CloudWatch event {}", safe_json(self._event, indent=3))

            result = []
            start = datetime.now()

            dt = self._event_time()
            config_task = None

            source_resource_tags = None

            try:

                # for all events tasks in configuration
                for config_task in TaskConfiguration(context=self._context, logger=self._logger).get_tasks():

                    self._logger.debug_enabled = config_task.get(handlers.TASK_DEBUG, False)

                    if not self._event_triggers_task(task=config_task):
                        continue

                    # tasks that can react to events with a wider resource scope than the actual resource causing the event may
                    # have a filter that can is used to filter based on the tags of the resource
                    event_source_tag_filter = config_task.get(handlers.TASK_EVENT_SOURCE_TAG_FILTER, None)
                    if event_source_tag_filter is not None:
                        if source_resource_tags is None:
                            # get the tags for the source resource of the event
                            session = services.get_session(self._role_executing_triggered_task, logger=self._logger)
                            if session is None:
                                self._logger.error(ERR_NO_SESSION_FOR_GETTING_TAGS)
                                continue
                            try:
                                source_resource_tags = self._source_resource_tags(session, config_task)
                            except Exception as ex:
                                self._logger.error(ERR_GETTING_EVENT_SOURCE_RESOURCE_TAGS, ex)
                                continue

                            self._logger.debug("Tags for event source resource are  {}", source_resource_tags)

                        # apply filter to source resource tags
                        if not TagFilterExpression(event_source_tag_filter).is_match(source_resource_tags):
                            self._logger.debug("Tags of source resource do not match tag filter {}", event_source_tag_filter)
                            continue

                    task_name = config_task[handlers.TASK_NAME]
                    result.append(task_name)

                    select_parameters = self._select_parameters(self._event_name(), config_task)
                    if select_parameters is None:
                        continue

                    self._logger.debug(DEBUG_EVENT, task_name, self._event_name(), select_parameters,
                                       self._event_account(), self._event_region(), safe_json(config_task, indent=3))

                    # create an event for lambda function that scans for resources for this task
                    lambda_event = {
                        handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                        handlers.HANDLER_EVENT_CUSTOM_SELECT: use_custom_select,
                        handlers.HANDLER_SELECT_ARGUMENTS: {
                            handlers.HANDLER_EVENT_REGIONS: [self._event_region()],
                            handlers.HANDLER_EVENT_ACCOUNT: self._event_account(),
                            handlers.HANDLER_EVENT_RESOURCE_NAME: config_task[handlers.TASK_RESOURCE_TYPE],
                        },
                        handlers.HANDLER_EVENT_SOURCE: "{}:{}:{}".format(self._handled_event_source, self._handled_detail_type,
                                                                         self._event_name()),
                        handlers.HANDLER_EVENT_TASK: config_task,
                        handlers.HANDLER_EVENT_TASK_DT: dt
                    }

                    for i in select_parameters:
                        lambda_event[handlers.HANDLER_SELECT_ARGUMENTS][i] = select_parameters[i]

                    if self._event_resources() is not None:
                        self._logger.debug(DEBUG_EVENT_RESOURCES, safe_json(self._event_resources(), indent=3))
                        lambda_event[handlers.HANDLER_SELECT_RESOURCES] = self._event_resources()

                    if not handlers.running_local(self._context):
                        # start lambda function to scan for task resources
                        payload = str.encode(safe_json(lambda_event))
                        client = get_client_with_retries("lambda", ["invoke"],
                                                         context=self._context,
                                                         logger=self._logger)
                        client.invoke_with_retries(FunctionName=self._context.function_name,
                                                   InvocationType="Event",
                                                   LogType="None",
                                                   Payload=payload)
                    else:
                        # or if not running in lambda environment pass event to main task handler
                        lambda_handler(lambda_event, None)

                return safe_dict({
                    "datetime": datetime.now().isoformat(),
                    "running-time": (datetime.now() - start).total_seconds(),
                    "event-datetime": dt,
                    "started-tasks": result
                })

            except ValueError as ex:
                self._logger.error(ERR_HANDLING_EVENT_IN_BASE_HANDLER, ex, safe_json(config_task, indent=2))

        finally:
            self._logger.flush()

    def _select_parameters(self, event_name, task):
        raise NotImplementedError

    def _event_region(self):
        return self._event.get("region")

    def _event_account(self):
        return self._event.get("account")

    def _event_name(self):
        return self._event["detail"].get(self.event_name_in_detail, "")

    def _event_time(self):
        return self._event["time"]

    def _event_resources(self):
        return None

    def _source_resource_tags(self, session, task):
        raise NotImplementedError

    def _new_tags_triggers_task(self, task):

        # get the changed tags and the new tak values
        changed_tag_keys = set(self._event.get("detail", {}).get("changed-tag-keys", []))
        tags = self._event.get("detail", {}).get("tags", {})
        task_tag_filter_str = task.get(handlers.TASK_TAG_FILTER, None)

        if task_tag_filter_str is None:
            # if there is no tag filtering to select resources check if the task that holds the actions is updates
            task_tag_name = os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME, "")
            if task_tag_name not in changed_tag_keys:
                self._logger.debug("Value of task tag {} is not changed", task_tag_name)
                return False
            # check if the new value does include the name of this task
            task_tag_value = tags.get(task_tag_name, "")
            if not task[handlers.TASK_NAME] in tagging.split_task_list(task_tag_value):
                self._logger.debug("Task name \"{}\" not in value \"{}\" of task task {}", task[handlers.TASK_NAME], task_tag_value,
                                   task_tag_name)
                return False
            return True

        # there is a tag filter
        task_tag_filter = TagFilterExpression(task_tag_filter_str)

        # test if the new tasks match the filter
        if not task_tag_filter.is_match(tags):
            self._logger.debug("Tags {} do not match tag filter {}", tags, task_tag_filter_str)
            return False

        self._logger.debug("Tags {} do match tag filter {}", tags, task_tag_filter_str)
        return True
