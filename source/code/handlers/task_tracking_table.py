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
import math
import os
import types
import uuid
from datetime import datetime
from time import time, sleep

import boto3
from boto3.dynamodb.conditions import Attr, Key

import actions
import boto_retry
import handlers
import metrics
import services.aws_service
from helpers import safe_json
from helpers.dynamodb import build_record, as_dynamo_safe_types
from metrics.task_metrics import TaskMetrics
from outputs import raise_exception
from services.aws_service import AwsService

NOT_LONGER_ACTIVE_STATUSES = [handlers.STATUS_COMPLETED, handlers.STATUS_FAILED, handlers.STATUS_TIMED_OUT]

INF_SKIP_POSSIBLE_INCONSISTENT_ITEM = "Delay completion checking for task {}, Action is {}"

ERR_ITEMS_NOT_WRITTEN = "Items can not be written to action table, items not writen are {}, ({})"
ERR_WRITING_RESOURCES = "Error writing resources to bucket {}, key {} for action {}, {}"
ER_STATUS_UPDATE = "Error updating TaskTrackingTable, data is\n{}\nresp is {}, exception is {}"

WARN_PUT_METRICS_ERROR = "Unable to write task {} status metrics, {}"


class TaskTrackingTable(object):
    """
    Class that implements logic to create and update the status of action in a dynamodb table.
    """

    def __init__(self, context=None, logger=None):
        """
        Initializes the instance
        """
        self._table = None
        self._client = None
        self._new_action_items = []
        self._context = context
        self._logger = logger
        self._s3_client = None
        self._account = None
        self._run_local = handlers.running_local(self._context)
        self._resource_encryption_key = os.getenv(handlers.ENV_RESOURCE_ENCRYPTION_KEY, "")
        self._kms_client = None

    def __enter__(self):
        """
        Returns itself as the managed resource.
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Writes all cached action items to dynamodb table when going out of scope
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.flush()

    @property
    def s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto_retry.get_client_with_retries("s3", ["put_object"], context=self._context)
        return self._s3_client

    @property
    def kms_client(self):
        if self._kms_client is None:
            self._kms_client = boto_retry.get_client_with_retries("kms", ["encrypt"], context=self._context)
        return self._kms_client

    @property
    def account(self):
        return os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)

    # noinspection PyDictCreation
    def add_task_action(self, task, assumed_role, action_resources, task_datetime, source, task_group=None):

        item = {
            handlers.TASK_TR_ID: str(uuid.uuid4()),
            handlers.TASK_TR_NAME: task[handlers.TASK_NAME],
            handlers.TASK_TR_ACTION: task[handlers.TASK_ACTION],
            handlers.TASK_TR_CREATED: datetime.now().isoformat(),
            handlers.TASK_TR_CREATED_TS: int(time()),
            handlers.TASK_TR_SOURCE: source,
            handlers.TASK_TR_DT: task_datetime,
            handlers.TASK_TR_STATUS: handlers.STATUS_PENDING,
            handlers.TASK_TR_DEBUG: task[handlers.TASK_DEBUG],
            handlers.TASK_TR_NOTIFICATIONS: task[handlers.TASK_NOTIFICATIONS],
            handlers.TASK_TR_METRICS: task[handlers.TASK_METRICS],
            handlers.TASK_TR_DRYRUN: task[handlers.TASK_DRYRUN],
            handlers.TASK_TR_INTERNAL: task[handlers.TASK_INTERNAL],
            handlers.TASK_TR_INTERVAL: task[handlers.TASK_INTERVAL],
            handlers.TASK_TR_TIMEZONE: task[handlers.TASK_TIMEZONE],
            handlers.TASK_TR_TIMEOUT: task[handlers.TASK_TIMEOUT],
            handlers.TASK_TR_STARTED_TS: int(time()),
            handlers.TASK_TR_EXECUTE_SIZE: task[handlers.TASK_EXECUTE_SIZE],
            handlers.TASK_TR_SELECT_SIZE: task[handlers.TASK_SELECT_SIZE],
            handlers.TASK_TR_EVENTS: task.get(handlers.TASK_EVENTS, {}),
            handlers.TASK_TR_COMPLETION_SIZE: task[handlers.TASK_COMPLETION_SIZE],
            handlers.TASK_TR_TAGFILTER: task[handlers.TASK_TAG_FILTER],
            handlers.TASK_TR_GROUP: task_group,
            handlers.TASK_TR_SERVICE: task[handlers.TASK_SERVICE],
            handlers.TASK_TR_RESOURCE_TYPE: task[handlers.TASK_RESOURCE_TYPE]
        }

        item[handlers.TASK_TR_RUN_LOCAL] = self._run_local

        if assumed_role is not None:
            item[handlers.TASK_TR_ASSUMED_ROLE] = assumed_role
            item[handlers.TASK_TR_ACCOUNT] = services.account_from_role_arn(assumed_role)
        else:
            item[handlers.TASK_TR_ACCOUNT] = self.account

        if len(task[handlers.TASK_PARAMETERS]) > 0:
            item[handlers.TASK_TR_PARAMETERS] = task[handlers.TASK_PARAMETERS]

        parameters = item.get(handlers.TASK_TR_PARAMETERS, None)
        if parameters is not None:
            item[handlers.TASK_TR_PARAMETERS] = parameters

        # check if the class has a field or static method that returns true if the action class needs completion
        # this way we can make completion dependent of parameter values
        has_completion = getattr(actions.get_action_class(task[handlers.TASK_ACTION]), actions.ACTION_PARAM_HAS_COMPLETION, None)
        if has_completion is not None:
            # if it is static method call it passing the task parameters
            if isinstance(has_completion, types.FunctionType):
                has_completion = has_completion(parameters)
        else:
            # if it does not have this method test if the class has an us_complete method
            has_completion = getattr(actions.get_action_class(task[handlers.TASK_ACTION]),
                                     handlers.COMPLETION_METHOD, None) is not None

        item[handlers.TASK_TR_HAS_COMPLETION] = has_completion

        resource_data_str = safe_json(action_resources)

        encrypted = self._resource_encryption_key not in [None, ""]
        item[handlers.TASK_TR_ENCRYPTED_RESOURCES] = encrypted
        if encrypted:
            resource_data_str = base64.b64encode(self.kms_client.encrypt_with_retries(
                KeyId=self._resource_encryption_key, Plaintext=resource_data_str)["CiphertextBlob"])

        if len(resource_data_str) < int(os.getenv(handlers.ENV_RESOURCE_TO_S3_SIZE, 16)) * 1024:
            if encrypted:
                item[handlers.TASK_TR_RESOURCES] = action_resources if not encrypted else resource_data_str
            else:
                item[handlers.TASK_TR_RESOURCES] = as_dynamo_safe_types(action_resources)
        else:
            bucket = os.getenv(handlers.ENV_RESOURCE_BUCKET)
            key = "{}.json".format(item[handlers.TASK_TR_ID])

            try:
                self.s3_client.put_object_with_retries(Body=resource_data_str, Bucket=bucket, Key=key)
            except Exception as ex:
                raise_exception(ERR_WRITING_RESOURCES, bucket, key, item[handlers.TASK_TR_ID], ex)
            item[handlers.TASK_TR_S3_RESOURCES] = True

        self._new_action_items.append(item)

        return item

    @property
    def items(self):
        return len(self._new_action_items)

    def update_task(self, action_id, task=None, task_metrics=None, status=None, status_data=None):
        """
        Updates the status of an action in the tracking table
        :param action_id: action id
        :param task: name of the task
        :param task_metrics: collect task metrics
        :param status: new action status
        :param status_data: additional date as a dictionary to be added to the tracking table
        :return:
        """

        data = {handlers.TASK_TR_UPDATED: datetime.now().isoformat(), handlers.TASK_TR_UPDATED_TS: int(time())}
        if status is not None:
            data[handlers.TASK_TR_STATUS] = status

        # for completed tasks remove the concurrency id and the wait for completion start time so these items
        # are not longer visible the GSI of these tables
        if status in NOT_LONGER_ACTIVE_STATUSES:
            data[handlers.TASK_TR_CONCURRENCY_ID] = None
            data[handlers.TASK_TR_LAST_WAIT_COMPLETION] = None

            # set TTL for tasks to be remove after retention period
            if os.getenv(handlers.ENV_TASK_CLEANUP_ENABLED, "").lower() == "true":
                if status == handlers.STATUS_COMPLETED or os.getenv(handlers.ENV_KEEP_FAILED_TASKS, "").lower() == "false":
                    task_retention_hours = int(os.getenv(handlers.ENV_TASK_RETENTION_HOURS, 168))
                    ttl = (task_retention_hours * 3600) + math.trunc(time())
                    data[handlers.TASK_TR_TTL] = ttl

        if status_data is not None:
            for i in status_data:
                data[i] = status_data[i]

            data = as_dynamo_safe_types(data)
        self._update(action_id, data)

        if task is not None:
            self._put_task_status_metrics(task, status, task_level=task_metrics, data=status_data)

    def flush(self, timeout_event=None):
        """
        Writes all cached action items in batches to the dynamodb table
        :return:
        """

        items_to_write = []
        has_failed_items_to_retry = False

        tasks_data = {}

        # create items to write to table
        for item in self._new_action_items:
            task_name = item[handlers.TASK_TR_NAME]
            if task_name in tasks_data:
                tasks_data[task_name]["count"] += 1
            else:
                tasks_data[task_name] = {"count": 1, "task_level_metrics": item[handlers.TASK_TR_METRICS]}

            items_to_write.append(
                {
                    "PutRequest": {
                        "Item": build_record(item)
                    }
                })

        if len(tasks_data) > 0:
            with TaskMetrics(dt=datetime.utcnow(), logger=self._logger, context=self._context)as task_metrics:
                for name in tasks_data:
                    # number of submitted task instances for task
                    task_metrics.put_task_state_metrics(task_name=name,
                                                        metric_state_name=metrics.METRICS_STATUS_NAMES[handlers.STATUS_PENDING],
                                                        count=tasks_data[name]["count"],
                                                        task_level=tasks_data[name]["task_level_metrics"])

        if timeout_event is not None and timeout_event.is_set():
            return

        # buffer to hold a max of 25 items to write in a batch
        batch_write_items = []
        # write until all items are written
        while len(items_to_write) > 0 and (not (timeout_event.is_set() if timeout_event is not None else False)):

            try:
                batch_write_items.append(items_to_write.pop(0))
                if len(batch_write_items) == 25 or len(items_to_write) == 0:
                    putrequest = {self._action_table.name: batch_write_items}
                    resp = self._dynamodb_client.batch_write_item_with_retries(RequestItems=putrequest)

                    # unprocessed items are put back in the list of items to write
                    unprocessed_items = resp.get("UnprocessedItems", [])
                    has_failed_items_to_retry = has_failed_items_to_retry or len(unprocessed_items) > 0
                    for unprocessed_item in unprocessed_items:
                        has_failed_items_to_retry = True
                        items_to_write += unprocessed_items[unprocessed_item]
                    batch_write_items = []
                    sleep(1)
            except Exception as ex:
                # when there are items that are retried
                if has_failed_items_to_retry:
                    raise_exception(ERR_ITEMS_NOT_WRITTEN, ",".join([str(i) for i in items_to_write]), str(ex))

        if self._run_local:
            for i in self._new_action_items:
                TaskTrackingTable._run_local_stream_event(os.getenv(handlers.ENV_ACTION_TRACKING_TABLE), "INSERT", new_item=i,
                                                          context=self._context)

        self._new_action_items = []

    def _put_task_status_metrics(self, task, status, task_level, data):

        if status in metrics.METRICS_STATUS_NAMES:
            try:
                metrics.put_task_state_metrics(task_name=task,
                                               metric_state_name=metrics.METRICS_STATUS_NAMES[
                                                   status],
                                               task_level=task_level,
                                               context=self._context,
                                               logger=self._logger,
                                               data=data)
            except Exception as ex:
                if self._logger is not None:
                    self._logger.warning(WARN_PUT_METRICS_ERROR, task, ex)

    @property
    def _action_table(self):
        """
        Returns boto3 resource for tracking table.
        :return: action table resource
        """
        table_name = os.environ.get(handlers.ENV_ACTION_TRACKING_TABLE)
        if table_name is None:
            raise Exception("No tracking table name defined in environment variable {}".format(handlers.ENV_ACTION_TRACKING_TABLE))
        if self._table is None:
            self._table = boto3.resource('dynamodb').Table(table_name)
            boto_retry.add_retry_methods_to_resource(self._table, ["get_item", "update_item", "query", "scan"],
                                                     context=self._context)
        return self._table

    @property
    def _dynamodb_client(self):
        """
        Returns boto3 dynamodb client
        :return:
        """
        if self._client is None:
            self._client = boto_retry.get_client_with_retries("dynamodb", ["batch_write_item"], context=self._context)
        return self._client

    def _item_in_consistent_expected_state(self, item, expected_state=None):
        # check recently added or updated as these are from a secondary might not be in a consistent state (ConsistentRead can not
        # be used on global indexes)
        ts = item.get(handlers.TASK_TR_CREATED_TS)

        if ts is None or ((int(time()) - int(ts)) < 120):
            # do a consistent read on source task tracking table with consistent read
            checked_item = self._action_table.get_item_with_retries(Key={handlers.TASK_TR_ID: item[handlers.TASK_TR_ID]},
                                                                    ConsistentRead=True).get("Item", {})
            status = checked_item.get(handlers.TASK_TR_STATUS)  # must be set for new items
            action = checked_item.get(handlers.TASK_TR_ACTION)
            # check if item has the expected attributes in the tracking table
            if (expected_state is not None and status != expected_state) or action in [None, ""]:
                return False
        return True

    def _update(self, action_id, data):
        """
        Updates an item for the specified action id with the ata passed in as a dictionary
        :param action_id: Id of item to update
        :param data: dictionary containing fields to update
        :return:
        """
        resp = None
        old_item = None
        attributes = {}

        if handlers.running_local(self._context):
            resp = self._action_table.get_item_with_retries(Key={handlers.TASK_TR_ID: action_id}, ConsistentRead=True)
            old_item = resp.get("Item")

        for i in data:
            if data[i] is not None or "":
                attributes[i] = {"Action": "PUT", "Value": data[i]}
            else:
                attributes[i] = {"Action": "DELETE"}

        retries = 10
        while True:
            try:
                resp = self._action_table.update_item_with_retries(Key={handlers.TASK_TR_ID: action_id},
                                                                   AttributeUpdates=attributes,
                                                                   Expected={handlers.TASK_TR_ACTION: {
                                                                       "ComparisonOperator": "NOT_NULL"
                                                                   }},
                                                                   _expected_boto3_exceptions_=["ConditionalCheckFailedException"])
                break
            except Exception as ex:
                if "ConditionalCheckFailedException" in str(ex) and retries > 0:
                    retries -= 1
                    sleep(1)
                    continue
                else:
                    raise Exception(
                        ER_STATUS_UPDATE.format(safe_json(data, indent=3), safe_json(resp, indent=3), str(ex)))

        if self._run_local:
            resp = self._action_table.get_item_with_retries(Key={handlers.TASK_TR_ID: action_id}, ConsistentRead=True)
            TaskTrackingTable._run_local_stream_event(os.getenv(handlers.ENV_ACTION_TRACKING_TABLE), "UPDATE",
                                                      new_item=resp.get("Item"), old_item=old_item, context=self._context)

    def get_waiting_tasks(self, concurrency_key):
        """
        Returns list of waiting tasks with the specified concurrency key
        :param concurrency_key: concurrency key of the tasks
        :return: concurrency_key: list of waiting tasks
        """

        args = {
            "IndexName": "WaitForExecutionTasks",
            "Select": "ALL_ATTRIBUTES",
            "KeyConditionExpression": Key(handlers.TASK_TR_CONCURRENCY_ID).eq(concurrency_key),
        }
        not_longer_waiting = []
        waiting_list = []
        while True:
            resp = self._action_table.query_with_retries(**args)

            for i in resp.get("Items"):
                status = i.get(handlers.TASK_TR_STATUS)
                if status is None:
                    continue

                if status == handlers.STATUS_WAITING:
                    if self._item_in_consistent_expected_state(i, handlers.STATUS_WAITING):
                        waiting_list.append(i)
                elif status in NOT_LONGER_ACTIVE_STATUSES:
                    not_longer_waiting.append(i)

            last = resp.get("LastEvaluatedKey")

            if last is not None:
                args["ExclusiveStartKey"] = last
            else:
                break

            for i in not_longer_waiting:
                self.update_task(i[handlers.TASK_TR_ID], status_data={handlers.TASK_TR_CONCURRENCY_ID: None})

        return waiting_list

    def get_tasks_to_check_for_completion(self):

        waiting_for_completion_tasks = []

        args = {
            "IndexName": "WaitForCompletionTasks"
            # "FilterExpression": "#status = :waiting",
            # "ExpressionAttributeNames": {"#status": handlers.TASK_TR_STATUS},
            # "ExpressionAttributeValues": {":waiting": handlers.STATUS_WAIT_FOR_COMPLETION}
        }

        while True:
            resp = self._action_table.scan_with_retries(**args)

            not_longer_waiting = []
            for item in resp.get("Items", []):

                # only handle completion for tasks that are created in the same environment (local or lambda)
                running_local = handlers.running_local(self._context)
                local_running_task = item.get(handlers.TASK_TR_RUN_LOCAL, False)
                if running_local != local_running_task:
                    continue

                if item.get(handlers.TASK_TR_STATUS) in NOT_LONGER_ACTIVE_STATUSES:
                    not_longer_waiting.append(item)
                    continue

                if item.get(handlers.TASK_TR_STATUS) == handlers.STATUS_WAIT_FOR_COMPLETION:
                    waiting_for_completion_tasks.append(item)

            if "LastEvaluatedKey" in resp:
                args["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        # cleanup items
        for i in not_longer_waiting:
            self.update_task(i[handlers.TASK_TR_ID], status_data={handlers.TASK_TR_LAST_WAIT_COMPLETION: None})

        tasks_to_schedule_completion_for = []
        for i in waiting_for_completion_tasks:
            if not self._item_in_consistent_expected_state(i):
                self._logger.info(INF_SKIP_POSSIBLE_INCONSISTENT_ITEM, i[handlers.TASK_TR_ID], i[handlers.TASK_TR_ACTION])
                continue
            tasks_to_schedule_completion_for.append(i)

        return tasks_to_schedule_completion_for

    def get_task_item(self, action_id, status=None):
        """
        Gets a task item from the tracking table
        :param action_id: id of the task item
        :param status: Status of the item, use None for any status
        :return:
        """
        resp = self._action_table.get_item_with_retries(Key={handlers.TASK_TR_ID: action_id}, ConsistentRead=True)
        item = resp.get("Item", None)
        if item is not None:
            if status is None or item.get(handlers.TASK_TR_STATUS, None) == status:
                return item
        return None

    def get_task_items_for_job(self, task_group):

        job_tasks = []

        args = {
            "Select": "ALL_ATTRIBUTES",
            "ProjectionExpression": handlers.TASK_TR_ID,
            "ConsistentRead": True,
            "FilterExpression": Attr(handlers.TASK_TR_GROUP).eq(task_group)
        }

        while True:
            resp = self._action_table.scan_with_retries(**args)

            for item in resp.get("Items", []):
                job_tasks.append(item)
            if "LastEvaluatedKey" in resp:
                args["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        return job_tasks

    @staticmethod
    def _run_local_stream_event(table, table_action, new_item, old_item=None, context=None):

        # if not running in lambda environment create event that normally results from dynamodb inserts and pass directly
        # to the main lambda handler to simulate an event triggered by the dynamodb stream

        if old_item is None:
            old_item = {}
        account = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)
        region = services.get_session().region_name
        event = {
            "Records": [
                {
                    "eventName": table_action,
                    "eventSourceARN": "arn:aws:dynamodb:{}:{}:table/{}/stream/{}".format(region, account, table,
                                                                                         datetime.utcnow().isoformat()),
                    "eventSource": "aws:dynamodb",
                    "dynamodb": {
                        "NewImage": build_record(new_item),
                        "OldImage": build_record(old_item)
                    }
                }]
        }

        handler = handlers.get_class_for_handler("TaskTrackingHandler")(event, context)
        handler.handle_request()
