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
import uuid
from datetime import datetime
from decimal import Decimal
from time import time

import boto3
from boto3.dynamodb.conditions import Attr, Key

import handlers
import main
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from services.aws_service import AwsService
from util import safe_json

# name of environment variable that hold the dynamodb action table
TASK_TR_ACCOUNT = "Account"
TASK_TR_ACTION = "Action"
TASK_TR_ASSUMED_ROLE = "AssumedRole"
TASK_TR_ASSUMED_ROLESTATUS = "Status"
TASK_TR_CREATED = "Created"
TASK_TR_CREATED_TS = "CreatedTs"
TASK_TR_STARTED_TS = "StartedTs"
TASK_TR_DEBUG = "Debug"
TASK_TR_DRYRUN = "Dryrun"
TASK_TR_INTERNAL = "Internal"
TASK_TR_DT = "TaskDatetime"
TASK_TR_ERROR = "Error"
TASK_TR_EXECUTION_TIME = "ExecutionTime"
TASK_TR_START_EXECUTION_TIME = "StartExecutionTime"
TASK_TR_ID = "Id"
TASK_TR_NAME = "TaskName"
TASK_TR_PARAMETERS = "Parameters"
TASK_TR_RESOURCES = "Resources"
TASK_TR_RESULT = "ActionResult"
TASK_TR_START_RESULT = "ActionStartResult"
TASK_TR_SOURCE = "Source"
TASK_TR_STATUS = "Status"
TASK_TR_TIMEOUT = "TaskTimeout"
TASK_TR_UPDATED = "Updated"
TASK_TR_UPDATED_TS = "UpdatedTs"
TASK_TR_CONCURRENCY_ID = "ConcurrencyId"
TASK_TR_CONCURRENCY_KEY = "ConcurrencyKey"
TASK_TR_LAST_WAIT_COMPLETION = "LastCompletionCheck"
TASK_TR_EXECUTION_LOGSTREAM = "LogStream"

STATUS_PENDING = "pending"
STATUS_STARTED = "started"
STATUS_WAIT_FOR_COMPLETION = "wait-to-complete"
STATUS_COMPLETED = "completed"
STATUS_TIMED_OUT = "timed-out"
STATUS_FAILED = "failed"
STATUS_WAITING = "wait-for-exec"

ITEMS_NOT_WRITTEN = "Items can not be written to action table, items not writen are {}, ({})"

class TaskTrackingTable:
    """
    Class that implements logic to create and update the status of action in a dynamodb table.
    """

    def __init__(self, context=None):
        """
        Initializes the instance
        """
        self._table = None
        self._client = None
        self._new_action_items = []
        self._context = context

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

    def add_task_action(self, task, assumed_role, action_resources, task_datetime, source):
        """
        Creates and adds a new action to be written to the tracking table. Note that the items are kept in an internal
        buffer and written in batches to the dynamodb table when the instance goes out of scope or the close method
        is called explicitly.
        :param task: Task that executes the action
        :param assumed_role: Role to assume to execute the action
        :param action_resources: Resources on which the action is performed
        :param task_datetime: Time the task was scheduled for
        :param source of event that started the task
        test run their actions
        :return: Created item
        """
        item = {
            TASK_TR_ID: str(uuid.uuid4()),
            TASK_TR_NAME: task[handlers.TASK_NAME],
            TASK_TR_ACTION: task[handlers.TASK_ACTION],
            TASK_TR_CREATED: datetime.now().isoformat(),
            TASK_TR_CREATED_TS: int(time()),
            TASK_TR_SOURCE: source,
            TASK_TR_DT: task_datetime,
            TASK_TR_RESOURCES: safe_json(action_resources),
            TASK_TR_STATUS: STATUS_PENDING,
            TASK_TR_DEBUG: task[handlers.TASK_DEBUG],
            TASK_TR_DRYRUN: task[handlers.TASK_DRYRUN],
            TASK_TR_INTERNAL: task[handlers.TASK_INTERNAL],
            TASK_TR_TIMEOUT: task[handlers.TASK_TIMOUT]
        }
        if assumed_role is not None:
            item[TASK_TR_ASSUMED_ROLE] = assumed_role
            item[TASK_TR_ACCOUNT] = AwsService.account_from_role_arn(assumed_role)
        else:
            item[TASK_TR_ACCOUNT] = AwsService.get_aws_account()

        if len(task[handlers.TASK_PARAMETERS]) > 0:
            item[TASK_TR_PARAMETERS] = task[handlers.TASK_PARAMETERS]

        if item[TASK_TR_PARAMETERS]:
            item[TASK_TR_PARAMETERS] = safe_json(item[TASK_TR_PARAMETERS])

        self._new_action_items.append(item)
        return item

    @property
    def items(self):
        return len(self._new_action_items)

    def update_action(self, action_id, status=None, status_data=None):
        """
        Updates the status of an action in the tracking table
        :param action_id: action id
        :param status: new action status
        :param status_data: additional date as a dictionary to be added to the tracking table
        :return:
        """

        data = {TASK_TR_UPDATED: datetime.now().isoformat(), TASK_TR_UPDATED_TS: int(time())}
        if status is not None:
            data[TASK_TR_STATUS] = status

        # for completed tasks remove the concurrency id and the wait for completion start time so these items
        # are not longer visible the GSI of these tables
        if status in [STATUS_COMPLETED, STATUS_FAILED, STATUS_TIMED_OUT]:
            data[TASK_TR_CONCURRENCY_ID] = None
            data[TASK_TR_LAST_WAIT_COMPLETION] = None

        if status_data is not None:
            for i in status_data:
                data[i] = status_data[i]
        self._update(action_id, data)

    @staticmethod
    def typed_item(o):
        if isinstance(o, bool):
            return {"BOOL": o}
        if isinstance(o, int) or isinstance(o, float) or isinstance(o, Decimal):
            return {"N": str(o)}
        return {"S": str(o)}

    def flush(self):
        """
        Writes all cached action items in batches to the dynamodb table
        :return:
        """

        items_to_write = []
        has_failed_items_to_retry = False

        # create items to write to table
        for item in self._new_action_items:
            items_to_write.append(
                {
                    "PutRequest": {
                        "Item": {attr: TaskTrackingTable.typed_item(item[attr]) for attr in item if item[attr] is not None}
                    }
                })

        # buffer to hold a max of 25 items to write in a batch
        batch_write_items = []
        # write until all items are written
        while len(items_to_write) > 0:

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
                        items_to_write.append(unprocessed_item)
                    batch_write_items = []
            except Exception as ex:
                # when there are items that are retried to write check for timeout in loop
                if has_failed_items_to_retry:
                    raise Exception(ITEMS_NOT_WRITTEN.format(",".join([str(i) for i in items_to_write]), str(ex)))

        if self._context is None:
            for i in self._new_action_items:
                TaskTrackingTable._simulate_stream_processing("INSERT", i)

        self._new_action_items = []

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
            add_retry_methods_to_resource(self._table, ["get_item", "update_item", "query", "scan"], context=self._context)
        return self._table

    @property
    def _dynamodb_client(self):
        """
        Returns boto3 dynamodb client
        :return:
        """
        if self._client is None:
            self._client = get_client_with_retries("dynamodb", ["batch_write_item"], context=self._context)
        return self._client

    def _update(self, action_id, data):
        """
        Updates an item for the specified action id with the ata passed in as a dictionary
        :param action_id: Id of item to update
        :param data: dictionary containing fields to update
        :return:
        """
        resp = None
        old_item = None
        try:
            if self._context is None:
                resp = self._action_table.get_item_with_retries(Key={TASK_TR_ID: action_id}, ConsistentRead=True)
                old_item = resp.get("Item")

            attributes = {}
            for i in data:
                if data[i] is not None or "":
                    attributes[i] = {"Action": "PUT", "Value": data[i]}
                else:
                    attributes[i] = {"Action": "DELETE"}
            resp = self._action_table.update_item_with_retries(
                Key={TASK_TR_ID: action_id},
                AttributeUpdates=attributes)
        except Exception as ex:
            raise Exception("Error updating TaskTrackingTable, data is {}, resp is {}, exception is {}".format(data, resp, str(ex)))

        if self._context is None:
            resp = self._action_table.get_item_with_retries(Key={TASK_TR_ID: action_id}, ConsistentRead=True)
            TaskTrackingTable._simulate_stream_processing("UPDATE", resp.get("Item"), old_item)

    def get_waiting_tasks(self, concurrency_key):
        """
        Returns list of waiting tasks with the specified concurrency key
        :param concurrency_key: concurrency key of the tasks
        :return: concurrency_key: list of waiting tasks
        """
        args = {
            "IndexName": "WaitForExecutionTasks",
            "Select": "ALL_ATTRIBUTES",
            "KeyConditionExpression": Key(TASK_TR_CONCURRENCY_ID).eq(concurrency_key),
            "FilterExpression": Attr(TASK_TR_STATUS).eq(STATUS_WAITING)
        }
        waiting_list = []
        while True:
            resp = self._action_table.query_with_retries(**args)
            waiting_list += resp.get("Items", [])

            last = resp.get("LastEvaluatedKey")

            if last is not None:
                args["ExclusiveStartKey"] = last
            else:
                break

        return waiting_list

    def get_tasks_to_check_for_completion(self):

        waiting_for_completion_tasks = []

        args = {
            # items are only in the GSi if the StartWaitCompletionIndex has a value
            "IndexName": "WaitForCompletionTasks"
        }

        while True:
            resp = self._action_table.scan_with_retries(**args)

            waiting_for_completion_tasks += resp.get("Items", [])

            if "LastEvaluatedKey" in resp:
                args["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        return waiting_for_completion_tasks

    @staticmethod
    def _simulate_stream_processing(table_action, new_item, old_item=None):

        # if not running in lambda environment create event that normally results from dynamodb inserts and pass directly
        # to the main lambda handler to simulate an event triggered by the dynamodb stream

        if old_item is None:
            old_item = {}
        account = AwsService.get_aws_account()
        region = boto3.Session().region_name
        table = os.environ.get(handlers.ENV_ACTION_TRACKING_TABLE)
        event = {
            "Records": [
                {
                    "eventName": table_action,
                    "eventSourceARN": "arn:aws:dynamodb:{}:{}:table/{}/stream/{}".format(region, account, table,
                                                                                      datetime.utcnow().isoformat()),
                    "eventSource": "aws:dynamodb",
                    "dynamodb": {
                        "NewImage": {n: TaskTrackingTable.typed_item(new_item[n]) for n in new_item},
                        "OldImage": {o: TaskTrackingTable.typed_item(old_item[o]) for o in old_item}
                    }
                }]
        }
        main.lambda_handler(event, None)


