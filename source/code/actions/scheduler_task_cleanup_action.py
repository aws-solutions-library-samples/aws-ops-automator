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

from boto3.dynamodb.conditions import Attr

from actions import *
from actions.action_base import ActionBase
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from outputs import raise_exception

PARAM_DESC_RETAIN_FAILED_TASKS = "Set to Yes to keep entries for failed tasks"
PARAM_DESC_TASK_RETENTION_HOURS = "Number of hours to keep completed entries before they are deleted from tracking table"

PARAM_LABEL_RETAIN_FAILED_TASKS = "Keep failed tasks"
PARAM_LABEL_TASK_RETENTION_HOURS = "Hours to keep entries"

PARAM_RETAIN_FAILED_TASKS = "RetainFailedTasks"
PARAM_TASK_RETENTION_HOURS = "TaskRetentionHours"
PARAM_TASK_TABLE = "TaskTable"

WARNING_DELETE_CAPACITY = "There are unprocessed items when cleaning up task items, consider raining capacity of table {}"
ERR_MISSING_ENVIRONMENT_VARIABLE_ = "Task tracking table not defined in environment variable {}"
INF_DELETE = "Deleting tasks older than {}"


class SchedulerTaskCleanupAction(ActionBase):
    properties = {

        ACTION_TITLE: "Scheduler Task Cleanup",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes old entries from task tracking table",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "6f0ac9ab-b0ea-4922-b674-253499dee6a2",

        ACTION_SERVICE: "time",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_CROSS_ACCOUNT: False,
        ACTION_INTERNAL: True,
        ACTION_MULTI_REGION: False,

        ACTION_EXECUTE_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_PARAMETERS: {

            PARAM_TASK_RETENTION_HOURS: {
                PARAM_LABEL: PARAM_LABEL_TASK_RETENTION_HOURS,
                PARAM_DESCRIPTION: PARAM_DESC_TASK_RETENTION_HOURS,
                PARAM_TYPE: type(int()),
                PARAM_MIN_VALUE: 1,
                PARAM_REQUIRED: True
            },
            PARAM_RETAIN_FAILED_TASKS: {
                PARAM_LABEL: PARAM_LABEL_RETAIN_FAILED_TASKS,
                PARAM_DESCRIPTION: PARAM_DESC_RETAIN_FAILED_TASKS,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: True
            }
        },
        ACTION_PERMISSIONS: [
            "dynamodb:Scan",
            "dynamodb:BatchWriteItem"
        ]
    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.task_table = os.getenv(handlers.ENV_ACTION_TRACKING_TABLE, None)
        if self.task_table is None:
            raise_exception(ERR_MISSING_ENVIRONMENT_VARIABLE_, handlers.ENV_ACTION_TRACKING_TABLE)

        # adding 48 hours as TTL is used in V2 as primary mechanism to delete items
        self.task_retention_seconds = (int(self.get(PARAM_TASK_RETENTION_HOURS)) + 48) * 3600
        self.retain_failed_tasks = self.get(PARAM_RETAIN_FAILED_TASKS, True)
        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)
        self.debug = self.get(ACTION_PARAM_DEBUG, False)

        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = get_client_with_retries("dynamodb",
                                                   methods=[
                                                       "batch_write_item"
                                                   ],
                                                   context=self._context_,
                                                   session=self._session_,
                                                   logger=self._logger_)
        return self._client

    def execute(self):

        self._logger_.info("{}, version {}", str(self.__class__).split(".")[-2], self.properties[ACTION_VERSION])
        self._logger_.debug("Implementation {}", __name__)

        self._logger_.info("Cleanup table {}", self.task_table)

        scanned_count = 0
        deleted_count = 0

        # calculate moment from when entries can be deleted
        dt = (self._datetime_.utcnow() - datetime(1970, 1, 1)).total_seconds()
        delete_before = int(dt - self.task_retention_seconds)
        self._logger_.info(INF_DELETE, datetime.fromtimestamp(delete_before).isoformat())

        #  status of deleted items for scan expression
        delete_status = [handlers.STATUS_COMPLETED]

        if not self.retain_failed_tasks:
            delete_status.append(handlers.STATUS_FAILED)
            delete_status.append(handlers.STATUS_TIMED_OUT)

        table = self._session_.resource("dynamodb").Table(self.task_table)
        add_retry_methods_to_resource(table, ["scan"], context=self._context_)

        args = {
            "Select": "ALL_ATTRIBUTES",
            "FilterExpression": (Attr(handlers.TASK_TR_STATUS).is_in(delete_status))
        }

        self._logger_.debug("table.scan arguments {}", args)

        # scan for items to delete
        while True:

            if self.time_out():
                break

            resp = table.scan_with_retries(**args)

            self._logger_.debug("table.scan result {}", resp)

            scanned_count += resp["ScannedCount"]

            to_delete = [i[handlers.TASK_TR_ID] for i in resp.get("Items", []) if
                         int(i[handlers.TASK_TR_CREATED_TS]) < delete_before]

            deleted_count += self.delete_tasks(to_delete)

            if "LastEvaluatedKey" in resp:
                args["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        return {
            "items-scanned": scanned_count,
            "items-deleted": deleted_count
        }

    def delete_tasks(self, items_to_delete):

        deleted = 0

        if not self.dryrun and len(items_to_delete) > 0:

            delete_requests = []

            # delete items in batches of max 25 items
            while len(items_to_delete) > 0:

                if self.time_out():
                    break

                delete_requests.append({
                    'DeleteRequest': {
                        'Key': {
                            handlers.TASK_TR_ID: {
                                "S": items_to_delete.pop(0)
                            }
                        }
                    }
                })

                if len(items_to_delete) == 0 or len(delete_requests) == 25:
                    self._logger_.debug("batch_write request items {}", delete_requests)
                    resp = self.client.batch_write_item_with_retries(RequestItems={self.task_table: delete_requests})

                    self._logger_.debug("batch_write response {}", resp)
                    deleted += len(delete_requests)

        return deleted
