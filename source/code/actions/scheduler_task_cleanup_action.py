import os
from datetime import datetime
from time import time

from boto3.dynamodb.conditions import Attr

import actions
import handlers
import handlers.task_tracking_table as tracking
from actions import *
from  boto_retry import add_retry_methods_to_resource, get_client_with_retries

PARAM_DESC_RETAIN_FAILED_TASKS = "Set to Yes to keep entries for failed tasks"
PARAM_DESC_TASK_RETENTION_HOURS = "Number of hours to keep completed entries before they are deleted from tracking table"

PARAM_LABEL_RETAIN_FAILED_TASKS = "Keep failed tasks"
PARAM_LABEL_TASK_RETENTION_HOURS = "Hours to keep entries"

PARAM_RETAIN_FAILED_TASKS = "RetainFailedTasks"
PARAM_TASK_RETENTION_HOURS = "TaskRetentionHours"
PARAM_TASK_TABLE = "TaskTable"


class SchedulerTaskCleanupAction:
    properties = {

        ACTION_TITLE: "Scheduler Task Cleanup",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPION: "Deletes old entries from task tracking table",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "6f0ac9ab-b0ea-4922-b674-253499dee6a2",

        ACTION_SERVICE: "time",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_MEMORY: 128,
        ACTION_CROSS_ACCOUNT: False,
        ACTION_INTERNAL: True,
        ACTION_MULTI_REGION: False,

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
        ACTION_PERMISSIONS: ["dynamodb:Scan", "dynamodb:BatchWriteItem"]
    }

    def __init__(self, arguments):
        self.logger = arguments[actions.ACTION_PARAM_LOGGER]
        self.context = arguments[actions.ACTION_PARAM_CONTEXT]
        self.session = arguments[actions.ACTION_PARAM_SESSION]
        self.task_table = os.getenv(handlers.ENV_ACTION_TRACKING_TABLE, None)
        if self.task_table is None:
            raise Exception("Task tracking table not defined in environment variable {}".format(handlers.ENV_ACTION_TRACKING_TABLE))
        self.task_retenion_seconds = int(arguments[PARAM_TASK_RETENTION_HOURS] * 3600)
        self.retain_failed_tasks = arguments[PARAM_RETAIN_FAILED_TASKS]
        self.session = arguments[actions.ACTION_PARAM_SESSION]
        self.dryrun = arguments.get(actions.ACTION_PARAM_DRYRUN, False)
        self.debug = arguments.get(actions.ACTION_PARAM_DEBUG, False)

    def execute(self, _):

        self.logger.info("{}, version {}", str(self.__class__).split(".")[-1], self.properties[ACTION_VERSION])
        self.logger.debug("Implementation {}", __name__)

        self.logger.info("Cleanup table {}", self.task_table)

        scanned_count = 0
        items_to_delete = []

        # calculate moment from when entries can be deleted

        delete_before = int(time()) - self.task_retenion_seconds
        self.logger.info("Deleting tasks older than {}", datetime.fromtimestamp(delete_before).isoformat())

        #  status of deleted items for scan expression
        delete_status = [tracking.STATUS_COMPLETED]

        if not self.retain_failed_tasks:
            delete_status.append(tracking.STATUS_FAILED)
            delete_status.append(tracking.STATUS_TIMED_OUT)

        table = self.session.resource("dynamodb").Table(self.task_table)
        add_retry_methods_to_resource(table, ["scan"], context=self.context)

        args = {
            "Select": "SPECIFIC_ATTRIBUTES",
            "ProjectionExpression": tracking.TASK_TR_ID,
            "FilterExpression": Attr(tracking.TASK_TR_CREATED_TS).lt(delete_before).__and__(Attr(tracking.TASK_TR_STATUS).is_in(
                delete_status))
        }

        self.logger.debug("table.scan arguments {}", args)

        # scan for items to delete
        while True:
            resp = table.scan_with_retries(**args)

            self.logger.debug("table.scan result {}", resp)

            items_to_delete += resp.get("Items", [])[:]
            scanned_count += resp["ScannedCount"]

            if "LastEvaluatedKey" in resp:
                args["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        deleted = 0
        client = None

        # not a dryrun and any items to delete were found

        if not self.dryrun and len(items_to_delete) > 0:

            if client is None:
                client = get_client_with_retries("dynamodb", ["batch_write_item"], context=self.context, session=self.session)

            delete_requests = []

            # delete items in batches of max 25 items
            while len(items_to_delete) > 0:
                delete_requests.append({
                    'DeleteRequest': {
                        'Key': {
                            tracking.TASK_TR_ID: {
                                "S": items_to_delete.pop(0)[
                                    tracking.TASK_TR_ID]
                            }
                        }
                    }
                })

                if len(items_to_delete) == 0 or len(delete_requests) == 25:
                    self.logger.debug("batch_write request items {}", delete_requests)
                    resp = client.batch_write_item_with_retries(RequestItems={self.task_table: delete_requests})
                    self.logger.debug("batch_write response {}", resp)
                    deleted += len(delete_requests)
                    delete_requests = []

        return {"items-scanned": scanned_count, "items-deleted": deleted}
