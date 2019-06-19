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

import decimal
import json
from time import time

from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

import actions
from actions import *
from actions.action_base import ActionBase
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from outputs import raise_exception

ERR_NO_ENVIRONMENT_VARIABLE_ = "Task tracking table not defined in environment variable {}"

INFO_EXPORTING_FROM = "Exporting from table {}"
INFO_EXPORTING_AFTER = "Exporting tasks after last export time: {}"
INFO_NO_TIME_FOUND = "No last export time found"

EXPORT_OBJECT_KEY_TEMPLATE = "{}{}/{}/{}/tasks-{}-{}.json"

PARAM_DESC_S3_BUCKET = "Name of Amazon S3 bucket to store exported files"
PARAM_DESC_S3_PREFIX = "Amazon S3 object prefix for exported files"

PARAM_LABEL_S3_BUCKET = "S3 bucket"
PARAM_LABEL_S3_PREFIX = "S3 object prefix"

PARAM_S3_BUCKET = "S3Bucket"
PARAM_S3_PREFIX = "S3Prefix"

LAST_EXPORT_KEY = "last_export_data"


class SchedulerTaskExportAction(ActionBase):
    properties = {

        ACTION_TITLE: "Scheduler Task export to Amazon S3",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Periodically exports task entries from task tracking table into Amazon S3 for further analytics",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "eb40fe14-549c-4108-92bc-7adba81a0349",

        ACTION_SERVICE: "time",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_CROSS_ACCOUNT: False,
        ACTION_INTERNAL: True,
        ACTION_MULTI_REGION: False,

        ACTION_EXECUTE_SIZE: ACTION_SIZE_ALL_WITH_ECS,

        ACTION_PARAMETERS: {

            PARAM_S3_BUCKET: {
                PARAM_LABEL: PARAM_LABEL_S3_BUCKET,
                PARAM_DESCRIPTION: PARAM_DESC_S3_BUCKET,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True,

            },
            PARAM_S3_PREFIX: {
                PARAM_LABEL: PARAM_LABEL_S3_PREFIX,
                PARAM_DESCRIPTION: PARAM_DESC_S3_PREFIX,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True,
                PARAM_MIN_LEN: 1

            }
        },

        ACTION_PERMISSIONS: [
            "dynamodb:Scan",
            "s3:GetObject",
            "s3:PutObject"
        ]
    }

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.task_table = os.getenv(handlers.ENV_ACTION_TRACKING_TABLE, None)
        if self.task_table is None:
            raise_exception(ERR_NO_ENVIRONMENT_VARIABLE_, handlers.ENV_ACTION_TRACKING_TABLE)

        self.dryrun = self.get(actions.ACTION_PARAM_DRYRUN, False)
        self.debug = self.get(actions.ACTION_PARAM_DEBUG, False)
        self.S3Bucket = self.get(PARAM_S3_BUCKET)
        self.S3Prefix = self.get(PARAM_S3_PREFIX, "")

    def execute(self):

        def decimal_default(obj):
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            raise TypeError

        self._logger_.info("{}, version {}", str(self.__class__).split(".")[-1], self.properties[ACTION_VERSION])
        self._logger_.debug("Implementation {}", __name__)

        self._logger_.info(INFO_EXPORTING_FROM, self.task_table)

        scanned_count = 0
        items_to_export = []
        last_export_time_ts = 0

        # fetch last_export_time after which when entries can be exported
        try:
            s3_get_client = get_client_with_retries("s3", ["get_object"], context=self._context_,
                                                    session=self._session_, logger=self._logger_)
            resp = s3_get_client.get_object_with_retries(Bucket=self.S3Bucket, Key=self.S3Prefix + LAST_EXPORT_KEY,
                                                         _expected_boto3_exceptions_=["NoSuchKey"])
            last_export_time_ts = int(resp['Body'].read())
            self._logger_.info("last export time response {}", last_export_time_ts)
        except ClientError:
            self._logger_.info(INFO_NO_TIME_FOUND)

        self._logger_.info(INFO_EXPORTING_AFTER, self._datetime_.fromtimestamp(last_export_time_ts).isoformat())

        #  status of to be exported items for scan expression
        export_status = [handlers.STATUS_COMPLETED, handlers.STATUS_FAILED, handlers.STATUS_TIMED_OUT]
        table = self._session_.resource("dynamodb").Table(self.task_table)
        add_retry_methods_to_resource(table, ["scan"], context=self._context_)

        # noinspection PyPep8
        args = {
            "Select": "ALL_ATTRIBUTES",
            "FilterExpression": (Attr(handlers.TASK_TR_CREATED_TS).gt(last_export_time_ts)
                                 .__or__(Attr(handlers.TASK_TR_UPDATED_TS).gt(last_export_time_ts)))
                .__and__(Attr(handlers.TASK_TR_STATUS).is_in(export_status))
                .__and__(Attr(handlers.TASK_TR_INTERNAL).eq(False))
        }
        self._logger_.debug("table.scan arguments {}", args)

        # scan for items to export
        while True:

            if self.time_out():
                break

            resp = table.scan_with_retries(**args)

            self._logger_.debug("table.scan result {}", resp)

            items_to_export += resp.get("Items", [])[:]
            scanned_count += resp["ScannedCount"]

            if "LastEvaluatedKey" in resp:
                args["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
            else:
                break

        # set new export time
        export_str = ''
        last_export_time_ts = int(time())
        s3_put_client = get_client_with_retries("s3",
                                                methods=[
                                                    "put_object"
                                                ],
                                                context=self._context_,
                                                session=self._session_,
                                                logger=self._logger_)

        # not a dryrun and any items to export were found
        if not self.dryrun and len(items_to_export) > 0:
            if len(items_to_export) > 0:
                self._logger_.debug("put_object items {}", items_to_export)
                for item in items_to_export:
                    export_str += json.dumps(dict(item), default=decimal_default, separators=(',', ':')) + '\n'
                last_export_time_dt = self._datetime_.fromtimestamp(last_export_time_ts)
                export_object_key = EXPORT_OBJECT_KEY_TEMPLATE.format(self.S3Prefix, last_export_time_dt.year,
                                                                      last_export_time_dt.month, last_export_time_dt.day,
                                                                      last_export_time_dt.hour,
                                                                      last_export_time_dt.minute)
                resp = s3_put_client.put_object_with_retries(Bucket=self.S3Bucket, Key=export_object_key, Body=export_str)
                self._logger_.debug("put_object response {}", resp)

        # export new last_export_time to S3
        self._logger_.debug("put_object response {}", str(last_export_time_ts))
        resp = s3_put_client.put_object_with_retries(Bucket=self.S3Bucket,
                                                     Key=self.S3Prefix + LAST_EXPORT_KEY,
                                                     Body=str(last_export_time_ts))
        self._logger_.debug("put_object response {}", resp)

        return {
            "items-scanned": scanned_count,
            "items-exported": len(items_to_export)
        }
