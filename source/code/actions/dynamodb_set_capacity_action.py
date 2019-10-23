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
import json

import actions
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries

INF_UPDATE_TABLE = "Updating throughput for table and indexes with arguments {}"
INF_INDEX_UPDATE = "Index {} throughput will be updated, current read/write {}/{}, new throughput will be {}/{}"
INF_TABLE_UPDATE = "Table {} throughput will be updated, current read/write {}/{}, new throughput will be {}/{}"

WARN_GSI_DOES_NOT_EXIST = "Global Secondary Index {} does not exist for table {}"

PARAM_GROUP_GSI = "Global Secondary Index {}"

PARAM_DESC_GSI_NAME = "Name of secondary global index (leave blank or set to None if not used)"
PARAM_DESC_READ_UNITS = "Provisioned read units for the table"
PARAM_DESC_INDEX_READ_UNITS = "Provisioned read units for the index"
PARAM_DESC_TABLE_NAME = "Name of the DynamoDB table"
PARAM_DESC_WRITE_UNITS = "Provisioned write units for the table"
PARAM_DESC_INDEX_WRITE_UNITS = "Provisioned write units for the index"

PARAM_GSI_NAME = "GlobalSecondaryIndexName{}"
PARAM_GSI_READ_UNITS = "GlobalSecondaryIndexRead{}"
PARAM_GSI_WRITE_UNITS = "GlobalSecondaryIndexWrite{}"

PARAM_LABEL_GSI_NAME = "Index name"
PARAM_LABEL_GSI_READ_UNITS = "Table read units"
PARAM_LABEL_TABLE_NAME = "Table name"
PARAM_LABEL_GSI_WRITE_UNITS = "Table write units"

PARAM_TABLE_NAME = "TableName"
PARAM_TABLE_READ_UNITS = "TableReadUnits"
PARAM_TABLE_WRITE_UNITS = "TableWriteUnits"


class DynamodbSetCapacityAction(ActionBase):
    properties = {

        ACTION_TITLE: "DynamoDB Set Capacity",
        ACTION_VERSION: "1.2",
        ACTION_DESCRIPTION: "Sets the read and write capacity for a DynamoDB table and it's global secondary indexes",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "66010073-a4fb-414a-87d9-2b33f6a20108",

        ACTION_SERVICE: "time",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_CROSS_ACCOUNT: True,
        ACTION_INTERNAL: False,
        ACTION_MULTI_REGION: True,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_PARAMETERS: {

            PARAM_TABLE_NAME: {
                PARAM_LABEL: PARAM_LABEL_TABLE_NAME,
                PARAM_DESCRIPTION: PARAM_DESC_TABLE_NAME,
                PARAM_TYPE: type(""),
                PARAM_MIN_LEN: 3,
                PARAM_MAX_LEN: 255,
                PARAM_PATTERN: "^([.A-Za-z0-9_-]{3,255})$",
                PARAM_REQUIRED: True
            },
            PARAM_TABLE_READ_UNITS: {
                PARAM_LABEL: PARAM_LABEL_GSI_READ_UNITS,
                PARAM_DESCRIPTION: PARAM_DESC_INDEX_READ_UNITS,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_REQUIRED: True
            },
            PARAM_TABLE_WRITE_UNITS: {
                PARAM_LABEL: PARAM_LABEL_GSI_WRITE_UNITS,
                PARAM_DESCRIPTION: PARAM_DESC_INDEX_WRITE_UNITS,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_REQUIRED: True
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: "DynamoDB table capacity",
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_TABLE_NAME,
                    PARAM_TABLE_READ_UNITS,
                    PARAM_TABLE_WRITE_UNITS
                ],
            }
        ],

        ACTION_MAX_CONCURRENCY: 1,
        ACTION_PERMISSIONS: ["dynamodb:DescribeTable", "dynamodb:UpdateTable"]
    }

    for i in range(1, 6):
        properties[ACTION_PARAMETERS][PARAM_GSI_NAME.format(i)] = {
            PARAM_LABEL: PARAM_LABEL_GSI_NAME,
            PARAM_DESCRIPTION: PARAM_DESC_GSI_NAME,
            PARAM_TYPE: str,
            PARAM_DEFAULT: "None",
            PARAM_REQUIRED: False
        }

        properties[ACTION_PARAMETERS][PARAM_GSI_READ_UNITS.format(i)] = {
            PARAM_LABEL: PARAM_LABEL_GSI_READ_UNITS,
            PARAM_DESCRIPTION: PARAM_DESC_READ_UNITS,
            PARAM_TYPE: int,
            PARAM_DEFAULT: 1,
            PARAM_MIN_VALUE: 1,
            PARAM_REQUIRED: False
        }

        properties[ACTION_PARAMETERS][PARAM_GSI_WRITE_UNITS.format(i)] = {
            PARAM_LABEL: PARAM_LABEL_GSI_WRITE_UNITS,
            PARAM_DESCRIPTION: PARAM_DESC_WRITE_UNITS,
            PARAM_TYPE: int,
            PARAM_DEFAULT: 1,
            PARAM_MIN_VALUE: 1,
            PARAM_REQUIRED: False
        }

        properties[ACTION_PARAMETER_GROUPS].append({
            ACTION_PARAMETER_GROUP_TITLE: PARAM_GROUP_GSI.format(i),
            ACTION_PARAMETER_GROUP_LIST: [
                PARAM_GSI_NAME.format(i),
                PARAM_GSI_READ_UNITS.format(i),
                PARAM_GSI_WRITE_UNITS.format(i)
            ],
        }, )

    @staticmethod
    def action_logging_subject(arguments, parameters):
        account = arguments[actions.ACTION_PARAM_RESOURCES]["AwsAccount"]
        region = arguments[actions.ACTION_PARAM_RESOURCES]["Region"]
        table = parameters[PARAM_TABLE_NAME]
        return "{}-{}-{}-{}".format(account, region, table, log_stream_date())

    @staticmethod
    def action_concurrency_key(arguments):

        tablename = arguments[PARAM_TABLE_NAME]
        region = arguments[ACTION_PARAM_RESOURCES]["Region"]
        account = arguments[ACTION_PARAM_RESOURCES]["AwsAccount"]
        return "ec2:UpdateTable:{}:{}:{}".format(account, region, tablename)

    def _get_throughput_update(self, resp):

        def get_gsi_throughput_updates():
            result = {}
            for i in range(1, 6):
                gsi_name = self.get(PARAM_GSI_NAME.format(i), None)
                if gsi_name in ["", None, "None"]:
                    continue
                result[gsi_name] = self.get(PARAM_GSI_READ_UNITS.format(i)), self.get(PARAM_GSI_WRITE_UNITS.format(i))
            return result

        update_args = {}

        table_provisioned_throughput = resp["Table"]["ProvisionedThroughput"]
        actual_read_units = int(table_provisioned_throughput["ReadCapacityUnits"])
        actual_write_units = int(table_provisioned_throughput["WriteCapacityUnits"])

        expected_read_units = int(self._table_read_units_)
        expected_write_units = int(self._table_write_units_)

        if actual_read_units != expected_read_units or actual_write_units != expected_write_units:
            update_args["ProvisionedThroughput"] = {
                "ReadCapacityUnits": expected_read_units,
                "WriteCapacityUnits": expected_write_units
            }

            self._logger_.info(INF_TABLE_UPDATE, self._table_name_, actual_read_units, actual_write_units,
                               expected_read_units,
                               expected_write_units)

        global_secondary_indexes = {g["IndexName"]: g for g in resp["Table"].get("GlobalSecondaryIndexes", [])}

        metrics_gsi_read_current = 0
        metrics_gsi_write_current = 0
        metrics_gsi_read_new = 0
        metrics_gsi_write_new = 0

        gsi_updates = get_gsi_throughput_updates()
        for index_name in gsi_updates:
            if index_name in global_secondary_indexes:

                index_throughput = global_secondary_indexes[index_name]["ProvisionedThroughput"]
                current_gsi_read_units = int(index_throughput["ReadCapacityUnits"])
                current_gsi_write_units = int(index_throughput["WriteCapacityUnits"])

                update_read_units = int(gsi_updates[index_name][0])
                update_write_units = int(gsi_updates[index_name][1])

                metrics_gsi_read_current += current_gsi_read_units
                metrics_gsi_write_current += current_gsi_write_units

                metrics_gsi_read_new += update_read_units
                metrics_gsi_write_new += update_write_units

                if (update_read_units != current_gsi_read_units) or (update_write_units != current_gsi_write_units):

                    if "GlobalSecondaryIndexUpdates" not in update_args:
                        update_args["GlobalSecondaryIndexUpdates"] = []

                    update_args["GlobalSecondaryIndexUpdates"].append({
                        "Update": {
                            "IndexName": index_name,
                            "ProvisionedThroughput": {
                                "ReadCapacityUnits": update_read_units,
                                "WriteCapacityUnits": update_write_units
                            }
                        }
                    })

                    self._logger_.info(INF_INDEX_UPDATE, index_name, current_gsi_read_units, current_gsi_write_units, update_read_units,
                                       update_write_units)

            else:
                self._logger_.warning(WARN_GSI_DOES_NOT_EXIST, index_name, self._table_name_)

        self.result[actions.METRICS_DATA] = build_action_metrics(
            action=self,
            OldReadUnits=actual_read_units,
            OldWriteUnits=actual_write_units,
            NewReadUnits=expected_read_units,
            NewWriteUnits=expected_write_units,
            OldGsiReadUnits=metrics_gsi_read_current,
            OldGsiWriteUnits=metrics_gsi_write_current,
            NewGsiReadUnits=metrics_gsi_read_new,
            NewGsiWriteUnits=metrics_gsi_write_new)

        return update_args

    def __init__(self, action_arguments, action_parameters):

        self._table_read_units_ = None
        self._table_write_units_ = None
        self._table_name_ = None

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.client = get_client_with_retries("dynamodb", ["describe_table", "update_table"], context=self._context_,
                                              region=self._region_, session=self._session_, logger=self._logger_)
        self.result = {}

    def is_completed(self, start_data):

        if "update" not in start_data:
            return start_data["current"]

        resp = self.client.describe_table_with_retries(TableName=self._table_name_)
        table = resp.get("Table", {})
        if table.get("TableStatus", "") != "ACTIVE":
            return None
        for i in table.get("GlobalSecondaryIndexes", []):
            if i.get("IndexStatus") != "ACTIVE":
                return None

        return resp["Table"]

    def execute(self):

        self._logger_.info("{}, version {}", str(self.__class__).split(".")[-1], self.properties[ACTION_VERSION])
        self._logger_.debug("Implementation {}", __name__)

        get_resp = self.client.describe_table_with_retries(TableName=self._table_name_)
        update_args = self._get_throughput_update(get_resp)
        self.result["current"] = get_resp["Table"]
        if len(update_args) > 0:
            update_args["TableName"] = self._table_name_
            self._logger_.info(INF_UPDATE_TABLE, json.dumps(update_args, indent=2))
            update_resp = self.client.update_table_with_retries(**update_args)
            self.result["update"] = update_resp["TableDescription"]
        else:

            self._logger_.info("Throughput for table {} and indexes already at requested capacity", self._table_name_)

        return self.result
