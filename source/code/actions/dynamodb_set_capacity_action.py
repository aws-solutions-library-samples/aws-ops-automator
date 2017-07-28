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

import actions
import handlers.task_tracking_table as tracking
from actions import *
from boto_retry import get_client_with_retries
from util import safe_json

INFO_INDEX_UPDATE = "Index {} throughput will be updated, current read/write {}/{}, new throughput will be {}/{}"
INFO_TABLE_UPDATE = "Table {} throughput will be updated, current read/write {}/{}, new throughput will be {}/{}"

WARN_GSI_DOES_NOT_EXIST = "Global Secondary Index {} does not exist for table {}"

PARAM_GROUP_GSI = "Global Secondary Index {}"

PARAM_DESC_GSI_NAME = "Name of secondary global index (leave blank or set to None if not used)"
PARAM_DESC_READ_UNITS = "Read units for table"
PARAM_DESC_TABLE_NAME = "Name of the DynamoDB table"
PARAM_DESC_WRITE_UNITS = "Write units for table"

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


class DynamodbSetCapacityAction:
    properties = {

        ACTION_TITLE: "DynamoDB set capacity",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPION: "Sets the read and write capacity for a DynamoDB table and it's global secondary indexes",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "66010073-a4fb-414a-87d9-2b33f6a20108",

        ACTION_SERVICE: "time",
        ACTION_RESOURCES: "",
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_MEMORY: 128,
        ACTION_CROSS_ACCOUNT: True,
        ACTION_INTERNAL: False,
        ACTION_MULTI_REGION: True,

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
                PARAM_DESCRIPTION: PARAM_DESC_READ_UNITS,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_REQUIRED: True
            },
            PARAM_TABLE_WRITE_UNITS: {
                PARAM_LABEL: PARAM_LABEL_GSI_WRITE_UNITS,
                PARAM_DESCRIPTION: PARAM_DESC_WRITE_UNITS,
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

    def __init__(self, arguments):
        self.arguments = arguments
        self.logger = self.arguments[actions.ACTION_PARAM_LOGGER]
        self.context = self.arguments[actions.ACTION_PARAM_CONTEXT]
        self.session = self.arguments[actions.ACTION_PARAM_SESSION]
        self.session = self.arguments[actions.ACTION_PARAM_SESSION]
        self.dryrun = self.arguments.get(actions.ACTION_PARAM_DRYRUN, False)
        self.debug = self.arguments.get(actions.ACTION_PARAM_DEBUG, False)

        self.account = self.arguments[actions.ACTION_PARAM_RESOURCES]["AwsAccount"]
        self.region = self.arguments[actions.ACTION_PARAM_RESOURCES]["Region"]
        self.tablename = self.arguments[PARAM_TABLE_NAME]

        self.read_units_update = self.arguments[PARAM_TABLE_READ_UNITS]
        self.write_units_update = self.arguments[PARAM_TABLE_WRITE_UNITS]

        self.client = get_client_with_retries("dynamodb", ["describe_table", "update_table"], context=self.context,
                                              region=self.region, session=self.session)
        self.result = {}

    @staticmethod
    def get_table_resource(item):
        tablename = item[tracking.TASK_TR_PARAMETERS][PARAM_TABLE_NAME]
        region = item[tracking.TASK_TR_RESOURCES]["Region"]
        account = item[tracking.TASK_TR_PARAMETERS]["AwsAccount"]
        return account, region, tablename

    @staticmethod
    def action_concurrency_key(arguments):
        """
        Returns key for concurrency control of the scheduler. As the CopySnapshot API call only allows 5 concurrent copies
        per account to a destination region this method returns a key containing the name of the api call, the account and
        the destination account.
        :param arguments: Task arguments
        :return: Concurrency key
        """
        tablename = arguments[PARAM_TABLE_NAME]
        region = arguments[ACTION_PARAM_RESOURCES]["Region"]
        account = arguments[ACTION_PARAM_RESOURCES]["AwsAccount"]
        return "ec2:UpdateTable:{}:{}:{}".format(account, region, tablename)

    def is_completed(self, _, start_result):
        """
        Tests if the copy snapshot action has been completed. This method uses the id of the copied snapshot and test if it
        does exist and is complete in the destination region. As long as this is not the case the method must return None
        :param _: not used
        :param start_result: output of initial execution
        :return:  Result of copy action, None of not completed yet
        """

        start_data = json.loads(start_result)
        if "current" in start_data:
            return start_data["current"]

        resp = self.client.describe_table_with_retries(TableName=self.tablename)
        if resp.get("Table", {}).get("TableStatus", "") == "ACTIVE":
            return safe_json(resp["Table"])
        else:
            return None

    def _get_throughput_update(self, resp):

        def get_gsi_throughput_updates():
            result = {}
            for i in range(1, 6):
                gsi_name = self.arguments[PARAM_GSI_NAME.format(i)]
                if gsi_name in ["", None, "None"]:
                    continue
                result[gsi_name] = self.arguments[PARAM_GSI_READ_UNITS.format(i)], self.arguments[PARAM_GSI_WRITE_UNITS.format(i)]
            return result

        update_args = {}
        table_provisioned_throughput = resp["Table"]["ProvisionedThroughput"]
        current_read_units = table_provisioned_throughput["ReadCapacityUnits"]
        current_write_units = table_provisioned_throughput["WriteCapacityUnits"]

        if current_read_units != self.read_units_update or current_write_units != self.write_units_update:

            update_args["ProvisionedThroughput"] = {
                "ReadCapacityUnits": self.read_units_update,
                "WriteCapacityUnits": self.write_units_update
            }

            self.logger.info(INFO_TABLE_UPDATE, self.tablename, current_read_units, current_write_units, self.read_units_update,
                             self.write_units_update)

        global_secundary_indexes = {g["IndexName"]: g for g in resp["Table"].get("GlobalSecondaryIndexes", [])}

        metrics_gsi_read_current = 0
        metrics_gsi_write_current = 0
        metrics_gsi_read_new = 0
        metrics_gsi_write_new = 0

        gsi_updates = get_gsi_throughput_updates()
        for index_name in gsi_updates:
            if index_name in global_secundary_indexes:
                current_gsi_read_units = global_secundary_indexes[index_name]["ProvisionedThroughput"]["ReadCapacityUnits"]
                current_gsi_write_units = global_secundary_indexes[index_name]["ProvisionedThroughput"]["WriteCapacityUnits"]
                update_read_units = gsi_updates[index_name][0]
                update_write_units = gsi_updates[index_name][1]
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

                    self.logger.info(INFO_INDEX_UPDATE, index_name, current_read_units, current_write_units, update_read_units,
                                     update_write_units)

            else:
                self.logger.warning(WARN_GSI_DOES_NOT_EXIST, index_name, self.tablename)

        self.result[actions.METRICS_DATA] = build_action_metrics(
            action=self,
            OldReadUnits=current_read_units,
            OldWriteUnits=current_write_units,
            NewReadUnits=self.read_units_update,
            NewWriteUnits=self.write_units_update,
            OldGsiReadUnits=metrics_gsi_read_current,
            OldGsiWriteUnits=metrics_gsi_write_current,
            NewGsiReadUnits=metrics_gsi_read_new,
            NewGsiWriteUnits=metrics_gsi_write_new)

        return update_args

    def execute(self, _):

        self.logger.info("{}, version {}", str(self.__class__).split(".")[-1], self.properties[ACTION_VERSION])
        self.logger.debug("Implementation {}", __name__)

        get_resp = self.client.describe_table_with_retries(TableName=self.tablename)
        update_args = self._get_throughput_update(get_resp)
        self.result["current"] = get_resp
        if len(update_args) > 0:
            update_args["TableName"] = self.tablename
            self.logger.info("Updating throughput for table and indexes with arguments {}", json.dumps(update_args, indent=2))
            update_resp = self.client.update_table_with_retries(**update_args)
            self.result["update"] = update_resp
        else:
            self.logger.info("Throughput for table {} and indexes already at requested capacity", self.tablename)

        return safe_json(self.result)

