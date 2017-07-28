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

from datetime import datetime, timedelta
from botocore.exceptions import ClientError

import dateutil.parser

import pytz
import services.ec2_service
from actions import *
from boto_retry import get_client_with_retries
from util import safe_json


MAX_SNAPSHOTS = 1000

GROUP_TITLE_DELETE_OPTIONS = "Snapshot delete options"

PARAM_DESC_RETENTION_COUNT = "Number of snapshots to keep for a volume, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Snapshot retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

INFO_ACCOUNT_SNAPSHOTS = "{} snapshots for account {}"
INFO_KEEP_RETENTION_COUNT = "Retaining latest {} snapshots for each Ec2 volume"
INFO_REGION = "Processing snapshots in region {}"
INFO_RETENTION_DAYS = "Deleting Ec2 snapshots older than {}"
INFO_SN_DELETE_RETENTION_COUNT = "Deleting snapshot {}, because count for its volume is {}"
INFO_SN_RETENTION_DAYS = "Deleting snapshot {} ({}) because it is older than retention period of {} days"
INFO_SNAPSHOT_DELETED = "Deleted snapshot {} for volume {}"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"
ERR_MAX_RETENTION_COUNT_SNAPSHOTS = "Can not delete if number of snapshots is larger than {} for volume {}"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class Ec2DeleteSnapshotAction:
    properties = {
        ACTION_TITLE: "EC2 Delete Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPION: "Deletes EC2 snapshots after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "8034f6b8-de65-4b8a-ad31-8ac8d13a532f",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_ACCOUNT,
        ACTION_MEMORY: 128,

        ACTION_ALLOW_TAGFILTER_WILDCARD : False,

        ACTION_SELECT_EXPRESSION: "Snapshots[?State=='completed'].{SnapshotId:SnapshotId, VolumeId:VolumeId, StartTime:StartTime,"
                                  "Tags:Tags}",
        ACTION_KEEP_RESOURCE_TAGS: False,

        ACTION_SELECT_PARAMETERS: {'OwnerIds': ["self"]},

        ACTION_PARAMETERS: {
            PARAM_RETENTION_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_DAYS,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_DAYS
            },
            PARAM_RETENTION_COUNT: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_COUNT,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_COUNT
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_DELETE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_RETENTION_DAYS,
                    PARAM_RETENTION_COUNT

                ],
            }],

        ACTION_PERMISSIONS: ["ec2:DescribeSnapshots",
                             "ec2:DeleteSnapshot"]

    }

    @staticmethod
    def can_execute(resources, _):
        if len(resources) > MAX_SNAPSHOTS:
            volume_id = resources[0]["VolumeId"]
            raise ValueError(ERR_MAX_RETENTION_COUNT_SNAPSHOTS.format( MAX_SNAPSHOTS, volume_id))

    @staticmethod
    def custom_aggregation(resources, _):
        snapshots_sorted_by_volumeid = sorted(resources, key=lambda k: k['VolumeId'])
        volume = snapshots_sorted_by_volumeid[0]["VolumeId"] if len(snapshots_sorted_by_volumeid) > 0 else None
        snapshots_for_volume = []
        for snapshot in snapshots_sorted_by_volumeid:
            if volume != snapshot["VolumeId"]:
                yield snapshots_for_volume
                volume = snapshot["VolumeId"]
                snapshots_for_volume = [snapshot]
            else:
                snapshots_for_volume.append(snapshot)
        yield snapshots_for_volume


    @staticmethod
    def action_validate_parameters(parameters):

        retention_days = parameters.get(PARAM_RETENTION_DAYS)
        retention_count = parameters.get(PARAM_RETENTION_COUNT)
        if not retention_count and not retention_days:
            raise ValueError(ERR_RETENTION_PARAM_NONE.format(PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS))

        if retention_days and retention_count:
            raise ValueError(ERR_RETENTION_PARAM_BOTH.format(PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS))

        return parameters

    def __init__(self, arguments):

        self._arguments = arguments
        self.context = self._arguments[ACTION_PARAM_CONTEXT]
        self.logger = self._arguments[ACTION_PARAM_LOGGER]

        self.task = self._arguments[ACTION_PARAM_TASK]
        self.snapshots = sorted(self._arguments[ACTION_PARAM_RESOURCES])
        self.retention_days = self._arguments.get(PARAM_RETENTION_DAYS)
        self.retention_count = self._arguments.get(PARAM_RETENTION_COUNT)

        self.dryrun = self._arguments.get(ACTION_PARAM_DRYRUN, False)
        self.session = self._arguments[ACTION_PARAM_SESSION]
        self.account = self.snapshots[0]["AwsAccount"]

        self.result = {
            "account": self.account,
            "task": self.task
        }

    def execute(self, _):

        def snapshots_to_delete():

            def by_retention_days():

                delete_before_dt = datetime.utcnow().replace(tzinfo=pytz.timezone("UTC")) - timedelta(days=int(self.retention_days))
                self.logger.info(INFO_RETENTION_DAYS, delete_before_dt)

                for sn in sorted(self.snapshots, key=lambda s: s["Region"]):
                    snapshot_dt = dateutil.parser.parse(sn["StartTime"])
                    if snapshot_dt < delete_before_dt:
                        self.logger.info(INFO_SN_RETENTION_DAYS, sn["SnapshotId"], sn["StartTime"], self.retention_days)
                        yield sn

            def by_retention_count():

                self.logger.info(INFO_KEEP_RETENTION_COUNT, self.retention_count)
                sorted_snapshots = sorted(self.snapshots,
                                          key=lambda s: (s["VolumeId"], dateutil.parser.parse(s["StartTime"])),
                                          reverse=True)
                volume = None
                count_for_volume = 0
                for sn in sorted_snapshots:
                    if sn["VolumeId"] != volume:
                        volume = sn["VolumeId"]
                        count_for_volume = 0

                    count_for_volume += 1
                    if count_for_volume > self.retention_count:
                        self.logger.info(INFO_SN_DELETE_RETENTION_COUNT, sn["SnapshotId"], count_for_volume)
                        yield sn

            return by_retention_days() if self.retention_days else by_retention_count()

        self.logger.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        region = None
        ec2 = None
        deleted_count = 0

        self.logger.info(INFO_ACCOUNT_SNAPSHOTS, len(self.snapshots), self.account)

        self.logger.debug("Snapshots : {}", self.snapshots)

        snapshot_id = ""
        for snapshot in snapshots_to_delete():

            if snapshot["Region"] != region:
                region = snapshot["Region"]
                self.logger.info(INFO_REGION, region)
                ec2 = get_client_with_retries("ec2", ["delete_snapshot"], region=region, context=self.context, session=self.session)
                if "deleted" not in self.result:
                    self.result["deleted"] = {}
                self.result["deleted"][region] = []

            try:
                snapshot_id = snapshot["SnapshotId"]
                ec2.delete_snapshot_with_retries(DryRun=self.dryrun, SnapshotId=snapshot_id)
                deleted_count += 1
                self.logger.info(INFO_SNAPSHOT_DELETED, snapshot_id, snapshot["VolumeId"])
                self.result["deleted"][region].append(snapshot_id)
            except ClientError as ex_client:
                if ex_client.response.get("Error", {}).get("Code", "") == "InvalidSnapshot.NotFound":
                    self.logger.info("Snapshot \"{}\" was not found and could not be deleted", snapshot_id)
                else:
                    raise ex_client
            except Exception as ex:
                if self.dryrun:
                    self.logger.debug(str(ex))
                    self.result["delete_snapshot"] = str(ex)
                    return self.result
                else:
                    raise ex

        self.result.update({
            "snapshots": len(self.snapshots),
            "snapshots-deleted": deleted_count,
            METRICS_DATA : build_action_metrics(self, DeletedSnapshots=deleted_count)

        })

        return safe_json(self.result)

