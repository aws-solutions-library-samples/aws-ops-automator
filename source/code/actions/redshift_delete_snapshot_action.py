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

import dateutil.parser

import pytz
import services.redshift_service
from actions import *
from boto_retry import get_client_with_retries

INFO_REVOKE_ACCESS = "Revoking restore access for account {}"

INFO_DELETE_SNAPHOT = "Deleting snapshot {} for cluster {}"

GROUP_TITLE_DELETE_OPTIONS = "Snapshot delete options"

PARAM_DESC_RETENTION_COUNT = "Number of snapshots to keep for a RedShift cluster, use 0 to use retention days"
PARAM_DESC_RETENTION_DAYS = "Snapshot retention period in days, use 0 to use retention count"

PARAM_LABEL_RETENTION_COUNT = "Retention count"
PARAM_LABEL_RETENTION_DAYS = "Retention days"

INFO_ACCOUNT_SNAPSHOTS = "{} cluster snapshots for account {}"
INFO_KEEP_RETENTION_COUNT = "Retaining latest {} snapshots for each Redshift cluster"
INFO_REGION = "Processing snapshots in region {}"
INFO_RETENTION_DAYS = "Deleting snapshots older than {}"
INFO_SN_DELETE_RETENTION_COUNT = "Deleting snapshot {}, because count for its volume is {}"
INFO_SN_RETENTION_DAYS = "Deleting snapshot {} ({}) because it is older than retention period of {} days"
INFO_SNAPSHOT_DELETED = "Deleted snapshot {} for volume {}"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class RedshiftDeleteSnapshotAction:
    properties = {
        ACTION_TITLE: "RedShift Delete Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPION: "Deletes Redshift snapshots after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "2fb2442c-b847-4dab-b53e-e481e029cc30f",

        ACTION_SERVICE: "redshift",
        ACTION_RESOURCES: services.redshift_service.CLUSTER_SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_ACCOUNT,
        ACTION_MEMORY: 128,

        ACTION_SELECT_EXPRESSION: "Snapshots[?Status=='available']|[?SnapshotType=='manual']."
                                  "{SnapshotIdentifier:SnapshotIdentifier, "
                                  "ClusterIdentifier:ClusterIdentifier,"
                                  "SnapshotCreateTime:SnapshotCreateTime,"
                                  "AccountsWithRestoreAccess:AccountsWithRestoreAccess[*].AccountId}",

        ACTION_KEEP_RESOURCE_TAGS: False,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

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

        ACTION_PERMISSIONS: ["redshift:DescribeClusterSnapshots",
                             "redshift:DeleteClusterSnapshot",
                             "redshift:RevokeSnapshotAccess"]

    }

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
                    snapshot_dt = dateutil.parser.parse(sn["SnapshotCreateTime"])
                    if snapshot_dt < delete_before_dt:
                        self.logger.info(INFO_SN_RETENTION_DAYS, sn["SnapshotIdentifier"], sn["SnapshotCreateTime"],
                                         self.retention_days)
                        yield sn

            def by_retention_count():

                self.logger.info(INFO_KEEP_RETENTION_COUNT, self.retention_count)
                sorted_snapshots = sorted(self.snapshots,
                                          key=lambda s: (s["ClusterIdentifier"], dateutil.parser.parse(s["SnapshotCreateTime"])),
                                          reverse=True)
                volume = None
                count_for_volume = 0
                for sn in sorted_snapshots:
                    if sn["ClusterIdentifier"] != volume:
                        volume = sn["ClusterIdentifier"]
                        count_for_volume = 0

                    count_for_volume += 1
                    if count_for_volume > self.retention_count:
                        self.logger.info(INFO_SN_DELETE_RETENTION_COUNT, sn["SnapshotIdentifier"], count_for_volume)
                        yield sn

            return by_retention_days() if self.retention_days else by_retention_count()

        self.logger.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        region = None
        redshift = None
        deleted_count = 0

        self.logger.info(INFO_ACCOUNT_SNAPSHOTS, len(self.snapshots), self.account)

        self.logger.debug("Cluster Snapshots : {}", self.snapshots)

        for snapshot in snapshots_to_delete():

            if snapshot["Region"] != region:
                region = snapshot["Region"]
                self.logger.info(INFO_REGION, region)
                redshift = get_client_with_retries("redshift", ["delete_cluster_snapshot", "revoke_snapshot_access"], region=region,
                                                   context=self.context, session=self.session)
                if "deleted" not in self.result:
                    self.result["deleted"] = {}
                self.result["deleted"][region] = []
            try:
                snapshot_id = snapshot["SnapshotIdentifier"]
                cluster_id = snapshot["ClusterIdentifier"]
                granted_accounts = snapshot.get("AccountsWithRestoreAccess", [])
                if granted_accounts is None:
                    granted_accounts = []

                self.logger.info(INFO_DELETE_SNAPHOT, snapshot_id, cluster_id)
                for account in granted_accounts:
                    self.logger.info(INFO_REVOKE_ACCESS, account)
                    redshift.revoke_snapshot_access_with_retries(SnapshotIdentifier=snapshot_id,
                                                                 SnapshotClusterIdentifier=cluster_id,
                                                                 AccountWithRestoreAccess=account)

                redshift.delete_cluster_snapshot_with_retries(SnapshotIdentifier=snapshot_id,
                                                              SnapshotClusterIdentifier=cluster_id)
                deleted_count += 1
                self.logger.info(INFO_SNAPSHOT_DELETED, snapshot_id, cluster_id)
                self.result["deleted"][region].append(snapshot_id)

            except Exception as ex:
                if self.dryrun:
                    self.logger.debug(str(ex))
                    self.result["delete_cluster_snapshot"] = str(ex)
                    return self.result
                else:
                    raise ex

        self.result.update({
            "snapshots": len(self.snapshots),
            "total-deleted": deleted_count
        })
        return self.result
