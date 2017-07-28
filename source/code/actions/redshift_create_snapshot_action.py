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

from datetime import datetime

import services.redshift_service
from actions import *
from boto_retry import get_client_with_retries
from util.tag_filter_set import TagFilterSet

GROUP_TITLE_TAGGING_NAMING = "Tagging and naming options"

PARAM_DESC_COPIED_CLUSTER_TAGS = "Copied tags from instance to snapshot"
PARAM_DESC_SNAPSHOT_TAGS = "Tags to add to snapshot, use list of tagname=tagvalue pairs"
PARAM_DESC_ACCOUNTS_RESTORE_ACCESS = "Comma separated list of accounts that will be granted access to restore the snapshot"

PARAM_LABEL_COPIED_CLUSTER_TAGS = "Copied cluster tags"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_CCOUNTS_RESTORE_ACCESS = "Grant restore access to accounts"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

INFO_CREATE_SNAPSHOT = "Creating snapshot for redshift cluster \"{}\""
INFO_SNAPSHOT_CREATED = "Snapshot is {}"
INFO_SNAPSHOT_NAME = "Name of the snapshot is {}"
INFO_START_SNAPSHOT_ACTION = "Creating snapshot for redshift cluster \"{}\" for task \"{}\""
INFO_GRANT_ACCOUNT_ACCESS = "Granted access to snapshot for account {}"

PARAM_COPIED_CLUSTER_TAGS = "CopiedInstanceTags"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_ACCOUNTS_RESTORE_ACCESS = "AccountsWithRestoreAccess"


class RedshiftCreateSnapshotAction:
    properties = {
        ACTION_TITLE: "RedShift Create Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPION: "Creates manual type snapshot for Redshift cluster",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "6310b757-d8a8-4031-af29-29b9fc5bcf65",

        ACTION_SERVICE: "redshift",
        ACTION_RESOURCES: services.redshift_service.CLUSTERS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_MEMORY: 128,

        ACTION_SELECT_EXPRESSION: "Clusters[*].{ClusterIdentifier:ClusterIdentifier,ClusterStatus:ClusterStatus,Tags:Tags}",

        ACTION_PARAMETERS: {

            PARAM_COPIED_CLUSTER_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_CLUSTER_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_CLUSTER_TAGS
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_ACCOUNTS_RESTORE_ACCESS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_RESTORE_ACCESS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_CCOUNTS_RESTORE_ACCESS
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_CLUSTER_TAGS,
                    PARAM_SNAPSHOT_TAGS
                ],
            }],

        ACTION_PERMISSIONS: ["redshift:DescribeClusters",
                             "redshift:CreateClusterSnapshot",
                             "redshift:DescribeTags",
                             "redshift:AuthorizeSnapshotAccess"]

    }

    def __init__(self, arguments):

        self._arguments = arguments
        self.logger = self._arguments[ACTION_PARAM_LOGGER]
        self.context = self._arguments[ACTION_PARAM_CONTEXT]
        self.session = self._arguments[ACTION_PARAM_SESSION]

        self.task = self._arguments[ACTION_PARAM_TASK]
        self.cluster = self._arguments[ACTION_PARAM_RESOURCES]

        self.cluster_id = self.cluster["ClusterIdentifier"]
        self.cluster_tags = self.cluster.get("Tags", {})
        self.cluster_status = self.cluster["ClusterStatus"]

        self.copied_instance_tagfilter = TagFilterSet(self._arguments.get(PARAM_COPIED_CLUSTER_TAGS, ""))

        self.snapshot_tags = {}
        for tag in self._arguments.get(PARAM_SNAPSHOT_TAGS, "").split(","):
            t = tag.partition("=")
            self.snapshot_tags[t[0]] = t[2]

        self.logger.debug("Arguments {}", self._arguments)
        self.granted_accounts = self._arguments.get(PARAM_ACCOUNTS_RESTORE_ACCESS, [])

        self.result = {
            "account": self.cluster["AwsAccount"],
            "region": self.cluster["Region"],
            "cluster-identifier": self.cluster_id,
            "task": self.task
        }

    def execute(self, _):

        self.logger.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self.logger.info(INFO_START_SNAPSHOT_ACTION, self.cluster_id, self.task)

        if self.cluster_status != "available":
            raise Exception("Status of cluster is \"{}\", can only make snapshot of cluster with status \"available\"".format(
                self.cluster_status))

        tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.cluster_tags)
        tags.update(self.snapshot_tags)
        snapshot_tags = [{"Key": t, "Value": tags[t]} for t in tags]

        dt = datetime.utcnow()
        snapshot_name = SNAPSHOT_NAME.format(self.cluster_id, dt.year, dt.month, dt.day, dt.hour, dt.minute)

        redshift = get_client_with_retries("redshift", ["create_cluster_snapshot", "authorize_snapshot_access"],
                                           context=self.context, session=self.session)

        create_snapshot_resp = redshift.create_cluster_snapshot_with_retries(SnapshotIdentifier=snapshot_name,
                                                                             Tags=snapshot_tags,
                                                                             ClusterIdentifier=self.cluster_id)
        self.result["snapshot-identifier"] = snapshot_name
        self.result["snapshot-create-time"] = create_snapshot_resp["Snapshot"]["SnapshotCreateTime"]
        self.logger.info(INFO_SNAPSHOT_CREATED, snapshot_name)

        if  self.granted_accounts is not None and len(self.granted_accounts) > 0:
            for account in self.granted_accounts:
                redshift.authorize_snapshot_access_with_retries(SnapshotIdentifier=snapshot_name,
                                                                SnapshotClusterIdentifier=self.cluster_id,
                                                                AccountWithRestoreAccess=account)

                self.logger.info(INFO_GRANT_ACCOUNT_ACCESS, account)
            self.result["granted-access-accounts"] = self.granted_accounts

        return self.result
