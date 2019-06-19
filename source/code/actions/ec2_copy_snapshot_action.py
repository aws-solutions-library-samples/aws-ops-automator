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
from datetime import datetime

import boto3

import services.ec2_service
from actions import *
from boto_retry import get_client_with_retries, get_default_retry_strategy
from util import safe_json
from util.tag_filter_set import TagFilterSet

GROUP_LABEL_SNAPSHOT_COPY_OPTIONS = "Snapshot copy options"

PARAM_DESC_COPIED_MARKER_TAG = "Name of tag to add source snapshot to mark it has been copied"
PARAM_DESC_COPIED_SNAPSHOT_TAGS = "Copied tags from source snapshot"
PARAM_DESC_DESCRIPTION = "Description for copied snapshot"
PARAM_DESC_DESTINATION_REGION = "Destination region for copied snapshot"
PARAM_DESC_SNAPSHOT_TAGS = "Tags to add to copied snapshot"

PARAM_LABEL_COPIED_MARKER_TAG = "Tagname for copied snapshots"
PARAM_LABEL_COPIED_SNAPSHOT_TAGS = "Copied tags"
PARAM_LABEL_DESCRIPTION = "Description"
PARAM_LABEL_DESTINATION_REGION = "Destination region"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"

PARAM_COPIED_MARKER_TAG = "CopiedToTag"
PARAM_COPIED_SNAPSHOT_TAGS = "CopiedSnapshotTags"
PARAM_DESCRIPTION = "Description"
PARAM_DESTINATION_REGION = "DestinationRegion"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"

INFO_ACCOUNT_SNAPSHOT = "Copying snapshot {} for account {} from region {} to region{}"
INFO_SNAPSHOT_COPIED = "Copy of  snapshot {} to region {} snapshot {} started"
INFO_CREATE_TAGS = "Creating tags {} for copied snapshot"
INFO_TAGS_CREATED = "Tags created for copied snapshots"
INFO_COPY_PENDING = "Snapshot with id {} does not exist or is pending in region {}"
INFO_COPY_COMPLETED = "Snapshot {} from region {} copied to snapshot {} in region"
INFO_CHECK_COMPLETED_RESULT = "Snapshot copy completion check result is {}"


class Ec2CopySnapshotAction:
    """
    Class implements action for copying EC2 Snapshots
    """
    properties = {
        ACTION_TITLE: "EC2 Copy Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPION: "Copies EC2 snapshot",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "eb287af5-e5c0-41cb-832b-d218c075fa26",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_MEMORY: 128,

        ACTION_SELECT_EXPRESSION:
        # selecting snapshot information, the jmespath expression does skip snapshots that already have
        # been tagged with the tag specified in the PARAM_COPIED_MARKER_TAG parameter
            "Snapshots[?State=='completed'].{SnapshotId:SnapshotId, VolumeId:VolumeId, StartTime:StartTime,Tags:Tags}" +
            "|[?Tags]| [?!contains(Tags[*].Key,'%{}%')]".format(PARAM_COPIED_MARKER_TAG),

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: {'OwnerIds': ["self"]},

        # Ec2 CopySnapshot only allows 5 concurrent copies per account to a destination region
        ACTION_MAX_CONCURRENCY: 5,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_PARAMETERS: {
            PARAM_DESTINATION_REGION: {
                PARAM_DESCRIPTION: PARAM_DESC_DESTINATION_REGION,
                PARAM_LABEL: PARAM_LABEL_DESTINATION_REGION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True,
                PARAM_ALLOWED_VALUES: [str(r) for r in boto3.Session().get_available_regions("ec2", "aws")]
            },
            PARAM_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_DESCRIPTION,
                PARAM_LABEL: PARAM_LABEL_DESCRIPTION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_COPIED_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_COPIED_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False
            },
            PARAM_COPIED_MARKER_TAG: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_MARKER_TAG,
                PARAM_LABEL: PARAM_LABEL_COPIED_MARKER_TAG,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: "Ec2CopySnapshot:SnapshotCopied"
            }
        },
        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_SNAPSHOT_COPY_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_SNAPSHOT_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_DESTINATION_REGION,
                    PARAM_DESCRIPTION,
                    PARAM_COPIED_MARKER_TAG
                ],
            }],

        ACTION_PERMISSIONS: ["ec2:CopySnapshot", "ec2:CreateTags"]

    }

    @staticmethod
    def action_validate_parameters(parameters):
        """
        Parameter validation logic. Checks if destination region is valid
        :param parameters: input parameters
        :return: validated parameters
        """
        valid_regions = boto3.Session().get_available_regions("ec2", "aws")
        region = parameters.get(PARAM_DESTINATION_REGION)
        if region not in valid_regions:
            raise ValueError("{} is not a valid region, valid regions are: ".format(region, ",".join(valid_regions)))
        return parameters

    @staticmethod
    def action_concurrency_key(arguments):
        """
        Returns key for concurrency control of the scheduler. As the CopySnapshot API call only allows 5 concurrent copies
        per account to a destination region this method returns a key containing the name of the api call, the account and
        the destination account.
        :param arguments: Task arguments
        :return: Concurrency key
        """
        return "ec2:CopySnapshot:{}:{}".format(arguments[ACTION_PARAM_ACCOUNT], arguments[PARAM_DESTINATION_REGION])

    def is_completed(self, _, start_results):
        """
        Tests if the copy snapshot action has been completed. This method uses the id of the copied snapshot and test if it
        does exist and is complete in the destination region. As long as this is not the case the method must return None
        :param start_results: Result of the api that started the copy, contains the id of the snapshot in the destination region
        :param _: not used
        :return:  Result of copy action, None if not completed yet
        """

        # start result data is passed in as text, for this action it is json formatted
        snapshot_create_data = json.loads(start_results)

        # create service instance to test is snapshot exists
        ec2 = services.create_service("ec2", session=self.session,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self.context))
        copied_snapshot_id = snapshot_create_data["copied-snapshot-id"]

        # test if the snapshot with the id that was returned from the CopySnapshot API call exists and is completed
        copied_snapshot = ec2.get("Snapshots", region=self.destination_region, select="Snapshots[?State=='completed']",
                                  OwnerIds=["self"], Filters=[{"Name": "snapshot-id", "Values": [copied_snapshot_id]}])

        if copied_snapshot is not None:
            # action completed
            self.logger.info(INFO_CHECK_COMPLETED_RESULT, copied_snapshot)
            self.logger.info(INFO_COPY_COMPLETED, self.source_snapshot_id, self.source_region, copied_snapshot_id,
                             self.destination_region)
            return safe_json(copied_snapshot)

        # not done yet
        self.logger.info(INFO_COPY_PENDING, copied_snapshot_id, self.destination_region)
        return None

    def __init__(self, arguments):
        """
        Initializes copy snapshot action
        :param arguments: arguments passed in by scheduler
        """

        # logger task and session
        self.logger = arguments[ACTION_PARAM_LOGGER]
        self.task = arguments[ACTION_PARAM_TASK]
        self.session = arguments[ACTION_PARAM_SESSION]
        self.context = arguments[ACTION_PARAM_CONTEXT]

        # debug and dryrun
        self.snapshot = arguments[ACTION_PARAM_RESOURCES]
        self.dryrun = arguments.get(ACTION_PARAM_DRYRUN, False)

        # snapshot source and destination information
        self.account = self.snapshot["AwsAccount"]
        self.source_snapshot_id = self.snapshot["SnapshotId"]
        self.source_region = self.snapshot["Region"]
        self.destination_region = arguments.get(PARAM_DESTINATION_REGION)
        self.description = arguments.get(PARAM_DESCRIPTION, "").strip()

        # tagging
        self.copied_volume_tagfiter = TagFilterSet(arguments.get(PARAM_COPIED_SNAPSHOT_TAGS, ""))
        self.snapshot_tags = {}
        lastkey = None
        for tag in arguments.get(PARAM_SNAPSHOT_TAGS, "").split(","):
            if "=" in tag:
                t = tag.partition("=")
                self.snapshot_tags[t[0]] = t[2]
                lastkey = t[0]
            elif lastkey is not None:
                self.snapshot_tags[lastkey] = ",".join([self.snapshot_tags[lastkey], tag])

        self.marked_as_copied_tag = arguments[PARAM_COPIED_MARKER_TAG]

        # setup result with known values
        self.result = {
            "account": self.account,
            "task": self.task,
            "destination-region": self.destination_region,
            "source-region": self.source_region,
            "source-snapshot-id": self.source_snapshot_id
        }

    def execute(self, _):
        """
        Executes logic of copy snapshot action
        :param _:
        :return: Result of starting the snapshot copy and setting the tags on the copy
        """

        def get_tags_for_copied_snapshot():

            snap_shot_tags = (self.copied_volume_tagfiter.pairs_matching_any_filter(self.snapshot.get("Tags", {})))
            snap_shot_tags.update(self.snapshot_tags)
            return {tag_key: snap_shot_tags[tag_key] for tag_key in snap_shot_tags if
                    not (tag_key.startswith("aws:") or tag_key.startswith("cloudformation:"))}

        # logged information
        self.logger.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])
        self.logger.info(INFO_ACCOUNT_SNAPSHOT, self.source_snapshot_id, self.account, self.source_region, self.destination_region)
        self.logger.debug("Snapshot : {}", self.snapshot)

        # ec2 client for destination to create copy and tag
        ec2_destination = get_client_with_retries("ec2", ["copy_snapshot", "create_tags"], region=self.destination_region,
                                                  context=self.context, session=self.session)
        # ec2 client for source to set tag on source to mark it as copied
        ec2_source = get_client_with_retries("ec2", ["create_tags"], region=self.source_region, context=self.context,
                                             session=self.session)

        boto_call = "copy_snapshot"
        try:
            # setup argument for CopySnapshot call
            args = {
                "SourceRegion": self.source_region,
                "SourceSnapshotId": self.source_snapshot_id
            }
            if self.dryrun:
                args["DryRun"] = True

            if self.description != "":
                args["Description"] = self.description

            # start the copy
            resp = ec2_destination.copy_snapshot_with_retries(**args)
            # id of the copy
            copied_snapshot_id = resp.get("SnapshotId")
            self.logger.info(INFO_SNAPSHOT_COPIED, self.source_snapshot_id, self.destination_region, copied_snapshot_id)
            self.result[boto_call] = resp
            self.result["copied-snapshot-id"] = copied_snapshot_id

            # set tags on the copy
            boto_call = "create_tags (target)"
            tags = get_tags_for_copied_snapshot()
            self.logger.info(INFO_CREATE_TAGS, tags)
            snapshot_tags = [{"Key": t, "Value": tags[t]} for t in tags]
            self.result[boto_call] = ec2_destination.create_tags_with_retries(Tags=snapshot_tags, Resources=[copied_snapshot_id])
            self.logger.info(INFO_TAGS_CREATED)

            # set a tag on the source snapshot to mark it as copied
            boto_call = "create_tags (source)"
            source_tags = [{
                "Key": self.marked_as_copied_tag,
                "Value": safe_json({
                    "destination-region": self.destination_region,
                    "copied-snapshot-id": copied_snapshot_id,
                    "copied": datetime.now().isoformat()
                })
            }]
            self.result[boto_call] = ec2_source.create_tags_with_retries(Tags=source_tags, Resources=[self.source_snapshot_id])

        except Exception as ex:
            if self.dryrun:
                self.logger.debug(str(ex))
                self.result[boto_call] = str(ex)
                return self.result
            else:
                raise ex

        self.result[METRICS_DATA] = build_action_metrics(self, CopiedSnapshots=1)

        return safe_json(self.result)
