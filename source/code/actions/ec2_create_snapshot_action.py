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

import services.ec2_service
from actions import *
from boto_retry import get_client_with_retries, get_default_retry_strategy
from util import safe_json
from util.tag_filter_set import TagFilterSet

SNAPHOT_STATE_ERROR = "error"
SNAPSHOT_STATE_PENDING = "pending"
SNAPSHOT_STATE_COMPLETED = "completed"

GROUP_TITLE_SNAPHOT_OPTIONS = "Snapshot volume options"
GROUP_TITLE_TAGGING_NAMING = "Tagging and naming options"

PARAM_DESC_BACKUP_DATA_VOLUMES = "Create snapshots of EC2 instance data volumes"
PARAM_DESC_BACKUP_ROOT_VOLUME = "Create snapshot of root EC2 instance volume"
PARAM_DESC_COPIED_INSTANCE_TAGS = "Enter a tag filter to copy tags from the instance to the snapshot.\
                                   For example, enter * to copy all tags from the instance to the snapshot."
PARAM_DESC_COPIED_VOLUME_TAGS = "Enter a tag filter to copy tags from the volume to the snapshot.\
                                 For example, enter * to copy all tags from the volume to the snapshot."
PARAM_DESC_SET_SNAPSHOT_NAME = "Set name of the snapshot"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = "Prefix for snapshot name"
PARAM_DESC_SNAPSHOT_TAGS = "Tags that will be added to created snapshots. Use a list of tagname=tagvalue pairs."

PARAM_LABEL_BACKUP_DATA_VOLUMES = "Copy data volumes"
PARAM_LABEL_BACKUP_ROOT_VOLUME = "Copy root volume"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied instance tags"
PARAM_LABEL_COPIED_VOLUME_TAGS = "Copied volume tags"
PARAM_LABEL_SET_SNAPSHOT_NAME = "Set snapshot name"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

INFO_COMPLETED = "Creation of snapshot(s) completed"
INFO_CREATE_SNAPSHOT = "Creating snapshot for {}volume {} ({}) of instance {}"
INFO_CREATE_TAGS = "Creating tags {} for snapshot"
INFO_CREATION_PENDING = "Creation of snapshots not completed yet"
INFO_SNAPSHOT_CREATED = "Snapshot is {}"
INFO_SNAPSHOT_NAME = "Name of the snapshot will be set to {}"
INFO_START_SNAPSHOT_ACTION = "Creating snapshot for EC2 instance {} for task {}"
INFO_STATE_SNAPSHOTS = "State of created snapshot(s) is {}"
INFO_TAGS_CREATED = "Snapshots tags created"

ERR_FAILED_SNAPSHOT = "Error creating snapshot {} for volume {}"

SNAPSHOT_DESCRIPTION = "Snapshot created by task {} for {}volume {} (device {}) of instance {}"

PARAM_BACKUP_DATA_DEVICES = "BackupDataVolumes"
PARAM_BACKUP_ROOT_DEVICE = "BackupRootVolumes"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_COPIED_VOLUME_TAGS = "CopiedVolumeTags"
PARAM_SET_SNAPSHOT_NAME = "SetSnapshotName"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"


class Ec2CreateSnapshotAction:
    properties = {
        ACTION_TITLE: "EC2 Create Snapshot",
        ACTION_VERSION: "1.1",
        ACTION_DESCRIPION: "Creates snapshot for EC2 Instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "444f070b-9302-4e67-989a-23e224518e87",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_MEMORY: 128,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_SELECT_EXPRESSION: "Reservations[?State.Name!='terminated'].Instances[].{InstanceId:InstanceId, Tags:Tags,"
                                  "RootDeviceName:RootDeviceName,BlockDeviceMappings:BlockDeviceMappings}",

        ACTION_PARAMETERS: {
            PARAM_BACKUP_ROOT_DEVICE: {
                PARAM_DESCRIPTION: PARAM_DESC_BACKUP_ROOT_VOLUME,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_BACKUP_ROOT_VOLUME
            },
            PARAM_BACKUP_DATA_DEVICES: {
                PARAM_DESCRIPTION: PARAM_DESC_BACKUP_DATA_VOLUMES,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_BACKUP_DATA_VOLUMES
            },
            PARAM_COPIED_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_INSTANCE_TAGS
            },
            PARAM_COPIED_VOLUME_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_VOLUME_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_COPIED_VOLUME_TAGS
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS
            },
            PARAM_SET_SNAPSHOT_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SET_SNAPSHOT_NAME,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_SET_SNAPSHOT_NAME
            },
            PARAM_SNAPSHOT_NAME_PREFIX: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_NAME_PREFIX,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_NAME_PREFIX
            }

        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_BACKUP_ROOT_DEVICE,
                    PARAM_BACKUP_DATA_DEVICES
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_COPIED_VOLUME_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_SET_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX
                ],
            }],

        ACTION_PERMISSIONS: ["ec2:CreateSnapshot",
                             "ec2:DescribeTags",
                             "ec2:DescribeInstances",
                             "ec2:CreateTags"],

    }

    def __init__(self, arguments):

        self._arguments = arguments
        self.logger = self._arguments[ACTION_PARAM_LOGGER]
        self.context = self._arguments[ACTION_PARAM_CONTEXT]
        self.session = self._arguments[ACTION_PARAM_SESSION]

        self.task = self._arguments[ACTION_PARAM_TASK]
        self.instance = self._arguments[ACTION_PARAM_RESOURCES]
        self.dryrun = self._arguments.get(ACTION_PARAM_DRYRUN, False)

        self.instance_id = self.instance["InstanceId"]
        self._ec2_client = None

        self.instance_tags = self.instance.get("Tags", {})

        self.volumes = {dev["Ebs"]["VolumeId"]: dev["DeviceName"] for dev in self.instance["BlockDeviceMappings"]}
        self.root_volume = [dev for dev in self.volumes if self.volumes[dev] == self.instance["RootDeviceName"]][0]

        self.copied_instance_tagfilter = TagFilterSet(self._arguments.get(PARAM_COPIED_INSTANCE_TAGS, ""))
        self.copied_volume_tagfiter = TagFilterSet(self._arguments.get(PARAM_COPIED_VOLUME_TAGS, ""))

        self.backup_root_device = self._arguments.get(PARAM_BACKUP_ROOT_DEVICE, True)
        self.backup_data_devices = self._arguments.get(PARAM_BACKUP_DATA_DEVICES, True)
        self.set_snapshot_name = self._arguments.get(PARAM_SET_SNAPSHOT_NAME, True)
        self.name_prefix = self._arguments.get(PARAM_SNAPSHOT_NAME_PREFIX, "")

        self.snapshot_tags = {}
        lastkey = None
        for tag in self._arguments.get(PARAM_SNAPSHOT_TAGS, "").split(","):
            if "=" in tag:
                t = tag.partition("=")
                self.snapshot_tags[t[0]] = t[2]
                lastkey = t[0]
            elif lastkey is not None:
                self.snapshot_tags[lastkey] = ",".join([self.snapshot_tags[lastkey], tag])

        self._all_volume_tags = None

        self.result = {
            "account": self.instance["AwsAccount"],
            "region": self.instance["Region"],
            "instance": self.instance_id,
            "task": self.task,
            "volumes": {}
        }

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = ["create_snapshot",
                       "describe_tags",
                       "describe_instances",
                       "create_tags"]
            self._ec2_client = get_client_with_retries("ec2", methods, region=self.instance["Region"], session=self.session)

        return self._ec2_client

    @property
    def all_volume_tags(self):
        if self._all_volume_tags is None:
            self._all_volume_tags = {}
            volumes = self.volumes.keys()
            decribe_tags_args = {
                "DryRun": self.dryrun,
                "Filters": [{"Name": "resource-id", "Values": volumes}]
            }
            try:
                while True:
                    describe_tag_resp = self.ec2_client.describe_tags_with_retries(**decribe_tags_args)
                    for tag in describe_tag_resp.get("Tags", []):
                        resource = tag["ResourceId"]
                        if resource not in self._all_volume_tags:
                            self._all_volume_tags[resource] = {}
                        self._all_volume_tags[resource][tag["Key"]] = tag["Value"]
                    if "NextToken" in describe_tag_resp:
                        decribe_tags_args["NextToken"] = describe_tag_resp["NextToken"]
                    else:
                        break
            except Exception as ex:
                if self.dryrun:
                    self.logger.debug(str(ex))
                    self.result["describe_tags"] = str(ex)
                    self._all_volume_tags = {v: {"dryrun": ""} for v in volumes}
                else:
                    raise ex
        return self._all_volume_tags

    def create_volume_snapshot(self, volume):
        def get_tags_for_volume_snapshot(vol):
            vol_tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.instance_tags)
            tags_on_volume = self.all_volume_tags.get(vol, {})
            vol_tags.update(self.copied_volume_tagfiter.pairs_matching_any_filter(tags_on_volume))
            vol_tags.update(self.snapshot_tags)

            return {tag_key: vol_tags[tag_key] for tag_key in vol_tags if
                    not (tag_key.startswith("aws:") or tag_key.startswith("cloudformation:"))}

        device = self.volumes[volume]
        self.result[volume] = {"device": device}

        description = SNAPSHOT_DESCRIPTION.format(self.task, "root " if volume == self.root_volume else "", volume, device,
                                                  self.instance_id)

        self.logger.info(INFO_CREATE_SNAPSHOT, volume, "root " if volume == self.root_volume else "", device, self.instance_id)

        snapshot = ""
        try:
            create_snapshot_resp = self.ec2_client.create_snapshot_with_retries(DryRun=self.dryrun, VolumeId=volume,
                                                                                Description=description)
            self.result["volumes"][volume] = {}
            self.result["volumes"][volume]["create_snapshot"] = create_snapshot_resp
            snapshot = create_snapshot_resp["SnapshotId"]
            self.logger.info(INFO_SNAPSHOT_CREATED, snapshot)

        except Exception as ex:
            if self.dryrun:
                self.logger.info(str(ex))
                self.result["volumes"][volume]["create_snapshot"] = str(ex)
            else:
                raise ex

        try:
            tags = get_tags_for_volume_snapshot(volume)

            if self.set_snapshot_name:
                dt = datetime.utcnow()
                snapshot_name = SNAPSHOT_NAME.format(volume, dt.year, dt.month, dt.day, dt.hour, dt.minute)
                if self.name_prefix:
                    snapshot_name = self.name_prefix + snapshot_name
                tags["Name"] = snapshot_name
                self.logger.info(INFO_SNAPSHOT_NAME, snapshot_name)

            self.logger.info(INFO_CREATE_TAGS, tags)
            snapshot_tags = [{"Key": t, "Value": tags[t]} for t in tags]
            create_tags_resp = self.ec2_client.create_tags_with_retries(DryRun=self.dryrun, Tags=snapshot_tags,
                                                                        Resources=[snapshot])
            self.result["volumes"][volume]["create_tags"] = create_tags_resp
            self.logger.info(INFO_TAGS_CREATED)
        except Exception as ex:
            if self.dryrun:
                self.logger.debug(str(ex))
                self.result["volumes"][volume]["create_tags"] = str(ex)
            else:
                raise ex

    def is_completed(self, _, start_results):
        """
        Tests if the create snapshot actions have been completed. This method uses the id of the created snapshots and test
        if the status of all snapshot are "available". As long as this is not the case the method must return None
        :param start_results: Result of the execute method that started the creation of the snapshots
        :param _: not used
        :return:  Result of test if all snapshots are available, None if at least one snapshot is in pending state
        """

        # start result data is passed in as text, for this action it is json formatted
        snapshot_create_data = json.loads(start_results)

        self.logger.debug("Start result data is {}", start_results)

        snapshot_ids = [volume.get("create_snapshot", {}).get("SnapshotId") for volume in
                        snapshot_create_data.get("volumes", {}).values()]

        self.logger.info("Checking status of snapshot(s) {}", ",".join(snapshot_ids))

        # create service instance to test is snapshots are available
        ec2 = services.create_service("ec2", session=self.session,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self.context))

        # test if the snapshot with the ids that were returned from the CreateSnapshot API call exists and are completed
        snapshots = ec2.describe("Snapshots", OwnerIds=["self"], Filters=[{"Name": "snapshot-id", "Values": snapshot_ids}])

        test_result = {
            "InstanceId": snapshot_create_data["instance"],
            "Volumes": [{
                "VolumeId": s["VolumeId"],
                "SnapshotId": s["SnapshotId"],
                "State": s["State"],
                "Progress": s["Progress"]
            } for s in snapshots]
        }

        self.logger.info(INFO_STATE_SNAPSHOTS, json.dumps(test_result))

        # wait until all snapshot are no longer pending
        for volume in test_result["Volumes"]:
            if volume["State"] == SNAPSHOT_STATE_PENDING:
                self.logger.info(INFO_CREATION_PENDING)
                return None

        # collect possible failed snapshots
        failed = []
        for volume in test_result["Volumes"]:
            if volume["State"] == SNAPHOT_STATE_ERROR:
                failed.append(volume)

        if len(failed) > 0:
            s = ",".join([ERR_FAILED_SNAPSHOT.format(volume["SnapshotId"], volume["VolumeId"]) for volume in failed])
            raise Exception(s)

        self.logger.info(INFO_COMPLETED)
        return safe_json(test_result)

    def execute(self, _):
        self.logger.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self.logger.info(INFO_START_SNAPSHOT_ACTION, self.instance_id, self.task)
        self.logger.debug("Instance block device mappings are {}", self.instance["BlockDeviceMappings"])

        if self.backup_root_device:
            self.create_volume_snapshot(self.root_volume)

        if self.backup_data_devices:
            for volume in self.volumes:
                if volume != self.root_volume:
                    self.create_volume_snapshot(volume)

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CreatedSnapshots=len(self.result.get("volumes", {}).values()),
            SnapshotsSizeTotal=sum(
                [volume.get("create_snapshot", {}).get("VolumeSize") for volume in
                 self.result.get("volumes", {}).values()]))

        return safe_json(self.result)
