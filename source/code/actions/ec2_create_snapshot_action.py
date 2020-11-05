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

from datetime import timedelta

import dateutil.parser

import actions
import handlers.ec2_state_event_handler
import handlers.ec2_tag_event_handler
import services
import services.aws_service
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from actions.action_ec2_events_base import ActionEc2EventBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
from outputs import raise_exception
from tagging.tag_filter_expression import TagFilterExpression
from tagging.tag_filter_set import TagFilterSet

TAG_PLACEHOLDER_INSTANCE_ID = "instance-id"
TAG_PLACEHOLDER_VOLUME_ID = "volume-id"
TAG_PLACEHOLDER_DEVICE = "device"
TAG_PLACEHOLDER_INSTANCE_SNAPSHOTS = "snapshot-ids"
TAG_PLACEHOLDER_VOLUME_SNAPSHOT = "snapshot-id"

SNAPSHOT_STATE_ERROR = "error"
SNAPSHOT_STATE_PENDING = "pending"
SNAPSHOT_STATE_COMPLETED = "completed"

GROUP_TITLE_SNAPSHOT_OPTIONS = "Snapshot volume options"
GROUP_TITLE_TAGGING = "Tagging options"
GROUP_TITLE_NAMING = "Snapshot naming and description"
GROUP_TITLE_SHARING = "Snapshot sharing options"

PARAM_DESC_SNAPSHOT_DESCRIPTION = \
    "Description for snapshot, leave blank for default description."
PARAM_DESC_SHARED_ACCOUNT_TAGGING_ROLENAME = \
    "Name of the cross account role in the accounts the snapshot is shared with. This role is used to create tags in these " \
    "accounts for the shared snapshot. Leave this parameter empty to use the default role with name \"{}\". The role must give " \
    "permissions to use the Ec2SetTags action."
PARAM_DESC_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = \
    "List of accounts that will be granted access to create volumes from the snapshot."
PARAM_DESC_BACKUP_DATA_VOLUMES = \
    "Create snapshots of EC2 instance data (non-root) volumes"
PARAM_DESC_BACKUP_ROOT_VOLUME = \
    "Create snapshot of root EC2 instance volume"
PARAM_DESC_COPIED_INSTANCE_TAGS = \
    "Tag filter expression to copy tags from the instance to the snapshot. For example, use * to copy all tags from " \
    "the instance to the snapshot."
PARAM_DESC_COPIED_VOLUME_TAGS = \
    "Tag filter expression to copy tags from the volume to the snapshot. For example, use * to copy all tags from the volume " \
    "to the snapshot."
PARAM_DESC_TAG_SHARED_SNAPSHOTS = \
    "Create tags for shared snapshots in the accounts that have create volume permission."
PARAM_DESC_SET_SNAPSHOT_NAME = \
    "Set name of the snapshot"
PARAM_DESC_SNAPSHOT_NAME_PREFIX = \
    "Prefix for snapshot name"
PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags that will be added to created snapshots. Use a list of tagname=tagvalue pairs."
PARAM_DESC_INSTANCE_TAGS = \
    "Tags to set on source EC2 instance after the snapshots has been created successfully."
PARAM_DESC_NAME = \
    "Name of the created snapshot, leave blank for default snapshot name"
PARAM_DESC_MAX_CONCURRENT = \
    "Maximum number of concurrent snapshots creation tasks running concurrently per account (1-{})"
PARAM_DESC_VOLUME_TAG_FILTER = \
    "Tag filter expression to specify filter that will be applied to select volumes for which a snapshot will be taken " \
    "based on the tags on these volumes. Lease blank to select all root and/or data volumes"
PARAM_DESC_VOLUME_TAGS = \
    "Tags to set on source EBS volume after the snapshot has been created successfully."

PARAM_LABEL_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "Accounts with create volume permission"
PARAM_LABEL_BACKUP_DATA_VOLUMES = "Copy data volumes"
PARAM_LABEL_BACKUP_ROOT_VOLUME = "Copy root volume"
PARAM_LABEL_COPIED_INSTANCE_TAGS = "Copied instance tags"
PARAM_LABEL_COPIED_VOLUME_TAGS = "Copied volume tags"
PARAM_LABEL_SHARED_ACCOUNT_TAGGING_ROLENAME = "Cross account role name for tagging of shared snapshots"
PARAM_LABEL_INSTANCE_TAGS = "Instance tags"
PARAM_LABEL_NAME = "Snapshot name"
PARAM_LABEL_SET_SNAPSHOT_NAME = "Set snapshot name"
PARAM_LABEL_SNAPSHOT_DESCRIPTION = "Snapshot description"
PARAM_LABEL_SNAPSHOT_NAME_PREFIX = "Snapshot name prefix"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_TAG_SHARED_SNAPSHOTS = "Create tags for shared snapshots"
PARAM_LABEL_VOLUME_TAG_FILTER = "Volume tag filter"
PARAM_LABEL_VOLUME_TAGS = "Volume tags"

SNAPSHOT_NAME = "{}-{:0>4d}{:0>2d}{:0>2d}{:0>02d}{:0>02d}"

INF_SKIP_VOLUME_TAG_FILTER = "Skipping volume {} as its tags {} do not match the volume tag filter expression"
INFO_COMPLETED = "Creation of snapshot(s) completed"
INFO_CREATE_SNAPSHOT = "Creating snapshot for {}volume {}({}) of instance {}"
INFO_CREATE_TAGS = "Creating snapshot tags\n{}"
INFO_CREATION_PENDING = "Creation of snapshots not completed yet"
INFO_NOT_ALL_IN_PROGRESS = "Not all snapshots have been created or are in progress yet"
INFO_PENDING_SNAPSHOTS = "Volume {} has a pending snapshots {}"
INFO_SET_INSTANCE_TAGS = "Set tags\n{}\nto instance {}"
INFO_SET_SNAPSHOT_TAGS_SHARED = "Set tags\n{}\nto snapshot {} in account {}, region {}"
INFO_SET_VOLUME_TAGS = "Set tags\n{}\nto volume {}"
INFO_SETTING_CREATE_VOLUME_PERMISSIONS = "Setting create volume permissions for {}"
INFO_SNAPSHOT_CREATED = "Snapshot is {}"
INFO_SNAPSHOT_NAME = "Name of the snapshot will be set to {}"
INFO_START_SNAPSHOT_ACTION = "Creating snapshot for EC2 instance {} in account{}, region {} for task {}"
INFO_STATE_SNAPSHOTS = "State of created snapshot(s) is\n{}"
INFO_TAGS_CREATED = "Snapshots tags created"
INFO_CHECKING_SNAPSHOT_STATUS = "Checking status of snapshot(s) {}"

ERR_FAILED_SNAPSHOT = "Error creating snapshot {} for instance volume {}"
ERR_SETTING_CREATE_VOLUME_PERMISSIONS = "Error setting create volume permissions for account(s) {}, {}"
ERR_SETTING_INSTANCE_TAGS = "Error setting tags to instance {}, {}"
ERR_SETTING_VOLUME_TAGS = "Error setting tags to volume {}, {}"
ERR_SETTING_SHARED_TAGS = "Can not set tags for created shared snapshots in account {}, {}"
ERR_SNAPSHOT_PENDING = "Volume {} already has pending snapshots, no snapshot will be taken for this volume"
ERR_MISSING_SNAPSHOTS = "One or more snapshots are not created or deleted {}"
ERR_TAGS_NOT_SET_IN_ACCOUNT = "Tags not set in account {}"

WARN_ROOT_NOT_FOUND = "Root device for instance {} not backed up as it could not be found. devices are {}."

SNAPSHOT_DESCRIPTION = "Snapshot created by task {} for {}volume {} (device {}) of instance {}"

PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "CreateVolumePermission"
PARAM_BACKUP_DATA_DEVICES = "BackupDataVolumes"
PARAM_BACKUP_ROOT_DEVICE = "BackupRootVolumes"
PARAM_COPIED_INSTANCE_TAGS = "CopiedInstanceTags"
PARAM_COPIED_VOLUME_TAGS = "CopiedVolumeTags"
PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME = "TagInSharedAccountRoleName"
PARAM_INSTANCE_TAGS = "InstanceTags"
PARAM_NAME = "SnapshotName"
PARAM_NO_REBOOT = "NoReboot"
PARAM_SET_SNAPSHOT_NAME = "SetSnapshotName"
PARAM_SNAPSHOT_DESCRIPTION = "SnapshotDescription"
PARAM_SNAPSHOT_NAME_PREFIX = "SnapshotNamePrefix"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_TAG_SHARED_SNAPSHOTS = "TagSharedSnapshots"
PARAM_VOLUME_TAG_FILTER = "VolumeTagFilter"
PARAM_VOLUME_TAGS = "VolumeTags"


class Ec2CreateSnapshotAction(ActionEc2EventBase):
    properties = {
        ACTION_TITLE: "EC2 Create Snapshot",
        ACTION_VERSION: "1.2",
        ACTION_DESCRIPTION: "Creates snapshots for selected volumes of an EC2 Instance",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "444f070b-9302-4e67-989a-23e224518e87",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,

        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,
        ACTION_SELECT_EXPRESSION: "Reservations[*].Instances[].{InstanceId:InstanceId, Tags:Tags,"
                                  "RootDeviceName:RootDeviceName,BlockDeviceMappings:BlockDeviceMappings, "
                                  "State:State.Name}|[?State!='terminated']",

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 60,

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ec2_state_event_handler.EC2_STATE_NOTIFICATION:
                    [handlers.ec2_state_event_handler.EC2_STATE_RUNNING,
                     handlers.ec2_state_event_handler.EC2_STATE_STOPPED]
            },
            handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                handlers.TAG_CHANGE_EVENT: [
                    handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT]
            }
        },

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],

        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_PARAMETERS: {
            PARAM_BACKUP_ROOT_DEVICE: {
                PARAM_DESCRIPTION: PARAM_DESC_BACKUP_ROOT_VOLUME,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_BACKUP_ROOT_VOLUME
            },
            PARAM_SNAPSHOT_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_DESCRIPTION,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_DESCRIPTION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_BACKUP_DATA_DEVICES: {
                PARAM_DESCRIPTION: PARAM_DESC_BACKUP_DATA_VOLUMES,
                PARAM_TYPE: type(True),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: True,
                PARAM_LABEL: PARAM_LABEL_BACKUP_DATA_VOLUMES
            },
            PARAM_VOLUME_TAG_FILTER: {
                PARAM_DESCRIPTION: PARAM_DESC_VOLUME_TAG_FILTER,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: "",
                PARAM_LABEL: PARAM_LABEL_VOLUME_TAG_FILTER
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
            PARAM_VOLUME_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_VOLUME_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_VOLUME_TAGS
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
            },
            PARAM_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_NAME,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_NAME
            },
            PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_VOLUME_CREATE_PERMISSIONS,
                PARAM_TYPE: type([]),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_VOLUME_CREATE_PERMISSIONS
            },
            PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SHARED_ACCOUNT_TAGGING_ROLENAME.format(handlers.default_rolename_for_stack()),
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SHARED_ACCOUNT_TAGGING_ROLENAME
            },
            PARAM_TAG_SHARED_SNAPSHOTS: {
                PARAM_DESCRIPTION: PARAM_DESC_TAG_SHARED_SNAPSHOTS,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_TAG_SHARED_SNAPSHOTS
            },
            PARAM_INSTANCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_INSTANCE_TAGS,
                PARAM_TYPE: type(""),
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_INSTANCE_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SNAPSHOT_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_BACKUP_ROOT_DEVICE,
                    PARAM_BACKUP_DATA_DEVICES,
                    PARAM_VOLUME_TAG_FILTER

                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_NAMING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_SET_SNAPSHOT_NAME,
                    PARAM_SNAPSHOT_NAME_PREFIX,
                    PARAM_NAME,
                    PARAM_SNAPSHOT_DESCRIPTION
                ]
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_TAGGING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_INSTANCE_TAGS,
                    PARAM_COPIED_VOLUME_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                    PARAM_VOLUME_TAGS,
                    PARAM_INSTANCE_TAGS
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_SHARING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS,
                    PARAM_TAG_SHARED_SNAPSHOTS,
                    PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME
                ],
            },

        ],

        ACTION_PERMISSIONS: [
            "ec2:CreateSnapshot",
            "ec2:DescribeTags",
            "ec2:DescribeInstances",
            "ec2:DescribeSnapshots",
            "ec2:DescribeVolumes",
            "ec2:ModifySnapshotAttribute",
            "ec2:CreateTags",
            "ec2:DeleteTags"
        ],

    }

    def __init__(self, arguments, action_parameters):
        """
        Initializes this volume

        Args:
            self: (dict): write your description
            arguments: (dict): write your description
            action_parameters: (todo): write your description
        """

        ActionBase.__init__(self, arguments, action_parameters)

        self.instance = self._resources_

        self.instance_id = self.instance["InstanceId"]
        self._ec2_client = None

        # tags on the instance
        self.tags_on_instance = self.instance.get("Tags", {})

        self.volumes = {dev["Ebs"]["VolumeId"]: dev["DeviceName"] for dev in self.instance["BlockDeviceMappings"]}

        self.root_volume = None
        for dev in self.volumes:
            if self.volumes[dev] == self.instance["RootDeviceName"]:
                self.root_volume = dev

        self.accounts_with_create_permissions = self.get(PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS, [])
        self.tag_shared_snapshots = self.get(PARAM_TAG_SHARED_SNAPSHOTS, False)

        self.copied_instance_tagfilter = TagFilterSet(self.get(PARAM_COPIED_INSTANCE_TAGS, ""))
        self.copied_volume_tagfilter = TagFilterSet(self.get(PARAM_COPIED_VOLUME_TAGS, ""))

        self.backup_root_device = self.get(PARAM_BACKUP_ROOT_DEVICE, True)
        self.backup_data_devices = self.get(PARAM_BACKUP_DATA_DEVICES, True)
        self.set_snapshot_name = self.get(PARAM_SET_SNAPSHOT_NAME, True)

        volume_tag_filter = self.get(PARAM_VOLUME_TAG_FILTER, None)
        self.volume_tag_filter = TagFilterExpression(volume_tag_filter) if volume_tag_filter not in ["", None] else None

        self._all_volume_tags = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "instance": self.instance_id,
            "task": self._task_,
            "volumes": {},
            "snapshots": {}
        }

    @staticmethod
    def action_logging_subject(arguments, _):
        """
        Returns a string representing the given action.

        Args:
            arguments: (todo): write your description
            _: (todo): write your description
        """
        instance = arguments[ACTION_PARAM_RESOURCES]
        instance_id = instance["InstanceId"]
        account = instance["AwsAccount"]
        region = instance["Region"]
        return "{}-{}-{}-{}".format(account, region, instance_id, log_stream_date())

    @property
    def ec2_client(self):
        """
        Return ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec

        Args:
            self: (todo): write your description
        """
        if self._ec2_client is None:
            methods = ["create_snapshot",
                       "describe_tags",
                       "delete_tags",
                       "describe_instances",
                       "modify_snapshot_attribute",
                       "create_tags"]
            self._ec2_client = get_client_with_retries("ec2", methods,
                                                       region=self.instance["Region"],
                                                       session=self._session_,
                                                       logger=self._logger_)

        return self._ec2_client

    @property
    def all_volume_tags(self):
        """
        Return a list of all ebs volumes

        Args:
            self: (todo): write your description
        """
        if self._all_volume_tags is None:
            self._all_volume_tags = {}
            volumes = list(self.volumes.keys())
            describe_tags_args = {
                "DryRun": self._dryrun_,
                "Filters": [
                    {
                        "Name": "resource-id", "Values": volumes
                    }
                ]
            }
            try:
                while True:
                    describe_tag_resp = self.ec2_client.describe_tags_with_retries(**describe_tags_args)
                    for tag in describe_tag_resp.get("Tags", []):
                        resource = tag["ResourceId"]
                        if resource not in self._all_volume_tags:
                            self._all_volume_tags[resource] = {}
                        self._all_volume_tags[resource][tag["Key"]] = tag["Value"]
                    if "NextToken" in describe_tag_resp:
                        describe_tags_args["NextToken"] = describe_tag_resp["NextToken"]
                    else:
                        break
            except Exception as ex:
                if self._dryrun_:
                    self._logger_.debug(str(ex))
                    self.result["describe_tags"] = str(ex)
                    self._all_volume_tags = {v: {"dryrun": ""} for v in volumes}
                else:
                    raise ex
        return self._all_volume_tags

    def create_volume_snapshot(self, volume):
        """
        Creates a new volume

        Args:
            self: (todo): write your description
            volume: (todo): write your description
        """

        def create_snapshot(vol, snapshot_description):
            """
            Creates a new snapshot.

            Args:
                vol: (str): write your description
                snapshot_description: (str): write your description
            """
            snapshot_id = ""
            try:
                create_snapshot_resp = self.ec2_client.create_snapshot_with_retries(DryRun=self._dryrun_, VolumeId=vol,
                                                                                    Description=snapshot_description)
                self.result["volumes"][vol] = {}
                self.result["volumes"][vol]["create_snapshot"] = create_snapshot_resp
                snapshot_id = create_snapshot_resp["SnapshotId"]
                self.result["volumes"][vol]["snapshot"] = snapshot_id
                self._logger_.info(INFO_SNAPSHOT_CREATED, snapshot_id)

            except Exception as ex:
                if self._dryrun_:
                    self._logger_.info(str(ex))
                    self.result["volumes"][volume]["create_snapshot"] = str(ex)
                else:
                    raise ex

            return snapshot_id

        def set_snapshot_tags(snap, vol, dev):
            """
            Sets the tags

            Args:
                snap: (todo): write your description
                vol: (str): write your description
                dev: (todo): write your description
            """
            try:
                tags = get_tags_for_volume_snapshot(vol, dev)

                if self.set_snapshot_name:

                    snapshot_name = self.build_str_from_template(parameter_name=PARAM_NAME,
                                                                 tag_variables={
                                                                     TAG_PLACEHOLDER_INSTANCE_ID: self.instance_id,
                                                                     TAG_PLACEHOLDER_VOLUME_ID: volume
                                                                 })
                    if snapshot_name == "":
                        dt = self._datetime_.utcnow()
                        snapshot_name = SNAPSHOT_NAME.format(volume, dt.year, dt.month, dt.day, dt.hour, dt.minute)

                    prefix = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_NAME_PREFIX,
                                                          tag_variables={
                                                              TAG_PLACEHOLDER_INSTANCE_ID: self.instance_id,
                                                              TAG_PLACEHOLDER_VOLUME_ID: volume
                                                          })
                    snapshot_name = prefix + snapshot_name

                    tags["Name"] = snapshot_name

                    self._logger_.info(INFO_SNAPSHOT_NAME, snapshot_name)

                if len(tags) > 0:
                    self._logger_.info(INFO_CREATE_TAGS, safe_json(tags, indent=3))
                    tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                         resource_ids=[snap],
                                         tags=tags,
                                         can_delete=False,
                                         logger=self._logger_)

                    if snap not in self.result["snapshots"]:
                        self.result["snapshots"][snap] = {}
                    self.result["snapshots"][snap]["tags"] = tags

                    self._logger_.info(INFO_TAGS_CREATED)
            except Exception as ex:
                if self._dryrun_:
                    self._logger_.debug(str(ex))
                    self.result["volumes"][volume]["create_tags"] = str(ex)
                else:
                    raise ex

        def get_tags_for_volume_snapshot(vol, dev):
            """
            Returns a list of all tags associated volume

            Args:
                vol: (todo): write your description
                dev: (todo): write your description
            """
            vol_tags = self.copied_instance_tagfilter.pairs_matching_any_filter(self.tags_on_instance)
            tags_on_volume = self.all_volume_tags.get(vol, {})
            vol_tags.update(self.copied_volume_tagfilter.pairs_matching_any_filter(tags_on_volume))
            vol_tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_INSTANCE_ID: self.instance_id,
                                                  TAG_PLACEHOLDER_VOLUME_ID: volume,
                                                  TAG_PLACEHOLDER_DEVICE: dev

                                              }))

            vol_tags[actions.marker_snapshot_tag_source_source_volume_id()] = volume

            return vol_tags

        device = self.volumes[volume]
        self.result[volume] = {"device": device}

        description = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_DESCRIPTION,
                                                   tag_variables={
                                                       TAG_PLACEHOLDER_INSTANCE_ID: self.instance_id,
                                                       TAG_PLACEHOLDER_VOLUME_ID: volume,
                                                       TAG_PLACEHOLDER_DEVICE: device
                                                   })
        if description == "":
            description = SNAPSHOT_DESCRIPTION.format(self._task_, "root " if volume == self.root_volume else "", volume, device,
                                                      self.instance_id)

        self._logger_.info(INFO_CREATE_SNAPSHOT, volume, "root " if volume == self.root_volume else "", device, self.instance_id)

        snapshot = create_snapshot(volume, description)
        set_snapshot_tags(snapshot, volume, device)

    def is_completed(self, snapshot_create_data):
        """
        Checks if a list of the specified.

        Args:
            self: (todo): write your description
            snapshot_create_data: (dict): write your description
        """

        def grant_create_volume_permissions(snap_ids):
            """
            Grant permissions onmissions.

            Args:
                snap_ids: (str): write your description
            """

            if self.accounts_with_create_permissions is not None and len(self.accounts_with_create_permissions) > 0:

                args = {

                    "CreateVolumePermission": {
                        "Add": [{"UserId": a.strip()} for a in self.accounts_with_create_permissions]
                    }
                }
                for snapshot_id in snap_ids:
                    args["SnapshotId"] = snapshot_id

                    try:
                        self.ec2_client.modify_snapshot_attribute_with_retries(**args)
                        self._logger_.info(INFO_SETTING_CREATE_VOLUME_PERMISSIONS, ", ".join(self.accounts_with_create_permissions))
                        self.result["create-volume-access-accounts"] = [a.strip() for a in self.accounts_with_create_permissions]
                    except Exception as ex:
                        raise_exception(ERR_SETTING_CREATE_VOLUME_PERMISSIONS, self.accounts_with_create_permissions, ex)

        def tag_shared_snapshots(snapshot_data, snap_ids):
            """
            Creates a list of snapshots.

            Args:
                snapshot_data: (dict): write your description
                snap_ids: (str): write your description
            """
            if self.accounts_with_create_permissions not in ["", None] and self.tag_shared_snapshots:

                for account in self.accounts_with_create_permissions:

                    session_for_tagging = self.get_action_session(account=account,
                                                                  param_name=PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME,
                                                                  logger=self._logger_)

                    if session_for_tagging is None:
                        self._logger_.error(ERR_TAGS_NOT_SET_IN_ACCOUNT, account)
                        continue

                    try:
                        ec2_client = get_client_with_retries(service_name="ec2",
                                                             methods=[
                                                                 "create_tags",
                                                                 "delete_tags"
                                                             ],
                                                             context=self._context_,
                                                             region=self._region_,
                                                             session=session_for_tagging,
                                                             logger=self._logger_)
                        for snap_id in snap_ids:
                            tags = snapshot_data.get(snap_id, {}).get("tags", None)
                            if tags is not None:
                                self._logger_.info(INFO_SET_SNAPSHOT_TAGS_SHARED, safe_json(tags, indent=3), snap_id, account,
                                                   self._region_)
                                tagging.set_ec2_tags(ec2_client=ec2_client,
                                                     resource_ids=[snap_id],
                                                     tags=tags,
                                                     logger=self._logger_)
                    except Exception as ex:
                        raise Exception(ERR_SETTING_SHARED_TAGS.format(account, str(ex)))

        def set_volume_tags(volume_id, snap_id):
            """
            Sets the volume

            Args:
                volume_id: (str): write your description
                snap_id: (str): write your description
            """
            tags = self.build_tags_from_template(parameter_name=PARAM_VOLUME_TAGS,
                                                 tag_variables={
                                                     TAG_PLACEHOLDER_VOLUME_SNAPSHOT: snap_id
                                                 })

            if len(tags) > 0:

                try:
                    tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                         resource_ids=[volume_id],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.info(INFO_SET_VOLUME_TAGS, safe_json(tags, indent=3), volume_id)
                except Exception as ex:
                    raise Exception(ERR_SETTING_VOLUME_TAGS.format(self.instance_id, ex))

        def set_instance_tags(snap_ids):
            """
            Sets the instance tags

            Args:
                snap_ids: (str): write your description
            """
            tags = self.build_tags_from_template(parameter_name=PARAM_INSTANCE_TAGS,
                                                 tag_variables={
                                                     TAG_PLACEHOLDER_INSTANCE_SNAPSHOTS: ','.join(sorted(snap_ids))
                                                 })
            if len(tags) > 0:
                try:
                    self.set_ec2_instance_tags_with_event_loop_check(instance_ids=[self.instance_id],
                                                                     tags_to_set=tags,
                                                                     client=self.ec2_client,
                                                                     region=self._region_)

                    self._logger_.info(INFO_SET_INSTANCE_TAGS, safe_json(tags, indent=3), self.instance_id)
                except Exception as ex:
                    raise Exception(ERR_SETTING_INSTANCE_TAGS.format(self.instance_id, ex))

        snapshot_ids = [volume.get("create_snapshot", {}).get("SnapshotId") for volume in
                        list(snapshot_create_data.get("volumes", {}).values())]

        self._logger_.info(INFO_CHECKING_SNAPSHOT_STATUS, ",".join(snapshot_ids))

        if len(snapshot_ids) == 0:
            return {
                "InstanceId": snapshot_create_data["instance"],
                "Volumes": []
            }

        # create service instance to test is snapshots are available
        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        # test if the snapshot with the ids that were returned from the CreateSnapshot API call exists and are completed
        snapshots = list(ec2.describe(services.ec2_service.SNAPSHOTS,
                                      OwnerIds=["self"],
                                      region=self.instance["Region"],
                                      Filters=[
                                          {
                                              "Name": "snapshot-id", "Values": snapshot_ids
                                          }
                                      ]))

        if len(snapshots) != len(snapshot_ids):
            # allow 5 minutes to all snapshots to appear
            start_time = dateutil.parser.parse(snapshot_create_data["start-time"])
            if self._datetime_.now() - start_time < timedelta(minutes=5):
                self._logger_.info(INFO_NOT_ALL_IN_PROGRESS)
                return None

        test_result = {
            "InstanceId": snapshot_create_data["instance"],
            "Volumes": [{
                "VolumeId": s["VolumeId"],
                "SnapshotId": s["SnapshotId"],
                "State": s["State"],
                "Progress": s["Progress"]
            } for s in snapshots]
        }

        self._logger_.info(INFO_STATE_SNAPSHOTS, safe_json(test_result, indent=3))

        # wait until all snapshot are no longer pending
        for volume in test_result["Volumes"]:
            if volume["State"] == SNAPSHOT_STATE_PENDING:
                self._logger_.info(INFO_CREATION_PENDING)
                return None

        # collect possible failed snapshots
        failed = []
        for volume in test_result["Volumes"]:
            if volume["State"] == SNAPSHOT_STATE_ERROR:
                failed.append(volume)

        if len(failed) > 0:
            s = ",".join([ERR_FAILED_SNAPSHOT.format(volume["SnapshotId"], volume["VolumeId"]) for volume in failed])
            raise Exception(s)

        if len(snapshot_ids) != len(snapshots):
            created_snapshots = [s["SnapshotId"] for s in snapshots]
            raise Exception(ERR_MISSING_SNAPSHOTS.format(",".join([s for s in snapshot_ids if s not in created_snapshots])))

        snapshot_ids = [s["SnapshotId"] for s in snapshots]
        # set tags on source instance
        set_instance_tags(snapshot_ids)

        for s in snapshots:
            set_volume_tags(volume_id=s["VolumeId"], snap_id=s["SnapshotId"])

        # set permissions to create volumes from snapshots
        grant_create_volume_permissions(snapshot_ids)
        # tag resources in accounts the snapshots are shared with
        tag_shared_snapshots(snapshot_create_data.get("snapshots", {}), snapshot_ids)
        self._logger_.info(INFO_COMPLETED)
        return test_result

    def execute(self):
        """
        Executes a single ebs volume

        Args:
            self: (todo): write your description
        """

        def volume_has_active_snapshots(ec2_service, vol_id):
            """
            Returns true if a list of the ebs

            Args:
                ec2_service: (todo): write your description
                vol_id: (str): write your description
            """

            # test if the snapshot with the ids that were returned from the CreateSnapshot API call exists and are completed
            volume_snapshots = list(
                ec2_service.describe(services.ec2_service.SNAPSHOTS,
                                     OwnerIds=["self"],
                                     region=self.instance["Region"],
                                     Filters=[
                                         {
                                             "Name": "volume-id", "Values": [vol_id]
                                         }
                                     ]))

            active = [s["SnapshotId"] for s in volume_snapshots if s.get("State", "") == "pending"]

            if len(active) > 0:
                self._logger_.info(INFO_PENDING_SNAPSHOTS, vol_id, ",".join(active))
                return True

            return False

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INFO_START_SNAPSHOT_ACTION, self.instance_id, self._account_, self._region_, self._task_)
        self._logger_.debug("Instance block device mappings are {}", self.instance["BlockDeviceMappings"])

        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        if self.volume_tag_filter is not None:
            volume_data = ec2.describe(services.ec2_service.VOLUMES,
                                       VolumeIds=list(self.volumes.keys()),
                                       tags=True,
                                       region=self._region_)
            volume_tags = {k["VolumeId"]: k.get("Tags", {}) for k in list(volume_data)}
        else:
            volume_tags = {}

        if self.backup_root_device:
            if self.root_volume is None:
                self._logger_.warning(WARN_ROOT_NOT_FOUND, self.instance_id, ",".join(self.volumes))
            else:
                if self.volume_tag_filter is None or self.volume_tag_filter.is_match(volume_tags.get(self.root_volume, {})):
                    if volume_has_active_snapshots(ec2, self.root_volume):
                        self._logger_.error(ERR_SNAPSHOT_PENDING, self.root_volume)
                    else:
                        self.create_volume_snapshot(self.root_volume)
                else:
                    self._logger_.info(INF_SKIP_VOLUME_TAG_FILTER, self.root_volume, volume_tags.get(self.root_volume, {}))

        if self.backup_data_devices:
            for volume in [v for v in self.volumes if v != self.root_volume]:
                if self.volume_tag_filter is None or self.volume_tag_filter.is_match(volume_tags.get(volume, {})):
                    if volume_has_active_snapshots(ec2, volume):
                        self._logger_.error(ERR_SNAPSHOT_PENDING, volume)
                    else:
                        self.create_volume_snapshot(volume)
                else:
                    self._logger_.info(INF_SKIP_VOLUME_TAG_FILTER, volume, volume_tags.get(volume, {}))

        self.result["start-time"] = self._datetime_.now().isoformat()

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CreatedSnapshots=len(list(self.result.get("volumes", {}).values())),
            SnapshotsSizeTotal=sum(
                [volume.get("create_snapshot", {}).get("VolumeSize") for volume in
                 list(self.result.get("volumes", {}).values())]))

        return self.result
