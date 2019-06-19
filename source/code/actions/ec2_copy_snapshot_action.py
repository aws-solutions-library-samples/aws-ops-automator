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
import re
import time
import uuid

import actions
import handlers.ebs_snapshot_event_handler
import handlers.ec2_tag_event_handler
import services
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from handlers import TASK_ACCOUNTS, TASK_THIS_ACCOUNT
from helpers import safe_json
from outputs import raise_exception, raise_value_error
from tagging import tag_key_value_list
from tagging.tag_filter_set import TagFilterSet

TAG_COPIED_BY_TASK = "copied-by-task"
TAG_REGION = "region"
TAG_COPY_SNAPSHOT_ID = "copy-snapshot-id"
COPY_SERIAL_NUMBER = "copy-serial-number"

SNAPSHOT_STATE_ERROR = "error"
SNAPSHOT_STATE_PENDING = "pending"
SNAPSHOT_STATE_COMPLETED = "completed"

KMS_KEY_ID_PATTERN = r"arn:aws:kms:(.)*:key\/([0-9,a-f]){8}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){4}-([0-9,a-f]){12}"

COPIED_SNAPSHOTS_BOTH = "Both"
COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT = "SharedToAccount"
COPIED_OWNED_BY_ACCOUNT = "OwnedByAccount"

TAG_PLACEHOLDER_SOURCE_REGION = "source-region"
TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID = "source-snapshot-id"
TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID = "copy-snapshot-id"
TAG_PLACEHOLDER_COPIED_REGION = "destination-region"
TAG_PLACEHOLDER_OWNER_ACCOUNT = "owner-account"
TAG_PLACEHOLDER_SOURCE_DESCRIPTION = "source-description"
TAG_PLACEHOLDER_SOURCE_VOLUME = "source-volume-id"

MARKER_TAG_SOURCE_SNAPSHOT_ID_TEMPLATE = "OpsAutomator:{}-Ec2CopySnapshot-CopiedFromSnapshot"
MARKER_TAG_COPIED_TO_TEMPLATE = "OpsAutomator:{}-{}-Ec2CopySnapshot-CopyStatus"

GROUP_LABEL_SNAPSHOT_COPY_OPTIONS = "Snapshot copy options"
GROUP_LABEL_ENCRYPTION_AND_PERMISSIONS = "Permissions and encryption"
GROUP_LABEL_SNAPSHOT_SHARING = "Snapshot sharing options"
GROUP_LABEL_SOURCE_SNAPSHOT_TAGGING = "Source snapshot tagging"
GROUP_LABEL_TAGGING_OPTIONS = "Snapshot copy tagging"

PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "CreateVolumePermission"
PARAM_COPIED_SNAPSHOT_TAGS = "CopiedSnapshotTags"
PARAM_COPIED_SNAPSHOTS = "SourceSnapshotTypes"
PARAM_COPY_FROM_OWNER_ACCOUNTS = "CopyFromOwningAccounts"
PARAM_DELETE_AFTER_COPY = "DeleteSourceAfterCopy"
PARAM_DESTINATION_ACCOUNT_TAG_ROLENAME = "TagInSharedAccountsRoleName"
PARAM_DESTINATION_REGION = "DestinationRegion"
PARAM_ENCRYPTED = "Encrypted"
PARAM_KMS_KEY_ID = "KmsKeyId"
PARAM_SNAPSHOT_DESCRIPTION = "SnapshotDescription"
PARAM_SNAPSHOT_TAGS = "SnapshotTags"
PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME = "TagOwnerAccountRoleName"
PARAM_SOURCE_SHARED_BY_TAGS = "OwnerAccountSourceSnapshotTags"
PARAM_SOURCE_TAGS = "SourceSnapshotTags"
PARAM_TAG_IN_DESTINATION_ACCOUNT = "TagSharedSnapshots"
PARAM_TAG_IN_SOURCE_ACCOUNT = "TagOwnerAccountSnapshots"

PARAM_LABEL_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = "Accounts with create volume permissions"
PARAM_LABEL_COPIED_SNAPSHOT_TAGS = "Copied tags from source snapshot"
PARAM_LABEL_COPIED_SNAPSHOTS = "Snapshots types copied"
PARAM_LABEL_COPY_FROM_OWNER_ACCOUNTS = "Copy shared snapshots owned by accounts"
PARAM_LABEL_DESTINATION_ACCOUNT_TAG_ROLENAME = "Name of cross account role for tagging in accounts with restore permissions."
PARAM_LABEL_DESTINATION_REGION = "Destination region"
PARAM_LABEL_ENCRYPTED = "Encrypted"
PARAM_LABEL_KMS_KEY_ID = "KMS Key Id"
PARAM_LABEL_SNAPSHOT_DESCRIPTION = "Snapshot copy description"
PARAM_LABEL_SNAPSHOT_TAGS = "Snapshot tags"
PARAM_LABEL_SOURCE_TAGS = "Source snapshot tags"
PARAM_LABEL_SOURCE_SHARED_BY_TAGS = "Source snapshot tags in account sharing the copied snapshot"
PARAM_LABEL_DELETE_AFTER_COPY = "Delete source snapshot"
PARAM_LABEL_SOURCE_ACCOUNT_TAG_ROLENAME = "Cross account roles for tagging source snapshots in account sharing the snapshot"
PARAM_LABEL_TAG_IN_SOURCE_ACCOUNT = "Create tags for shared source snapshot in owning account"
PARAM_LABEL_TAG_IN_DESTINATION_ACCOUNT = "Create tags for shared snapshots"

PARAM_DESC_COPIED_SNAPSHOT_TAGS = \
    "Copied tags from source snapshot"
PARAM_DESC_SNAPSHOT_DESCRIPTION = \
    "Description for copied snapshot"
PARAM_DESC_DESTINATION_REGION = \
    "Destination region for copied snapshot"
PARAM_DESC_COPIED_SNAPSHOTS = \
    "Select which snapshots are copied. Snapshots owned by the account, shared with the account or both."
PARAM_DESC_SNAPSHOT_TAGS = \
    "Tags to add to copied snapshot. If snapshots are shared with other accounts then the tags can also be created in these " \
    "accounts for the shared snapshot if the \"{}\" is set".format(PARAM_LABEL_TAG_IN_DESTINATION_ACCOUNT)
PARAM_DESC_ACCOUNTS_VOLUME_CREATE_PERMISSIONS = \
    "List of account that will be granted access to create volumes from the copied snapshot."
PARAM_DESC_DESTINATION_ACCOUNT_TAG_ROLENAME = \
    "Name of the cross account role in the accounts the snapshot is shared with, that is used to create tags in these accounts " \
    "for the shared snapshot. Leave this parameter empty to use the default role with name \"OpsAutomatorActionsRole\" if it " \
    "exists or \"{}\" for this account. The role must give permissions to use the Ec2SetTags " \
    "action.".format(handlers.default_rolename_for_stack())
PARAM_DESC_KMS_KEY_ID = \
    "The full ARN of the AWS Key Management Service (AWS KMS) CMK to use when creating the snapshot copy. This parameter is only " \
    "required if you want to use a non-default CMK; if this parameter is not specified, the default CMK for EBS is used. " \
    "The ARN contains the arn:aws:kms namespace, followed by the region of the CMK, the AWS account ID of the CMK owner, " \
    "the key namespace, and then the CMK ID. The specified CMK must exist in the region that the snapshot is being copied to. " \
    "The account or the role that is used by the Ops Automator, or the cross account role must have been given  permission to " \
    "use the key."
PARAM_DESC_COPY_FROM_OWNER_ACCOUNTS = \
    "Comma separated list of accounts to copy shared snapshots from. Leave blank to copy shared snapshots from all accounts"
PARAM_DESC_ENCRYPTED = \
    "Specifies whether the destination snapshot should be encrypted."
PARAM_DESC_SOURCE_TAGS = \
    "Tags to create for source snapshot after a successful copy. These tags are created in the account running the task for the " \
    "(shared) source snapshots. To create tags for shared snapshots, in these are shared from, use " \
    "the \"{}\" and \"{}\" parameters".format(PARAM_TAG_IN_SOURCE_ACCOUNT, PARAM_SOURCE_TAGS)
PARAM_DESC_SOURCE_SHARED_BY_TAGS = \
    "Tags created for the shared source snapshot in the account the snapshot owning the snapshot after a successful copy."
PARAM_DESC_DELETE_AFTER_COPY = \
    "Delete the source snapshot after a successful copy. Only owned source snapshots can be deleted. " \
    "To delete a share snapshot after it has been copied, use the OwnerAccountSourceSnapshotTags and TagOwnerAccountRoleName " \
    "parameters to create tags in the source account for the shared snapshot. The configure an Ec2RemoveSnapshot task for that " \
    "account that selects the snapshots to be deleted selected by these tags."
PARAM_DESC_SOURCE_ACCOUNT_TAG_ROLENAME = \
    "Name of the cross account role in the accounts that own a shared snapshot, that is used to create tags in these accounts." \
    " Leave this parameter empty to use the default role with name \"OpsAutomatorActionsRole\" if it exists or \"{}\" for this " \
    "account. The role must give permissions to use the Ec2SetTags action.".format(handlers.default_rolename_for_stack())
PARAM_DESC_TAG_IN_SOURCE_ACCOUNT = \
    "When enabled snapshots, tags will be created for the copied shared snapshot, in the account that owns the shared snapshot."
PARAM_DESC_TAG_IN_DESTINATION_ACCOUNT = \
    "When enabled snapshots, tags will be created for the copied shared snapshots, in the accounts that the copied " \
    "snapshots are shared with."

DEBUG_ONLY_COPY_OWNED_SNAPSHOTS = "Snapshot {} is owned by account {}, because option {} is set to only copy snapshots " \
                                  "owned by account {} it is not selected"
DEBUG_ONLY_COPY_SHARED_SNAPSHOTS = "Snapshot {} is owned by account {}, because option {} is set to only copy snapshots " \
                                   "shared to account {} it is not selected"
DEBUG_SHARED_SNAPSHOT_OWNER_NOT_IN_LIST = "Snapshot {} shared by account {} is not copied as it is not in the list of accounts" \
                                          " to copy snapshots from {}"
DEBUG_SNAPSHOT_ALREADY_COPIED = "Snapshot {} not selected as it already has been {} or is being copied as snapshot in " \
                                "destination region {},"

INF_ACCOUNT_SNAPSHOT = "Copying snapshot {} from account {} from region {} to region {}"
INF_CHECK_COMPLETED_RESULT = "Snapshot copy completion check result is {}"
INF_COPY_COMPLETED = "Snapshot {} from region {} copied to snapshot {} in region {}"
INF_COPY_PENDING = "Snapshot with id {} does not exist or is pending in region {}"
INF_CREATE_COPIED_TAGS = "Creating tags {} for copied snapshot in account {}"
INF_CREATE_SOURCE_TAGS = "Creating tags {} for source snapshot in account {}"
INF_SETTING_CREATE_VOLUME_PERMISSIONS = "Setting create volume permissions for {}"
INF_SNAPSHOT_COPIED = "Copy of  snapshot {} to region {} snapshot {} started"
INF_TAGS_CREATED = "Tags created for copied snapshots"
INF_NO_ROLE_TO_SET_TAG = "No matching role for account {} in {} to set tags on shared snapshot"
INF_USING_OWN_ROLE_TO_SET_TAGS = "Using Ops Automator Role to set tags on shared snapshots for account {}"
INF_USING_ROLE_TO_SET_TAGS = "Using role {} to tag shared snapshots in account {}"
INF_SNAPSHOT_DELETED = "Snapshot {} deleted in source region {}"
INF_DELETING_SNAPSHOT = "Deleting source snapshot {}"
INF_CREATE_SHARED_TAGS = "Creating tags {} for shared snapshot in account {}"
INF_COMPLETED_NOT_LONGER_AVAILABLE = "Source snapshot {} was not longer available for copying"
INF_COPIED_BY_OTHER = "Snapshot already copied, or being copied, to region {} by this task, no new copy will" \
                      " be started, copy-serial-number is {}"
INF_COMPLETE_ALREADY_COPIED = "Snapshot {} already copied, or being copied."
INF_WARNING_NO_SNAPSHOT = "Source snapshot {} was not longer available"
INF_CREATE_SHARED_ACCOUNT_SNAPSHOT_TAGS = "Creating tags {} for shared snapshot {} in account {}"

ERR_ACCOUNTS_BUT_NOT_SHARED = "Parameter {} can only be used if {} parameter has been set to copy shared snapshots"
ERR_INVALID_DESTINATION_REGION = "{} is not a valid region, valid regions are: "
ERR_INVALID_KMS_ID_ARN = "{} is not a valid KMS Id ARN"
ERR_KMS_KEY_NOT_IN_REGION = "KMS key with id {} is not available in destination region {}"
ERR_KMS_KEY_ONLY_IF_ENCRYPTED = "{} parameter can only be used if encryption is enabled"
ERR_SETTING_CREATE_VOLUME_PERMISSIONS = "Error setting create volume permissions for account(s) {}, {}"
ERR_COPY_SNAPSHOT = "Error copying snapshot"
ERR_SETTING_SHARED_TAGS = "Can not set tags for copied shared snapshots in account {}, {}"
ERR_CANNOT_DELETE_SHARED_SNAPSHOTS = "Delete after copy option can be used for tasks that copy owned snapshots only"
ERR_SETTING_SOURCE_SHARED_TAGS = "Error setting tags in source account {} for copied shared snapshot"
ERR_TAGS_NOT_SET_IN_ACCOUNT = "Tags not set in account {}"

WARN_SETTING_COPIED_TAG = "Error setting copied tag to instance {}, ({})"


class Ec2CopySnapshotAction(ActionBase):
    """
    Class implements action for copying EC2 Snapshots
    """

    properties = {
        ACTION_TITLE: "EC2 Copy Snapshot",
        ACTION_VERSION: "1.3",
        ACTION_DESCRIPTION: "Copies EC2 snapshot",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "eb287af5-e5c0-41cb-832b-d218c075fa26",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_RESOURCE,

        ACTION_COMPLETION_TIMEOUT_MINUTES: 60,

        ACTION_MIN_INTERVAL_MIN: 60,

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_SELECT_EXPRESSION:
            "Snapshots[?State=='completed'].{SnapshotId:SnapshotId, "
            "VolumeId:VolumeId, OwnerId:OwnerId, "
            "StartTime:StartTime,"
            "Description:Description, "
            "Tags:Tags}",

        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: {'RestorableByUserIds': ["self"], },

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_NOTIFICATION: [
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_CREATED,
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_SHARED]
            }
        },

        # Ec2 CopySnapshot only allows 5 concurrent copies per account to a destination region
        ACTION_MAX_CONCURRENCY: int(os.getenv(handlers.ENV_SERVICE_LIMIT_CONCURRENT_EBS_SNAPSHOT_COPY, 5)),

        ACTION_PARAMETERS: {
            PARAM_DESTINATION_REGION: {
                PARAM_DESCRIPTION: PARAM_DESC_DESTINATION_REGION,
                PARAM_LABEL: PARAM_LABEL_DESTINATION_REGION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: services.get_session().region_name,
                PARAM_ALLOWED_VALUES: [str(r) for r in services.get_session().get_available_regions("ec2", "aws")]
            },
            PARAM_SNAPSHOT_DESCRIPTION: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_DESCRIPTION,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_DESCRIPTION,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_COPIED_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_COPIED_SNAPSHOT_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
            },
            PARAM_SNAPSHOT_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SNAPSHOT_TAGS,
                PARAM_LABEL: PARAM_LABEL_SNAPSHOT_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_COPIED_SNAPSHOTS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPIED_SNAPSHOTS,
                PARAM_LABEL: PARAM_LABEL_COPIED_SNAPSHOTS,
                PARAM_TYPE: str,
                PARAM_ALLOWED_VALUES: [COPIED_OWNED_BY_ACCOUNT, COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT, COPIED_SNAPSHOTS_BOTH],
                PARAM_DEFAULT: COPIED_OWNED_BY_ACCOUNT,
                PARAM_REQUIRED: False
            },
            PARAM_SOURCE_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SOURCE_TAGS,
                PARAM_LABEL: PARAM_LABEL_SOURCE_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },

            PARAM_DELETE_AFTER_COPY: {
                PARAM_DESCRIPTION: PARAM_DESC_DELETE_AFTER_COPY,
                PARAM_LABEL: PARAM_LABEL_DELETE_AFTER_COPY,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: False,
                PARAM_REQUIRED: False
            },
            PARAM_ENCRYPTED: {
                PARAM_DESCRIPTION: PARAM_DESC_ENCRYPTED,
                PARAM_LABEL: PARAM_LABEL_ENCRYPTED,
                PARAM_TYPE: bool,
                PARAM_DEFAULT: False,
                PARAM_REQUIRED: False
            },
            PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS: {
                PARAM_DESCRIPTION: PARAM_DESC_ACCOUNTS_VOLUME_CREATE_PERMISSIONS,
                PARAM_TYPE: list,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_ACCOUNTS_VOLUME_CREATE_PERMISSIONS
            },
            PARAM_COPY_FROM_OWNER_ACCOUNTS: {
                PARAM_DESCRIPTION: PARAM_DESC_COPY_FROM_OWNER_ACCOUNTS,
                PARAM_LABEL: PARAM_LABEL_COPY_FROM_OWNER_ACCOUNTS,
                PARAM_TYPE: list,
                PARAM_REQUIRED: False
            },

            PARAM_TAG_IN_DESTINATION_ACCOUNT: {
                PARAM_DESCRIPTION: PARAM_DESC_TAG_IN_DESTINATION_ACCOUNT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_TAG_IN_DESTINATION_ACCOUNT
            },
            PARAM_DESTINATION_ACCOUNT_TAG_ROLENAME: {
                PARAM_DESCRIPTION: PARAM_DESC_DESTINATION_ACCOUNT_TAG_ROLENAME,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_DESTINATION_ACCOUNT_TAG_ROLENAME
            },
            PARAM_TAG_IN_SOURCE_ACCOUNT: {
                PARAM_DESCRIPTION: PARAM_DESC_TAG_IN_SOURCE_ACCOUNT,
                PARAM_TYPE: bool,
                PARAM_REQUIRED: False,
                PARAM_DEFAULT: False,
                PARAM_LABEL: PARAM_LABEL_TAG_IN_SOURCE_ACCOUNT
            },
            PARAM_SOURCE_SHARED_BY_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_SOURCE_SHARED_BY_TAGS,
                PARAM_LABEL: PARAM_LABEL_SOURCE_SHARED_BY_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            },
            PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME: {
                PARAM_DESCRIPTION: PARAM_DESC_SOURCE_ACCOUNT_TAG_ROLENAME,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_SOURCE_ACCOUNT_TAG_ROLENAME
            },

            PARAM_KMS_KEY_ID: {
                PARAM_DESCRIPTION: PARAM_DESC_KMS_KEY_ID,
                PARAM_LABEL: PARAM_LABEL_KMS_KEY_ID,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_SNAPSHOT_COPY_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_DESTINATION_REGION,
                    PARAM_COPIED_SNAPSHOTS,
                    PARAM_COPY_FROM_OWNER_ACCOUNTS,
                    PARAM_SNAPSHOT_DESCRIPTION,
                    PARAM_DELETE_AFTER_COPY
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_TAGGING_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_COPIED_SNAPSHOT_TAGS,
                    PARAM_SNAPSHOT_TAGS,
                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_SOURCE_SNAPSHOT_TAGGING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_SOURCE_TAGS,
                    PARAM_TAG_IN_SOURCE_ACCOUNT,
                    PARAM_SOURCE_SHARED_BY_TAGS,
                    PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME,

                ],
            },
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_SNAPSHOT_SHARING,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS,
                    PARAM_TAG_IN_DESTINATION_ACCOUNT,
                    PARAM_DESTINATION_ACCOUNT_TAG_ROLENAME
                ],
            },

            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_LABEL_ENCRYPTION_AND_PERMISSIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_ENCRYPTED,
                    PARAM_KMS_KEY_ID

                ],
            }
        ],

        ACTION_PERMISSIONS: ["ec2:CopySnapshot",
                             "ec2:CreateTags",
                             "ec2:DeleteTags",
                             "ec2:DescribeSnapshots",
                             "ec2:DeleteSnapshot",
                             "ec2:ModifySnapshotAttribute"]

    }

    @staticmethod
    def marker_tag_source_snapshot_id():
        return MARKER_TAG_SOURCE_SNAPSHOT_ID_TEMPLATE.format(os.getenv(handlers.ENV_STACK_NAME))

    @staticmethod
    def marker_tag_copied_to(taskname):
        return MARKER_TAG_COPIED_TO_TEMPLATE.format(os.getenv(handlers.ENV_STACK_NAME), taskname)

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):

        def get_snapshot_tags(client, snap_id):
            try:
                resp = client.describe_snapshots_with_retries(RestorableByUserIds=["self"], SnapshotIds=[snap_id])
                list_of_tags = resp.get("Snapshots", [{}])[0].get("Tags", [])
                return {tag["Key"].strip(): tag.get("Value", "").strip() for tag in list_of_tags}, True

            except Exception as ex:
                if getattr(ex, "response", {}).get("Error", {}).get("Code", "") == "InvalidSnapshot.NotFound":
                    return {}, False
                else:
                    raise ex

        def mark_as_being_selected_for_copy(client, snapshot):
            try:
                tag_name = Ec2CopySnapshotAction.marker_tag_copied_to(task[handlers.TASK_NAME])

                # Serial number for copy. This is stored in the tag of the snapshot anf the stored resources in the task
                # Before starting a copy there will be check if these match to avoid double copied of a snapshot
                copy_serial = str(uuid.uuid4())

                tag_data = {
                    tag_name: safe_json(
                        {
                            TAG_REGION: task.get(handlers.TASK_PARAMETERS, {}).get(PARAM_DESTINATION_REGION, ""),
                            COPY_SERIAL_NUMBER: copy_serial,
                            TAG_COPY_SNAPSHOT_ID: ""
                        })
                }

                client.create_tags_with_retries(Resources=[snapshot["SnapshotId"]],
                                                Tags=tag_key_value_list(tag_data))

                # store the copy serial number as part of the selected resource
                resource[COPY_SERIAL_NUMBER] = copy_serial

            except Exception as ex:
                logger.warning(WARN_SETTING_COPIED_TAG, snapshot["SnapshotId"], ex)

        # source snapshot
        snapshot_id = resource["SnapshotId"]

        # owner of the snapshot
        snapshot_owner = resource["OwnerId"]

        parameters = task.get(handlers.TASK_PARAMETERS, {})

        # copy owned, shared or both
        copied_snapshot_types = parameters[PARAM_COPIED_SNAPSHOTS]

        this_account = task.get(TASK_THIS_ACCOUNT, False)
        accounts = task.get(TASK_ACCOUNTS, [])

        if this_account and len(accounts) == 0:
            account = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)
        elif not this_account and len(accounts) == 1:
            account = accounts[0]
        else:
            account = services.account_from_role_arn(task_assumed_role)

        if copied_snapshot_types == COPIED_OWNED_BY_ACCOUNT and account != snapshot_owner:
            logger.debug(DEBUG_ONLY_COPY_OWNED_SNAPSHOTS, snapshot_id, snapshot_owner, PARAM_COPIED_SNAPSHOTS, account)
            return None

        if copied_snapshot_types == COPIED_SNAPSHOTS_SHARED_TO_ACCOUNT and account == snapshot_owner:
            logger.debug(DEBUG_ONLY_COPY_SHARED_SNAPSHOTS, snapshot_id, snapshot_owner, PARAM_COPIED_SNAPSHOTS, account)
            return None

        copy_from_accounts = parameters.get(PARAM_COPY_FROM_OWNER_ACCOUNTS, None)
        if copy_from_accounts not in [None, []]:

            if copied_snapshot_types == COPIED_OWNED_BY_ACCOUNT:
                raise_value_error(ERR_ACCOUNTS_BUT_NOT_SHARED, PARAM_COPY_FROM_OWNER_ACCOUNTS, PARAM_COPIED_SNAPSHOTS)
            if snapshot_owner != account and snapshot_owner not in [a.strip() for a in copy_from_accounts]:
                logger.debug(DEBUG_SHARED_SNAPSHOT_OWNER_NOT_IN_LIST, snapshot_id, snapshot_owner, ",".join(copy_from_accounts))
                return None

        # name of tag that is used to mark snapshots being copied
        copied_tag_name = Ec2CopySnapshotAction.marker_tag_copied_to(task[handlers.TASK_NAME])

        if copied_tag_name in resource.get("Tags", {}):
            # noinspection PyBroadException
            try:
                logger.debug("Snapshot already copied or being copied, copy data is:\n  {}",
                             safe_json(json.loads(resource.get("Tags", {}).get(copied_tag_name, {}))))
            except Exception:
                pass
            return None

        # ec2 client for getting most current tag values and setting tags
        ec2 = get_client_with_retries(service_name="ec2", methods=["create_tags", "describe_snapshots"],
                                      region=resource["Region"],
                                      context=context,
                                      session=service.session,
                                      logger=logger)

        # get the most current tags as they might be changed by overlapping copy tasks
        tags, snapshot_found = get_snapshot_tags(ec2, snapshot_id)

        # snapshot no longer there
        if not snapshot_found:
            logger.debug("Snapshot {} not longer available", snapshot_id)
            return None

        if copied_tag_name in tags:
            return None

        mark_as_being_selected_for_copy(ec2, resource)
        return resource

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):

        valid_regions = services.get_session().get_available_regions("ec2", "aws")
        region = parameters.get(PARAM_DESTINATION_REGION)
        if region not in valid_regions:
            raise_value_error(ERR_INVALID_DESTINATION_REGION, region, ",".join(valid_regions))

        if parameters.get(PARAM_DELETE_AFTER_COPY, False) and parameters.get(PARAM_COPIED_SNAPSHOTS) != COPIED_OWNED_BY_ACCOUNT:
            raise_value_error(ERR_CANNOT_DELETE_SHARED_SNAPSHOTS)

        kms_key_id = parameters.get(PARAM_KMS_KEY_ID, None)
        if not parameters[PARAM_ENCRYPTED] and kms_key_id not in ["", None]:
            raise_value_error(ERR_KMS_KEY_ONLY_IF_ENCRYPTED, PARAM_KMS_KEY_ID)

        if kms_key_id not in ["", None]:
            if re.match(KMS_KEY_ID_PATTERN, kms_key_id) is None:
                raise_value_error(ERR_INVALID_KMS_ID_ARN, kms_key_id)

            destination_region = parameters[PARAM_DESTINATION_REGION]
            if kms_key_id.split(":")[3] != destination_region:
                raise_value_error(ERR_KMS_KEY_NOT_IN_REGION, kms_key_id, destination_region)

        return parameters

    @staticmethod
    def action_logging_subject(arguments, _):
        snapshot = arguments[ACTION_PARAM_RESOURCES]
        account = snapshot["AwsAccount"]
        snapshot_id = snapshot["SnapshotId"]
        region = snapshot["Region"]
        return "{}-{}-{}-{}".format(account, region, snapshot_id, log_stream_date())

    @staticmethod
    def action_concurrency_key(arguments):
        # copies per account/destination
        return "ec2:CopySnapshot:{}:{}".format(arguments[ACTION_PARAM_ACCOUNT], arguments[PARAM_DESTINATION_REGION])

    @property
    def ec2_destination_client(self):
        if self._ec2_destination_client is None:
            methods = ["copy_snapshot",
                       "create_tags",
                       "delete_tags",
                       "modify_snapshot_attribute"]

            self._ec2_destination_client = get_client_with_retries("ec2",
                                                                   methods=methods,
                                                                   region=self._destination_region_,
                                                                   context=self._context_,
                                                                   session=self._session_,
                                                                   logger=self._logger_)
        return self._ec2_destination_client

    @property
    def ec2_source_client(self):
        if self._ec2_source_client is None:
            methods = [
                "create_tags",
                "delete_tags",
                "delete_snapshot"
            ]

            self._ec2_source_client = get_client_with_retries("ec2",
                                                              methods=methods,
                                                              region=self.source_region,
                                                              context=self._context_,
                                                              session=self._session_,
                                                              logger=self._logger_)
        return self._ec2_source_client

    def __init__(self, action_args, action_parameters):
        self._destination_region_ = None

        ActionBase.__init__(self, action_args, action_parameters)

        # debug and dryrun
        self.snapshot = self._resources_

        # snapshot source and destination information
        self.source_snapshot_id = self.snapshot["SnapshotId"]
        self.source_region = self.snapshot["Region"]
        self.owner = self.snapshot.get("OwnerId", "")

        self.encrypted = self.get(PARAM_ENCRYPTED, False)
        self.kms_key_id = self.get(PARAM_KMS_KEY_ID, None)
        self.accounts_with_create_permissions = self.get(PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS, [])

        self.delete_after_copy = self.get(PARAM_DELETE_AFTER_COPY, False)

        # filter for copied tags from source snapshot
        self.copied_volume_tagfiter = TagFilterSet(self.get(PARAM_COPIED_SNAPSHOT_TAGS, ""))

        self.tag_snapshots_in_shared_accounts = self.get(PARAM_TAG_IN_DESTINATION_ACCOUNT, False)
        self.tag_snapshots_in_source_account = self.get(PARAM_TAG_IN_SOURCE_ACCOUNT, False)

        # tagging roles
        self.dest_account_snapshot_tagging_rolename = self.get(PARAM_DESTINATION_ACCOUNT_TAG_ROLENAME, "")
        self.source_account_tagging_role_name = self.get(PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME, "")

        volume_id = self.snapshot["VolumeId"]
        if volume_id == DUMMY_VOLUME_IF_FOR_COPIED_SNAPSHOT:
            volume_from_tag = self.snapshot.get("Tags", {}).get(actions.marker_snapshot_tag_source_source_volume_id(), None)
            if volume_from_tag is not None:
                volume_id = volume_from_tag
        self.source_volume_id = volume_id

        self._ec2_destination_client = None
        self._ec2_source_client = None

        # setup result with known values
        self.result = {
            "account": self._account_,
            "task": self._task_,
            "destination-region": self._destination_region_,
            "source-region": self.source_region,
            "source-snapshot-id": self.source_snapshot_id,
            "encrypted": self.encrypted,
            "kms-id": self.kms_key_id if self.kms_key_id is not None else ""
        }

    def is_completed(self, snapshot_create_data):
        def delete_source_after_copy():
            self._logger_.info(INF_DELETING_SNAPSHOT, self.source_snapshot_id)
            self.ec2_source_client.delete_snapshot_with_retries(SnapshotId=self.source_snapshot_id)
            self._logger_.info(INF_SNAPSHOT_DELETED, self.source_snapshot_id, self.source_region)

        def source_tags(copy_id, source_tags_param):
            snapshot_tags = {}
            snapshot_tags.update(
                self.build_tags_from_template(parameter_name=source_tags_param,
                                              region=self.source_region,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID: copy_id,
                                                  TAG_PLACEHOLDER_COPIED_REGION: self._destination_region_
                                              }))
            return snapshot_tags

        def set_source_snapshot_tags(copy_id):
            snapshot_tags = source_tags(copy_id, PARAM_SOURCE_TAGS)
            if len(snapshot_tags) == 0:
                return

            self._logger_.info(INF_CREATE_SOURCE_TAGS, snapshot_tags, self._account_)

            if len(snapshot_tags) > 0:
                tagging.set_ec2_tags(ec2_client=self.ec2_source_client,
                                     resource_ids=[self.source_snapshot_id],
                                     tags=snapshot_tags,
                                     logger=self._logger_)

                self._logger_.info(INF_TAGS_CREATED)

        def grant_create_volume_permissions(snap_id):

            if self.accounts_with_create_permissions is not None and len(self.accounts_with_create_permissions) > 0:

                args = {

                    "CreateVolumePermission": {
                        "Add": [{"UserId": a.strip()} for a in self.accounts_with_create_permissions]

                    },
                    "SnapshotId": snap_id
                }

                try:
                    self.ec2_destination_client.modify_snapshot_attribute_with_retries(**args)
                    self._logger_.info(INF_SETTING_CREATE_VOLUME_PERMISSIONS, ", ".join(self.accounts_with_create_permissions))
                except Exception as ex:
                    raise_exception(ERR_SETTING_CREATE_VOLUME_PERMISSIONS, self.accounts_with_create_permissions, ex)

        def tag_shared_snapshots(tags, snap_id):
            # creates tags for snapshots that have been shared in account the snapshots are shared with

            if len(tags) == 0 or not self.tag_snapshots_in_shared_accounts:
                return

            if self.accounts_with_create_permissions in ["", None]:
                return

            for account in self.accounts_with_create_permissions:

                session_for_tagging = self.get_action_session(account=account,
                                                              param_name=PARAM_DESTINATION_ACCOUNT_TAG_ROLENAME,
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
                                                         region=self.get(PARAM_DESTINATION_REGION),
                                                         session=session_for_tagging,
                                                         logger=self._logger_)

                    tagging.set_ec2_tags(ec2_client=ec2_client,
                                         resource_ids=[snap_id],
                                         tags=tags,
                                         logger=self._logger_)

                    self._logger_.info(INF_CREATE_SHARED_TAGS, tags, account)

                except Exception as ex:
                    raise_exception(ERR_SETTING_SHARED_TAGS, account, str(ex))

        def tag_shared_source_snapshot(copy_id):
            # created tags for snapshots for shared snapshots in the source account of the shares snapshots
            snapshot_tags = source_tags(copy_id, PARAM_SOURCE_SHARED_BY_TAGS)
            if len(snapshot_tags) == 0 or not self.tag_snapshots_in_source_account:
                return

            # only for snapshots that have been shared by other account
            if self.owner == self.get_account_for_task():
                self._logger_.debug("Account {} is owner, no tags set for snapshot {} in account of owner", self._account_,
                                    self.source_snapshot_id)
                return

            session_for_tagging = self.get_action_session(account=self.owner,
                                                          param_name=PARAM_SOURCE_ACCOUNT_TAG_ROLE_NAME,
                                                          logger=self._logger_)

            if session_for_tagging is None:
                self._logger_.error(ERR_TAGS_NOT_SET_IN_ACCOUNT, self.owner)
                return

            try:

                self._logger_.info(INF_CREATE_SHARED_ACCOUNT_SNAPSHOT_TAGS, snapshot_tags, self.source_snapshot_id,
                                   self.owner)
                ec2_client = get_client_with_retries(service_name="ec2",
                                                     methods=[
                                                         "create_tags",
                                                         "delete_tags"
                                                     ],
                                                     context=self._context_,
                                                     region=self.source_region,
                                                     session=session_for_tagging,
                                                     logger=self._logger_)

                tagging.set_ec2_tags(ec2_client=ec2_client,
                                     resource_ids=[self.source_snapshot_id],
                                     tags=snapshot_tags,
                                     logger=self._logger_)

            except Exception as ex:
                raise_exception(ERR_SETTING_SOURCE_SHARED_TAGS, self.owner, str(ex))

        if snapshot_create_data.get("already-copied", False):
            self._logger_.info(INF_COMPLETE_ALREADY_COPIED, self.source_snapshot_id)
            return self.result

        if snapshot_create_data.get("not-longer-available", False):
            self._logger_.info(INF_COMPLETED_NOT_LONGER_AVAILABLE, self.source_snapshot_id)
            return self.result

        # create service instance to test if snapshot exists
        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        copy_snapshot_id = snapshot_create_data["copy-snapshot-id"]
        # test if the snapshot with the id that was returned from the CopySnapshot API call exists and is completed
        copied_snapshot = ec2.get(services.ec2_service.SNAPSHOTS,
                                  region=self._destination_region_,
                                  OwnerIds=["self"],
                                  Filters=[
                                      {
                                          "Name": "snapshot-id", "Values": [copy_snapshot_id]
                                      }
                                  ])

        if copied_snapshot is not None:
            self._logger_.debug(INF_CHECK_COMPLETED_RESULT, copied_snapshot)

        state = copied_snapshot["State"] if copied_snapshot is not None else None

        if copied_snapshot is None or state == SNAPSHOT_STATE_PENDING:
            self._logger_.info(INF_COPY_PENDING, copy_snapshot_id, self._destination_region_)
            return None

        if state == SNAPSHOT_STATE_ERROR:
            copied_tag_name = Ec2CopySnapshotAction.marker_tag_copied_to(self._task_)
            self.ec2_source_client.delete_tags_with_retries(Resources=[self.source_snapshot_id],
                                                            Tags=[
                                                                {
                                                                    "Key": copied_tag_name
                                                                }
                                                            ])
            raise_exception(ERR_COPY_SNAPSHOT)

        if state == SNAPSHOT_STATE_COMPLETED:
            self._logger_.info(INF_COPY_COMPLETED, self.source_snapshot_id, self.source_region, copy_snapshot_id,
                               self._destination_region_)
            grant_create_volume_permissions(copy_snapshot_id)
            tag_shared_snapshots(snapshot_create_data.get("tags", {}), copy_snapshot_id)
            tag_shared_source_snapshot(copy_snapshot_id)
            if self.delete_after_copy:
                delete_source_after_copy()
            else:
                set_source_snapshot_tags(copy_snapshot_id)

            # wait there for 15 seconds as count the limit for max number of concurrent snapshot copies
            # by the EC2 service is sometimes delayed
            time.sleep(5)

            return copied_snapshot

        return None

    def execute(self):
        def get_tags_for_copied_snapshot():

            snapshot_tags = (self.copied_volume_tagfiter.pairs_matching_any_filter(self.snapshot.get("Tags", {})))
            snapshot_tags[actions.marker_snapshot_tag_source_source_volume_id()] = self.source_volume_id
            snapshot_tags.update(
                self.build_tags_from_template(parameter_name=PARAM_SNAPSHOT_TAGS,
                                              region=self.source_region,
                                              tag_variables={
                                                  TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID: self.source_snapshot_id,
                                                  TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                                                  TAG_PLACEHOLDER_OWNER_ACCOUNT: self.owner,
                                                  TAG_PLACEHOLDER_SOURCE_VOLUME: self.source_volume_id
                                              }))

            snapshot_tags[Ec2CopySnapshotAction.marker_tag_source_snapshot_id()] = self.source_snapshot_id
            snapshot_tags[actions.marker_snapshot_tag_source_source_volume_id()] = self.source_volume_id

            return snapshot_tags

        def get_source_snapshot():
            ec2 = services.create_service("ec2", session=self._session_,
                                          service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

            snapshot = ec2.get(services.ec2_service.SNAPSHOTS,
                               region=self.source_region,
                               RestorableByUserIds=["self"],
                               Filters=[{"Name": "snapshot-id", "Values": [self.source_snapshot_id]}])
            return snapshot

        def should_copy_snapshot():
            snapshot = get_source_snapshot()

            # source snapshot was already deleted by tasks that were in wait for execution list
            if snapshot is None:
                self.result["not-longer-available"] = True
                self._logger_.info(INF_WARNING_NO_SNAPSHOT, self.source_snapshot_id)
                return False

            # get tags from the snapshot, these must have contain the mark_as_copied tag and this tag must contain the same
            # copy serial number as the snapshot that was in the selected resource for this task instance
            source_snapshot_tags = snapshot.get("Tags", {}) if snapshot is not None else {}
            marked_as_copied_tag = Ec2CopySnapshotAction.marker_tag_copied_to(self._task_)
            if marked_as_copied_tag in source_snapshot_tags:
                snapshot_copy_data = json.loads(source_snapshot_tags[marked_as_copied_tag])
            else:
                snapshot_copy_data = {}
            if snapshot_copy_data.get(COPY_SERIAL_NUMBER, "") != self.snapshot.get(COPY_SERIAL_NUMBER):
                self._logger_.info(INF_COPIED_BY_OTHER, snapshot_copy_data.get(TAG_REGION, ""),
                                   snapshot_copy_data(COPY_SERIAL_NUMBER, ""))
                self.result["already-copied"] = True
                self.result["copied-data"] = snapshot_copy_data
                return False
            return True

        # logged information
        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])
        self._logger_.info(INF_ACCOUNT_SNAPSHOT, self.source_snapshot_id, self._account_, self.source_region,
                           self._destination_region_)
        self._logger_.debug("Snapshot : {}", self.snapshot)

        boto_call = "copy_snapshot"
        try:
            # setup argument for CopySnapshot call
            args = {
                "SourceRegion": self.source_region,
                "SourceSnapshotId": self.source_snapshot_id
            }

            if not should_copy_snapshot():
                return self.result

            if self.encrypted:
                args["Encrypted"] = True
                self.result["encrypted"] = True
                if self.kms_key_id not in ["", None]:
                    args["KmsKeyId"] = self.kms_key_id

            if self._dryrun_:
                args["DryRun"] = True

            source_description = self.snapshot.get("Description", "")

            description_variables = {
                TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID: self.source_snapshot_id,
                TAG_PLACEHOLDER_SOURCE_REGION: self.source_region,
                TAG_PLACEHOLDER_OWNER_ACCOUNT: self.owner,
                TAG_PLACEHOLDER_SOURCE_VOLUME: self.source_volume_id,
                TAG_PLACEHOLDER_SOURCE_DESCRIPTION: source_description
            }

            args["Description"] = self.build_str_from_template(parameter_name=PARAM_SNAPSHOT_DESCRIPTION,
                                                               region=self.source_region,
                                                               tag_variables=description_variables)
            if args["Description"] == "":
                args["Description"] = source_description

            # start the copy
            resp = self.ec2_destination_client.copy_snapshot_with_retries(**args)

            # id of the copy
            copy_snapshot_id = resp.get("SnapshotId")
            self._logger_.info(INF_SNAPSHOT_COPIED, self.source_snapshot_id, self._destination_region_, copy_snapshot_id)
            self.result[boto_call] = resp
            self.result["copy-snapshot-id"] = copy_snapshot_id

            # update the tag that marks the snapshot as being copied
            boto_call = "create_tags (source)"
            copied_tag_name = Ec2CopySnapshotAction.marker_tag_copied_to(self._task_)

            copy_data_tag = {
                copied_tag_name: safe_json(
                    {
                        TAG_REGION: self._destination_region_,
                        COPY_SERIAL_NUMBER: self.snapshot.get(COPY_SERIAL_NUMBER, ""),
                        TAG_COPIED_BY_TASK: self.get(ACTION_PARAM_TASK_ID, ""),
                        TAG_COPY_SNAPSHOT_ID: copy_snapshot_id
                    })
            }

            self.ec2_source_client.create_tags_with_retries(Resources=[self.source_snapshot_id],
                                                            Tags=tag_key_value_list(copy_data_tag))

            # set tags on the copy
            boto_call = "create_tags (target)"
            tags = get_tags_for_copied_snapshot()
            self._logger_.info(INF_CREATE_COPIED_TAGS, tags, self._account_)

            if len(tags) > 0:
                tagging.set_ec2_tags(ec2_client=self.ec2_destination_client,
                                     resource_ids=[copy_snapshot_id],
                                     tags=tags,
                                     logger=self._logger_)

                self.result["tags"] = tags
                self._logger_.info(INF_TAGS_CREATED)

        except Exception as ex:
            if self._dryrun_:
                self._logger_.debug(str(ex))
                self.result[boto_call] = str(ex)
                return self.result
            else:
                raise ex

        self.result[METRICS_DATA] = build_action_metrics(self, CopiedSnapshots=1)

        return self.result
