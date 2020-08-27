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
import inspect
import json
import unittest
from types import FunctionType
import sys

import actions
import actions.ec2_copy_snapshot_action as copy_snapshot
import handlers
import testing.tags
from tagging.tag_filter_expression import TagFilterExpression
from testing.console_logger import ConsoleLogger
from testing.ec2 import Ec2
from testing.stack import Stack
from tests.action_tests import region, remote_region, tasklist_tagname, get_resource_stack, get_task_runner, template_path

TESTED_ACTION = "Ec2CopySnapshot"
TEST_RESOURCES_TEMPLATE = "test_resources.template"
TEST_REMOTE_RESOURCES_TEMPLATE = "test_resources_destination_region.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False


class TestAction(unittest.TestCase):
    logger = None
    resource_stack = None
    task_runner = None
    ec2 = None
    volume_unencrypted = None
    volume_encrypted_default = None
    volume_encrypted_custom = None

    snapshot_unencrypted = None
    snapshot_encrypted_default = None
    snapshot_encrypted_custom = None

    def __init__(self, method_name):
        unittest.TestCase.__init__(self, method_name)
        self.snapshots = []

    @classmethod
    def get_methods(cls):
        return [x for x, y in list(cls.__dict__.items()) if type(y) == FunctionType and x.startswith("test_")]

    @classmethod
    def setUpClass(cls):
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")
            
        cls.logger = ConsoleLogger()

        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK)

        cls.resource_stack = get_resource_stack(TESTED_ACTION,
                                                create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK,
                                                region_name=region())

        assert (cls.resource_stack is not None)

        cls.ec2 = Ec2(region())

        cls.volume_unencrypted = cls.resource_stack.stack_outputs["VolumeIdEncryptedUnencrypted"]
        cls.volume_encrypted_default = cls.resource_stack.stack_outputs["VolumeIdEncryptedDefault"]
        cls.volume_encrypted_custom = cls.resource_stack.stack_outputs["VolumeIdEncryptedCustom"]
        cls.custom_key_arn = cls.resource_stack.stack_outputs["EncryptionKeyArn"]

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            role = cls.task_runner.action_stack.stack_resources[testing.OPS_AUTOMATOR_ROLE_NAME]["PhysicalResourceId"]
            resource_stack = Stack(resource_stack_name, region=region())
            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE),
                                        iam_capability=True,
                                        params={"TaskRole": role})
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    @classmethod
    def create_remote_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test remote resources stack {}", resource_stack_name)
            role = cls.task_runner.action_stack.stack_resources[testing.OPS_AUTOMATOR_ROLE_NAME]["PhysicalResourceId"]
            resource_stack = Stack(resource_stack_name, region=remote_region())
            resource_stack.create_stack(template_file=template_path(__file__, TEST_REMOTE_RESOURCES_TEMPLATE),
                                        iam_capability=True,
                                        params={"TaskRole": role})
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    def check_copy_snapshot(self, snapshot_id, source_snapshot, destination):

        ec2_destination = Ec2(region=destination)
        self.logger.test("Checking copied snapshot")
        snapshot_copy = ec2_destination.get_snapshot(snapshot_id)
        self.assertIsNotNone(snapshot_copy, "Snapshot copy does exist")

        snapshot_tags = snapshot_copy.get("Tags", {})
        self.assertTrue(TagFilterExpression("copied-tag=copied-value").is_match(snapshot_tags), "Source snapshot tag copied")
        self.assertFalse(TagFilterExpression("not-copied-tag=*").is_match(snapshot_tags), "Source snapshot tag not copied")
        self.logger.test("[X] Expected source snapshot tag copied")

        snapshot_placeholders = {
            copy_snapshot.TAG_PLACEHOLDER_SOURCE_REGION: source_snapshot["Region"],
            copy_snapshot.TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID: source_snapshot["SnapshotId"],
            copy_snapshot.TAG_PLACEHOLDER_SOURCE_VOLUME: source_snapshot["VolumeId"],
            copy_snapshot.TAG_PLACEHOLDER_OWNER_ACCOUNT: source_snapshot["AwsAccount"]
        }
        self.assertTrue(testing.tags.verify_placeholder_tags(snapshot_tags, snapshot_placeholders),
                        "All placeholder tags set on snapshot {}".format(snapshot_id))
        self.logger.test("[X] Placeholder tags created")

        self.assertTrue(TagFilterExpression(
            "{}={}".format(actions.marker_snapshot_tag_source_source_volume_id(), source_snapshot["VolumeId"])).is_match(
            snapshot_tags), "Source volume tag set")
        self.logger.test("[X] Source volume tag created")

        self.assertTrue(TagFilterExpression(
            "{}={}".format(copy_snapshot.Ec2CopySnapshotAction.marker_tag_source_snapshot_id(),
                           source_snapshot["SnapshotId"])).is_match(
            snapshot_tags), "Source snapshot tag set")
        self.logger.test("[X] Source snapshot tag created")

        self.assertEqual(ec2_destination.get_snapshot_create_volume_permission_users(snapshot_id), ["123456789012"],
                         "Create volume permissions set")
        self.logger.test("[X] Volume create permission set")

        self.assertEqual(snapshot_copy["Description"], source_snapshot["Description"], "Description is copied")
        self.logger.test("[X] Description is copied")

    def check_source_snapshot(self, snapshot_source__id, snapshot_copy_id, task_name, destination):
        self.logger.test("Checking source snapshot")
        snapshot_tags = self.ec2.get_snapshot_tags(snapshot_source__id)

        snapshot_placeholders = {
            copy_snapshot.TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID: snapshot_copy_id,
            copy_snapshot.TAG_PLACEHOLDER_COPIED_REGION: destination
        }
        self.assertTrue(testing.tags.verify_placeholder_tags(snapshot_tags, snapshot_placeholders),
                        "All placeholder tags set on source snapshot {}".format(snapshot_tags))
        self.logger.test("[X] Placeholder tags created on source snapshot")

        copied_tag_value = snapshot_tags.get(copy_snapshot.Ec2CopySnapshotAction.marker_tag_copied_to(task_name))
        self.assertIsNotNone(copied_tag_value, "Marker for snapshot copied by task is set")
        try:
            copied_data = json.loads(copied_tag_value)
        except ValueError:
            copied_data = {}
        self.assertEqual(destination, copied_data.get("region"), "Destination region is set in copied marker tag")
        self.logger.test("[X] Snapshot copied marker tag set and has expected values")

    def cleanup_leftover_source_snapshots(self, test_method):
        self.ec2.delete_snapshots_by_tags(tag_filter_expression="{}={}".format(tasklist_tagname(TESTED_ACTION), test_method))

    def test_copy_unencrypted_snapshot_same_region(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot_unencrypted = self.ec2.create_snapshot(self.volume_unencrypted, tags={
            "copied-tag": "copied-value",
            "not-copied-tag": "not-copied-value",
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
        self.snapshots.append(source_snapshot_unencrypted["SnapshotId"])

        testing.tags.set_ec2_tag_to_delete(self.ec2, [source_snapshot_unencrypted["SnapshotId"]])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
            copy_snapshot.PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS: ["123456789012"],
            copy_snapshot.PARAM_COPIED_SNAPSHOT_TAGS: "copied-tag",
            copy_snapshot.PARAM_SNAPSHOT_DESCRIPTION: "{{{}}}".format(copy_snapshot.TAG_PLACEHOLDER_SOURCE_DESCRIPTION),
            copy_snapshot.PARAM_SNAPSHOT_TAGS: testing.tags.common_placeholder_tags(
                test_delete=False,
                placeholders=[
                    copy_snapshot.TAG_PLACEHOLDER_SOURCE_REGION,
                    copy_snapshot.TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID,
                    copy_snapshot.TAG_PLACEHOLDER_SOURCE_VOLUME,
                    copy_snapshot.TAG_PLACEHOLDER_OWNER_ACCOUNT]),
            copy_snapshot.PARAM_SOURCE_TAGS: testing.tags.common_placeholder_tags([
                copy_snapshot.TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID,
                copy_snapshot.TAG_PLACEHOLDER_COPIED_REGION
            ]),
            copy_snapshot.PARAM_COPIED_SNAPSHOTS: copy_snapshot.COPIED_OWNED_BY_ACCOUNT,
            copy_snapshot.PARAM_DELETE_AFTER_COPY: False,
            copy_snapshot.PARAM_ENCRYPTED: False
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        self.check_copy_snapshot(snapshot_copy_id, source_snapshot_unencrypted, region())
        self.check_source_snapshot(source_snapshot_unencrypted["SnapshotId"], snapshot_copy_id, test_method, region())

        assert (self.task_runner.max_concurrency == 5)
        assert (self.task_runner.concurrency_key == "ec2:CopySnapshot:{}:{}".format(self.task_runner.tested_account,
                                                                                    self.task_runner.tested_region))

    def test_copy_snapshot_description(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot = self.ec2.create_snapshot(self.volume_unencrypted,
                                                   tags={
                                                       "Name": "Ec2CopySnapshot_{}".format(test_method),
                                                       tasklist_tagname(TESTED_ACTION): test_method
                                                   }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
        self.snapshots.append(source_snapshot["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
            copy_snapshot.PARAM_COPIED_SNAPSHOTS: copy_snapshot.COPIED_OWNED_BY_ACCOUNT,
            copy_snapshot.PARAM_DELETE_AFTER_COPY: False,
            copy_snapshot.PARAM_ENCRYPTED: False
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        copied_snapshot = self.ec2.get_snapshot(snapshot_copy_id)
        self.assertEqual(source_snapshot.get("Description"), copied_snapshot.get("Description"), "Description copied as default")
        self.logger.test("[X]Source description copied")

    def test_snapshot_only_copied_once(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot = self.ec2.create_snapshot(self.volume_unencrypted, tags={
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
        assert (source_snapshot is not None)
        self.snapshots.append(source_snapshot["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
        }

        self.logger.test("Running task to copy snapshot")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)

        self.logger.test("Running task again")
        self.task_runner.run(parameters, task_name=test_method, complete_check_polling_interval=10)
        self.logger.test("[X] Task completed")
        self.assertEqual(0, len(self.task_runner.results), "Snapshot already copied")
        self.logger.test("[X] Snapshot was not copied for second time")

    def test_snapshot_not_longer_available(self):

        def delete_snapshot_after_select(tracker):
            for item in tracker.task_items:
                self.ec2.delete_snapshots(snapshot_ids=[item.get(handlers.TASK_TR_RESOURCES, {}).get("SnapshotId")])

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot = self.ec2.create_snapshot(self.volume_unencrypted, tags={
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
        assert (source_snapshot is not None)
        self.snapshots.append(source_snapshot["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
        }

        self.logger.test("Running task to copy snapshot")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10, run_after_select=delete_snapshot_after_select)

        self.logger.test("[X] Task completed")
        self.assertEqual(1, len(self.task_runner.results), "Snapshot not longer available")
        self.logger.test("[X] Snapshot was not longer available")

    def test_copy_unencrypted_snapshot_other_region(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        destination_ec2 = Ec2(region=remote_region())
        remote_snapshot_copy_id = None

        try:
            self.logger.test("Creating source snapshot")
            source_snapshot_unencrypted = self.ec2.create_snapshot(self.volume_unencrypted, tags={
                "Name": "Ec2CopySnapshot_{}".format(test_method),
                "copied-tag": "copied-value",
                "not-copied-tag": "not-copied-value",
                tasklist_tagname(TESTED_ACTION): test_method
            }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))

            self.snapshots.append(source_snapshot_unencrypted["SnapshotId"])

            parameters = {

                copy_snapshot.PARAM_DESTINATION_REGION: remote_region(),
                copy_snapshot.PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS: ["123456789012"],
                copy_snapshot.PARAM_COPIED_SNAPSHOT_TAGS: "copied-tag",
                copy_snapshot.PARAM_SNAPSHOT_DESCRIPTION: "{{{}}}".format(copy_snapshot.TAG_PLACEHOLDER_SOURCE_DESCRIPTION),
                copy_snapshot.PARAM_SNAPSHOT_TAGS: testing.tags.common_placeholder_tags(
                    test_delete=False,
                    placeholders=[
                        copy_snapshot.TAG_PLACEHOLDER_SOURCE_REGION,
                        copy_snapshot.TAG_PLACEHOLDER_SOURCE_SNAPSHOT_ID,
                        copy_snapshot.TAG_PLACEHOLDER_SOURCE_VOLUME,
                        copy_snapshot.TAG_PLACEHOLDER_OWNER_ACCOUNT]),
                copy_snapshot.PARAM_SOURCE_TAGS: testing.tags.common_placeholder_tags([
                    copy_snapshot.TAG_PLACEHOLDER_COPIED_SNAPSHOT_ID,
                    copy_snapshot.TAG_PLACEHOLDER_COPIED_REGION
                ])
            }

            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=10)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")

            remote_snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
            self.logger.test("[X] Task completed")

            self.logger.test("Checking snapshot copy")
            snapshot_copy = destination_ec2.get_snapshot(remote_snapshot_copy_id)
            self.assertIsNotNone(snapshot_copy, "Snapshot created in destination region")

            self.check_copy_snapshot(remote_snapshot_copy_id, source_snapshot_unencrypted, remote_region())
            self.check_source_snapshot(source_snapshot_unencrypted["SnapshotId"],
                                       remote_snapshot_copy_id, test_method,
                                       remote_region())

        finally:
            if remote_snapshot_copy_id is not None:
                destination_ec2.delete_snapshots([remote_snapshot_copy_id])

    def test_delete_source_snapshot_after_copy(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot = self.ec2.create_snapshot(self.volume_unencrypted, tags={
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))

        self.snapshots.append(source_snapshot["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
            copy_snapshot.PARAM_DELETE_AFTER_COPY: True
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")

        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        self.logger.test("Checking snapshot copy")
        snapshot_copy = self.ec2.get_snapshot(snapshot_copy_id)
        self.assertIsNotNone(snapshot_copy, "Snapshot copy created")
        self.logger.test("[X] Snapshot copy created")

        self.logger.test("Checking snapshot source")
        self.assertIsNone(self.ec2.get_snapshot(source_snapshot["SnapshotId"]), "Snapshot deleted")
        self.logger.test("[X] Source snapshot deleted")

    def test_copy_default_key_encrypted_snapshot_same_region(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot_encrypted_default = self.ec2.create_snapshot(self.volume_encrypted_default, tags={
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
        self.snapshots.append(source_snapshot_encrypted_default["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region()
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        self.logger.test("Checking snapshot copy")
        snapshot_copy = self.ec2.get_snapshot(snapshot_copy_id)
        self.assertIsNotNone(snapshot_copy, "Snapshot created")

    def test_copy_custom_key_encrypted_snapshot_same_region(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating source snapshot")
        source_snapshot_encrypted_custom = self.ec2.create_snapshot(self.volume_encrypted_custom, tags={
            tasklist_tagname(TESTED_ACTION): test_method,
            "Name": "Ec2CopySnapshot-Snapshot-{}-encrypted-custom".format(self.volume_encrypted_custom),
        }, description="Ec2CopySnapshot : Snapshot for encrypted volume {} using custom key".format(self.volume_encrypted_custom))

        self.snapshots.append(source_snapshot_encrypted_custom["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region()
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters, task_name=test_method, complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        self.logger.test("Checking snapshot copy")
        snapshot_copy = self.ec2.get_snapshot(snapshot_copy_id)
        self.assertIsNotNone(snapshot_copy, "Snapshot created")
        self.logger.test("[X] Snapshot created")

    def test_copy_snapshot_encrypt_with_custom_key_other_region(self):

        destination_ec2 = Ec2(region=remote_region())

        self.logger.test("Creating stack in destination region with custom encryption key")
        remote_stack = get_resource_stack(TESTED_ACTION, create_resource_stack_func=self.create_remote_resource_stack,
                                          use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK, region_name=remote_region())

        remote_snapshot_copy_id = None
        try:

            remote_custom_key_arn = remote_stack.stack_outputs["EncryptionKeyArn"]

            test_method = inspect.stack()[0][3]

            self.cleanup_leftover_source_snapshots(test_method)

            self.logger.test("Creating source snapshot")
            source_snapshot_unencrypted = self.ec2.create_snapshot(self.volume_unencrypted, tags={
                "Name": "Ec2CopySnapshot_{}".format(test_method),
                tasklist_tagname(TESTED_ACTION): test_method
            }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
            assert (source_snapshot_unencrypted is not None)
            self.snapshots.append(source_snapshot_unencrypted["SnapshotId"])

            parameters = {
                copy_snapshot.PARAM_DESTINATION_REGION: remote_region(),
                copy_snapshot.PARAM_ENCRYPTED: True,
                copy_snapshot.PARAM_KMS_KEY_ID: remote_custom_key_arn
            }

            self.snapshots.append(source_snapshot_unencrypted["SnapshotId"])

            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=10)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")
            remote_snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
            self.logger.test("[X] Task completed")

            self.logger.test("Checking snapshot copy")
            snapshot_copy = destination_ec2.get_snapshot(remote_snapshot_copy_id)
            self.assertIsNotNone(snapshot_copy, "Snapshot created in destination region")
            self.assertTrue(snapshot_copy["Encrypted"], "Snapshot is encrypted")
            self.assertEqual(snapshot_copy["KmsKeyId"], remote_custom_key_arn, "Custom encryption key is used")
            self.logger.test("[X] Snapshot created and encrypted")

        finally:
            if not KEEP_AND_USE_EXISTING_RESOURCES_STACK and remote_stack is not None:
                remote_stack.delete_stack(600)
            if remote_snapshot_copy_id is not None:
                destination_ec2.delete_snapshots([remote_snapshot_copy_id])

    def test_copy_snapshot_encrypted_with_default_key(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        snapshot_unencrypted = self.ec2.create_snapshot(self.volume_unencrypted, tags={
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }, description="Snapshot for testing Ec2CopySnapshot : {}".format(test_method))
        assert (snapshot_unencrypted is not None)
        self.snapshots.append(snapshot_unencrypted["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
            copy_snapshot.PARAM_ENCRYPTED: True
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        self.logger.test("Checking snapshot copy")
        snapshot_copy = self.ec2.get_snapshot(snapshot_copy_id)
        self.assertIsNotNone(snapshot_copy, "Snapshot created")
        self.assertTrue(snapshot_copy["Encrypted"], "Snapshot is encrypted")
        self.assertEqual(snapshot_copy["KmsKeyId"], self.ec2.ebs_default_key_arn, "Default EBS encryption key is used")
        self.logger.test("[X] Snapshot created and encrypted")

    def test_copy_snapshot_encrypted_with_custom_key(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        tags = {
            "Name": "Ec2CopySnapshot_{}".format(test_method),
            tasklist_tagname(TESTED_ACTION): test_method
        }
        source_snapshot_unencrypted = self.ec2.create_snapshot(self.volume_unencrypted,
                                                               tags=tags,
                                                               description="Snapshot for testing Ec2CopySnapshot : {}".format(
                                                                   test_method))
        assert (source_snapshot_unencrypted is not None)
        self.snapshots.append(source_snapshot_unencrypted["SnapshotId"])

        parameters = {
            copy_snapshot.PARAM_DESTINATION_REGION: region(),
            copy_snapshot.PARAM_ENCRYPTED: True,
            copy_snapshot.PARAM_KMS_KEY_ID: self.custom_key_arn
        }

        self.logger.test("Running task")
        self.task_runner.run(parameters,
                             task_name=test_method,
                             complete_check_polling_interval=10)
        self.assertTrue(self.task_runner.success(), "Task executed successfully")
        snapshot_copy_id = self.task_runner.results[0].result["copy-snapshot-id"]
        self.snapshots.append(snapshot_copy_id)
        self.logger.test("[X] Task completed")

        self.logger.test("Checking snapshot copy")
        snapshot_copy = self.ec2.get_snapshot(snapshot_copy_id)
        self.assertIsNotNone(snapshot_copy, "Snapshot created")
        self.assertTrue(snapshot_copy["Encrypted"], "Snapshot is encrypted")
        self.assertEqual(snapshot_copy["KmsKeyId"], self.custom_key_arn, "Custom encryption key is used")
        self.logger.test("[X] Snapshot created and encrypted")

    def delete_snapshots(self):
        if len(self.snapshots) > 0:
            self.logger.test("Deleting created snapshots {}", ",".join(self.snapshots))
        # noinspection PyBroadException,PyPep8
        try:
            self.ec2.delete_snapshots(self.snapshots)
            self.snapshots = []
        except:
            pass

    @classmethod
    def tearDownClass(cls):

        if not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            if cls.resource_stack is not None:
                cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

    def setUp(self):
        self.snapshots = []

    def tearDown(self):
        self.delete_snapshots()


if __name__ == '__main__':
    unittest.main()
