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
import re
import unittest
from types import FunctionType

import actions
import actions.ec2_create_snapshot_action as create_snapshot_action
import services
import tagging
import testing.tags
from tagging.tag_filter_expression import TagFilterExpression
from testing.console_logger import ConsoleLogger
from testing.ec2 import Ec2
from testing.stack import Stack
from tests.action_tests import region, tasklist_tagname, get_resource_stack, get_task_runner, template_path

TESTED_ACTION = "Ec2CreateSnapshot"
TEST_RESOURCES_TEMPLATE = "test_resources.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False


class TestAction(unittest.TestCase):
    logger = None
    resource_stack = None
    task_runner = None
    ec2 = None

    def __init__(self, method_name):
        unittest.TestCase.__init__(self, method_name)
        self.created_snapshots = None
        self.account = services.get_aws_account()

    @classmethod
    def get_methods(cls):
        return [x for x, y in list(cls.__dict__.items()) if type(y) == FunctionType and x.startswith("test_")]

    @classmethod
    def setUpClass(cls):

        cls.logger = ConsoleLogger()

        cls.resource_stack = get_resource_stack(TESTED_ACTION, create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK, region_name=region())
        assert (cls.resource_stack is not None)

        cls.instance_id = cls.resource_stack.stack_outputs["InstanceId0"]
        cls.root_volume = Ec2(region()).get_root_volume(cls.instance_id)
        cls.data_volumes = [cls.resource_stack.stack_outputs["VolumeId0"], cls.resource_stack.stack_outputs["VolumeId1"]]

        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK)

        cls.ec2 = Ec2(region())

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            ami = Ec2(region()).latest_aws_linux_image["ImageId"]
            resource_stack = Stack(resource_stack_name, region=region())
            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE), iam_capability=True,
                                        params={
                                            "InstanceAmi": ami,
                                            "InstanceType": "t2.micro",
                                            "TaskListTagName": tasklist_tagname(TESTED_ACTION),
                                            "TaskListTagValue": ",".join(cls.get_methods())
                                        })
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    def test_create_snapshots_all_volumes(self):

        def check_snapshot(snap_id, source_volume_id):
            checked_snapshot = self.ec2.get_snapshot(snap_id)
            self.assertIsNotNone(checked_snapshot, "Snapshot does exist")
            self.logger.test("[X] Snapshot was created")

            snapshot_tags = checked_snapshot.get("Tags", {})
            self.assertTrue(TagFilterExpression("InstanceTag=Instance0").is_match(snapshot_tags), "Instance tag copied")
            self.logger.test("[X] Instance tags copied")

            source_volume = self.ec2.get_volume(source_volume_id)
            assert (source_volume is not None)
            device = source_volume.get("Attachments", [{}])[0].get("Device", "")
            if source_volume_id in self.data_volumes:
                volume_tags = source_volume.get("Tags", {})
                self.assertTrue(
                    TagFilterExpression("VolumeTag={}".format(volume_tags.get("VolumeTag", ""))).is_match(snapshot_tags),
                    "Volume tag copied")
            self.logger.test("[X] Volume tags copied")

            snapshot_placeholders = {
                create_snapshot_action.TAG_PLACEHOLDER_VOLUME_ID: source_volume_id,
                create_snapshot_action.TAG_PLACEHOLDER_INSTANCE_ID: self.instance_id,
                create_snapshot_action.TAG_PLACEHOLDER_DEVICE: device
            }
            self.assertTrue(testing.tags.verify_placeholder_tags(snapshot_tags, snapshot_placeholders),
                            "All placeholder tags set on snapshot {}".format(snap_id))
            self.logger.test("[X] Placeholder tags created")

            self.assertTrue(TagFilterExpression(
                "{}={}".format(actions.marker_snapshot_tag_source_source_volume_id(), source_volume_id)).is_match(
                snapshot_tags), "Source volume tag set")
            self.logger.test("[X] Source volume tags created")

            self.assertTrue(self.ec2.get_snapshot_create_volume_permission_users(snap_id) == ["123456789012"],
                            "Create volume permissions set")
            self.logger.test("[X] Volume create permissions set")
            expected_name = "snapshot-{}-{}-{}".format(self.task_runner.action_stack_name, self.instance_id, source_volume_id)
            self.assertEqual(expected_name, snapshot_tags["Name"], "Name has been set")
            self.logger.test("[X] Snapshot name set")

            description = checked_snapshot.get("Description", "")
            self.assertTrue(
                all([p in description for p in [source_volume_id, self.instance_id, region(), self.task_runner.task_name]]),
                "Description is set")
            self.logger.test("[X] Snapshot description set")

        def check_volume(snapshot, volume):
            volume_tags = self.ec2.get_volume_tags(volume_id=volume)
            volume_placeholders = {create_snapshot_action.TAG_PLACEHOLDER_VOLUME_SNAPSHOT: snapshot}
            self.assertTrue(testing.tags.verify_placeholder_tags(volume_tags, volume_placeholders),
                            "All placeholder tags set on volume")
            self.logger.test("[X] Volume placeholder tags created")

        def check_instance():
            instance_tags = self.ec2.get_instance_tags(self.instance_id)
            instance_placeholders = {
                create_snapshot_action.TAG_PLACEHOLDER_INSTANCE_SNAPSHOTS: ",".join(sorted(self.created_snapshots))}
            self.assertTrue(testing.tags.verify_placeholder_tags(instance_tags, instance_placeholders),
                            "All placeholder tags set on snapshot")
            self.logger.test("[X] Instance placeholder tags created")

        try:
            parameters = {
                create_snapshot_action.PARAM_ACCOUNTS_VOLUME_CREATE_PERMISSIONS: ["123456789012"],
                create_snapshot_action.PARAM_BACKUP_DATA_DEVICES: True,
                create_snapshot_action.PARAM_BACKUP_ROOT_DEVICE: True,
                create_snapshot_action.PARAM_COPIED_INSTANCE_TAGS: "InstanceTag",
                create_snapshot_action.PARAM_COPIED_VOLUME_TAGS: "VolumeTag",
                create_snapshot_action.PARAM_SHARED_ACCOUNT_TAGGING_ROLENAME: "",
                create_snapshot_action.PARAM_INSTANCE_TAGS: testing.tags.common_placeholder_tags(
                    placeholders=[
                        create_snapshot_action.TAG_PLACEHOLDER_INSTANCE_SNAPSHOTS
                    ]),
                create_snapshot_action.PARAM_VOLUME_TAGS: testing.tags.common_placeholder_tags(
                    placeholders=[
                        create_snapshot_action.TAG_PLACEHOLDER_VOLUME_SNAPSHOT
                    ]),
                create_snapshot_action.PARAM_NAME: "{{{}}}-{{{}}}".format(create_snapshot_action.TAG_PLACEHOLDER_INSTANCE_ID,
                                                                          create_snapshot_action.TAG_PLACEHOLDER_VOLUME_ID),
                create_snapshot_action.PARAM_SET_SNAPSHOT_NAME: True,
                create_snapshot_action.PARAM_SNAPSHOT_DESCRIPTION:
                    "Snapshot for volume {{{}}} ({{{}}})for instance {{{}}} in {{{}}} created by task {{{}}}".format(
                        create_snapshot_action.TAG_PLACEHOLDER_VOLUME_ID,
                        create_snapshot_action.TAG_PLACEHOLDER_DEVICE,
                        create_snapshot_action.TAG_PLACEHOLDER_INSTANCE_ID,
                        tagging.TAG_VAL_REGION, tagging.TAG_VAL_TASK),
                create_snapshot_action.PARAM_SNAPSHOT_NAME_PREFIX: "snapshot-{}-".format(self.task_runner.action_stack_name),
                create_snapshot_action.PARAM_SNAPSHOT_TAGS: testing.tags.common_placeholder_tags(
                    test_delete=False,
                    placeholders=[
                        create_snapshot_action.TAG_PLACEHOLDER_INSTANCE_ID,
                        create_snapshot_action.TAG_PLACEHOLDER_VOLUME_ID,
                        create_snapshot_action.TAG_PLACEHOLDER_DEVICE
                    ]),
                create_snapshot_action.PARAM_VOLUME_TAG_FILTER: ""
            }

            test_method = inspect.stack()[0][3]

            testing.tags.set_ec2_tag_to_delete(self.ec2, [self.instance_id] + [self.root_volume] + self.data_volumes)

            self.logger.test("Running task")
            self.task_runner.run(parameters, task_name=test_method, complete_check_polling_interval=15)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")
            self.logger.test("[X] Task completed")

            volume_snapshots = getattr(self.task_runner.results[0], "ActionStartResult", {}).get("volumes", {})
            self.created_snapshots = [volume_snapshots[i]["snapshot"] for i in volume_snapshots]

            self.assertEqual(3, len(volume_snapshots), "[X] All expected snapshots created")
            self.assertTrue(all([self.ec2.get_snapshot_status(s) == "completed" for s in self.created_snapshots]),
                            "All snapshots completed")
            self.logger.test("[X] Snapshots created")

            for volume_id in volume_snapshots:
                snapshot_id = volume_snapshots[volume_id]["snapshot"]

                self.logger.test("Checking snapshot {} for volume {}", snapshot_id, volume_id)
                check_snapshot(snap_id=snapshot_id, source_volume_id=volume_id)

                self.logger.test("Checking volume {}", volume_id)
                check_volume(snapshot_id, volume_id)

            self.logger.test("Checking instance {}", self.instance_id)
            check_instance()

        finally:
            self.delete_snapshots()

    def test_create_snapshots_root_volume_only(self):

        try:
            parameters = {
                create_snapshot_action.PARAM_BACKUP_DATA_DEVICES: False,
                create_snapshot_action.PARAM_BACKUP_ROOT_DEVICE: True,
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task")
            self.task_runner.run(parameters, task_name=test_method, complete_check_polling_interval=15)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.logger.test("Checking root volume snapshot")
            volume_snapshots = getattr(self.task_runner.results[0], "ActionStartResult", {}).get("volumes", {})
            self.created_snapshots = [volume_snapshots[i]["snapshot"] for i in volume_snapshots]
            self.assertEqual(1, len(volume_snapshots), "[X] Single snapshot created")

            snapshot = self.ec2.get_snapshot(snapshot_id=self.created_snapshots[0])
            self.assertEqual(self.root_volume, snapshot["VolumeId"], "Snapshot is for root volume")
            self.logger.test("[X] Snapshot is for root device created")

        finally:
            self.delete_snapshots()

    def test_create_snapshots_with_default_name(self):

        try:
            parameters = {
                create_snapshot_action.PARAM_BACKUP_DATA_DEVICES: False,
                create_snapshot_action.PARAM_BACKUP_ROOT_DEVICE: True,
                create_snapshot_action.PARAM_SET_SNAPSHOT_NAME: True,
                create_snapshot_action.PARAM_SNAPSHOT_NAME_PREFIX: "snapshot-"
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task")
            self.task_runner.run(parameters, task_name=test_method, complete_check_polling_interval=15)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.logger.test("Checking root volume snapshot")
            volume_snapshots = getattr(self.task_runner.results[0], "ActionStartResult", {}).get("volumes", {})
            self.created_snapshots = [volume_snapshots[i]["snapshot"] for i in volume_snapshots]
            self.assertEqual(1, len(volume_snapshots), "[X] Snapshot created")

            snapshot = self.ec2.get_snapshot(snapshot_id=self.created_snapshots[0])
            expected_name = "snapshot-" + self.root_volume + "-" + r"\d{12}"
            self.assertIsNotNone(re.match(expected_name, snapshot.get("Tags", {}).get("Name", "")))
            self.logger.test("[X])  Expected default name for snapshot set")

        finally:
            self.delete_snapshots()

    def test_create_snapshots_data_volumes_only(self):

        try:
            parameters = {
                create_snapshot_action.PARAM_BACKUP_DATA_DEVICES: True,
                create_snapshot_action.PARAM_BACKUP_ROOT_DEVICE: False,
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task")
            self.task_runner.run(parameters, task_name=test_method, complete_check_polling_interval=30)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.logger.test("Checking data volume snapshots")
            volume_snapshots = getattr(self.task_runner.results[0], "ActionStartResult", {}).get("volumes", {})
            self.created_snapshots = [volume_snapshots[i]["snapshot"] for i in volume_snapshots]
            self.assertEqual(2, len(volume_snapshots), "[X] Data volume snapshots created")

            for snapshot_id in self.created_snapshots:
                snapshot = self.ec2.get_snapshot(snapshot_id=snapshot_id)
                self.assertIn(snapshot["VolumeId"], self.data_volumes, "Snapshot is for data volume")
            self.logger.test("[X] Snapshots are for data volumes")

        finally:
            self.delete_snapshots()

    def test_create_snapshots_volume_with_tagfilter(self):

        try:
            parameters = {
                create_snapshot_action.PARAM_BACKUP_DATA_DEVICES: True,
                create_snapshot_action.PARAM_BACKUP_ROOT_DEVICE: False,
                create_snapshot_action.PARAM_VOLUME_TAG_FILTER: "Backup=True",
            }

            test_method = inspect.stack()[0][3]

            from datetime import timedelta
            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=15)
            self.assertTrue(self.task_runner.success(), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.logger.test("Checking data snapshot")
            volume_snapshots = getattr(self.task_runner.results[0], "ActionStartResult", {}).get("volumes", {})
            self.created_snapshots = [volume_snapshots[i]["snapshot"] for i in volume_snapshots]
            self.assertEqual(1, len(volume_snapshots), "[X] Volume snapshots created")

            snapshot = self.ec2.get_snapshot(self.created_snapshots[0])
            snapshot_volume_tags = self.ec2.get_volume_tags(snapshot["VolumeId"])
            self.assertTrue(TagFilterExpression("Backup=True").is_match(snapshot_volume_tags),
                            "Snapshot is for selected data volume")
            self.logger.test("[X] Snapshot is for selected data volumes")

        finally:
            self.delete_snapshots()

    def delete_snapshots(self):
        if len(self.created_snapshots) > 0:
            self.logger.test("Deleting created snapshots {}", ",".join(self.created_snapshots))
        # noinspection PyBroadException,PyPep8
        try:
            self.ec2.delete_snapshots(self.created_snapshots)
            self.created_snapshots = []
        except:
            pass

    @classmethod
    def tearDownClass(cls):

        if cls.resource_stack is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

    def setUp(self):
        self.created_snapshots = []

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
