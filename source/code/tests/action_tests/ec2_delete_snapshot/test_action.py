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
import unittest
from datetime import timedelta
import sys

import actions.ec2_delete_snapshot_action as delete_snapshot_action
from testing.console_logger import ConsoleLogger
from testing.ec2 import Ec2
from testing.stack import Stack
from tests.action_tests import region, tasklist_tagname, get_resource_stack, get_task_runner, set_snapshot_sources_tags, \
    template_path

TESTED_ACTION = "Ec2DeleteSnapshot"
TEST_RESOURCES_TEMPLATE = "test_resources.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False


class TestAction(unittest.TestCase):
    volume_id = None
    logger = None
    resource_stack = None
    task_runner = None
    ec2 = None

    def __init__(self, method_name):
        unittest.TestCase.__init__(self, method_name)
        self.created_snapshots = None
        self.deleted_snapshot = None

    @classmethod
    def setUpClass(cls):
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")
            
        cls.logger = ConsoleLogger()

        cls.resource_stack = get_resource_stack(TESTED_ACTION,
                                                create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK, region_name=region())
        assert (cls.resource_stack is not None)
        cls.volume_id = cls.resource_stack.stack_outputs["VolumeId0"]

        cls.ec2 = Ec2(region())

        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK)

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            resource_stack = Stack(resource_stack_name, region=region())
            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE),
                                        iam_capability=True,
                                        params={})
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    def cleanup_leftover_source_snapshots(self, test_method):
        self.ec2.delete_snapshots_by_tags(tag_filter_expression="{}={}".format(tasklist_tagname(TESTED_ACTION), test_method))

    def test_delete_by_retention_count(self):

        snapshots_to_create = 4
        snapshots_to_keep = 2

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        for i in range(0, snapshots_to_create):
            snapshot = self.ec2.create_snapshot(self.volume_id, tags={
                tasklist_tagname(TESTED_ACTION): test_method
            }, wait_to_complete=300)
            assert (snapshot is not None)
            self.created_snapshots.append(snapshot["SnapshotId"])

        try:
            parameters = {
                delete_snapshot_action.PARAM_RETENTION_COUNT: snapshots_to_keep,
                delete_snapshot_action.PARAM_RETENTION_DAYS: 0
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method, debug=False)
            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.deleted_snapshots = getattr(self.task_runner.results[0], "result", {})["deleted"][region()]

            self.assertEqual(sorted(self.created_snapshots[0:snapshots_to_create - snapshots_to_keep]),
                              sorted(self.deleted_snapshots), "Expected snapshots deleted")
            self.logger.test("[X] {} oldest snapshots deleted out of {}", len(self.deleted_snapshots), len(self.created_snapshots))

            remaining_snapshots = [s["SnapshotId"] for s in self.ec2.get_snapshots_for_volume(volume_id=self.volume_id)]
            self.assertEqual(sorted(self.created_snapshots[snapshots_to_create - snapshots_to_keep:]),
                              sorted(remaining_snapshots), "Expected snapshots retained")
            self.logger.test("[X] {} latest snapshots retained out of {}", len(remaining_snapshots), len(self.created_snapshots))

        finally:
            self.delete_volume_snapshots()

    def test_delete_by_retention_count_with_copied_snapshot(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating (source) snapshot")
        snapshot_source = self.ec2.create_snapshot(self.volume_id, tags={
            tasklist_tagname(TESTED_ACTION): test_method
        }, wait_to_complete=300)
        assert (snapshot_source is not None)
        self.created_snapshots.append(snapshot_source["SnapshotId"])

        self.logger.test("Copying snapshot")
        snapshot_copy = self.ec2.copy_snapshot(snapshot_id=snapshot_source["SnapshotId"], destination_region=region(), tags={
            tasklist_tagname(TESTED_ACTION): test_method
        })
        assert (snapshot_copy is not None)
        self.created_snapshots.append(snapshot_copy["SnapshotId"])
        set_snapshot_sources_tags(snapshot_id=snapshot_copy["SnapshotId"], source_volume_id=self.volume_id,
                                  source_snapshot_id=snapshot_source["SnapshotId"])

        try:
            parameters = {
                delete_snapshot_action.PARAM_RETENTION_COUNT: 1,
                delete_snapshot_action.PARAM_RETENTION_DAYS: 0
            }

            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method)
            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.deleted_snapshots = getattr(self.task_runner.results[0], "result", {})["deleted"][region()]

            self.logger.test("Checking deleted snapshot")
            self.assertEqual(1, len(self.deleted_snapshots), "Deleted single snapshot")
            self.assertEqual(self.deleted_snapshots[0], snapshot_source["SnapshotId"], "Oldest (source) snapshot deleted")
            self.logger.test("[X] Source snapshot deleted ")

            remaining_snapshot = self.ec2.get_snapshot(snapshot_copy["SnapshotId"])
            self.assertIsNotNone(remaining_snapshot, "Copied snapshot still exists")
            self.logger.test("[X] Copied snapshot still exists")

        finally:
            self.delete_volume_snapshots()

    def test_delete_by_retention_count_all_retained(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        self.logger.test("Creating (source) snapshot")
        snapshot = self.ec2.create_snapshot(self.volume_id, tags={
            tasklist_tagname(TESTED_ACTION): test_method
        }, wait_to_complete=300)
        assert (snapshot is not None)
        self.created_snapshots.append(snapshot["SnapshotId"])

        try:
            parameters = {
                delete_snapshot_action.PARAM_RETENTION_COUNT: 1,
                delete_snapshot_action.PARAM_RETENTION_DAYS: 0
            }

            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method)
            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.logger.test("Checking retained snapshot")
            self.assertEqual(0, getattr(self.task_runner.results[0], "result", {}).get("snapshots-deleted"),
                             "Snapshot is not deleted")
            self.assertIsNotNone(self.ec2.get_snapshot(snapshot["SnapshotId"]), "Snapshot still exists")
            self.logger.test("[X] Snapshot is retained")

        finally:
            self.delete_volume_snapshots()

    def test_delete_by_retention_days_all_retained(self):

        test_method = inspect.stack()[0][3]

        snapshot = self.ec2.create_snapshot(self.volume_id, tags={
            tasklist_tagname(TESTED_ACTION): test_method
        }, wait_to_complete=300)
        assert (snapshot is not None)
        self.created_snapshots.append(snapshot["SnapshotId"])

        try:
            parameters = {
                delete_snapshot_action.PARAM_RETENTION_COUNT: 0,
                delete_snapshot_action.PARAM_RETENTION_DAYS: 2
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task at current date")
            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=30)
            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.assertEqual(0, getattr(self.task_runner.results[0], "result", {}).get("snapshots-deleted"),
                             "Snapshot is not deleted")
            self.assertIsNotNone(self.ec2.get_snapshot(snapshot["SnapshotId"]), "Snapshot still exists")
            self.logger.test("[X] Snapshot is retained")
        finally:
            self.delete_volume_snapshots()

    def test_delete_by_retention_days_all_deleted(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        snapshot = self.ec2.create_snapshot(self.volume_id, tags={
            tasklist_tagname(TESTED_ACTION): test_method
        }, wait_to_complete=300)
        assert (snapshot is not None)
        self.created_snapshots.append(snapshot["SnapshotId"])

        try:
            parameters = {
                delete_snapshot_action.PARAM_RETENTION_COUNT: 0,
                delete_snapshot_action.PARAM_RETENTION_DAYS: 2
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task at simulated date +3 days")

            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=30,
                                 datetime_delta=timedelta(days=3))
            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
            self.logger.test("[X] Task completed")

            self.assertEqual(1, getattr(self.task_runner.results[0], "result", {}).get("snapshots-deleted"),
                             "Snapshot is deleted")
            self.assertIsNone(self.ec2.get_snapshot(snapshot["SnapshotId"]), "Snapshot no longer exists")
            self.logger.test("[X] Snapshot deleted")
        finally:
            self.delete_volume_snapshots()

    def test_no_snapshopts_selected(self):

        test_method = inspect.stack()[0][3]

        self.cleanup_leftover_source_snapshots(test_method)

        try:
            parameters = {
                delete_snapshot_action.PARAM_RETENTION_COUNT: 1,
                delete_snapshot_action.PARAM_RETENTION_DAYS: 0
            }

            test_method = inspect.stack()[0][3]

            self.logger.test("Running task")

            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=30)
            self.assertEqual(0,len(self.task_runner.executed_tasks), "Task not executed")

            self.logger.test("[X] Task not executed as expected")
        finally:
            self.delete_volume_snapshots()

    @classmethod
    def delete_volume_snapshots(cls):
        cls.ec2.delete_snapshots([s["SnapshotId"] for s in cls.ec2.get_snapshots_for_volume(cls.volume_id)])

    @classmethod
    def tearDownClass(cls):

        if cls.resource_stack is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

        cls.delete_volume_snapshots()

    def setUp(self):
        self.delete_volume_snapshots()
        self.deleted_snapshots = []
        self.created_snapshots = []

    def tearDown(self):
        self.delete_volume_snapshots()
        pass


if __name__ == '__main__':
    unittest.main()
