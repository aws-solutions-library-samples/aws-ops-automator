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
import time
import unittest
from types import FunctionType
import sys

import actions.ec2_resize_instance_action as r
import handlers.ec2_tag_event_handler
import testing.tags
from testing.console_logger import ConsoleLogger
from testing.ec2 import Ec2
from testing.stack import Stack
from tests.action_tests import get_resource_stack, get_task_runner, region, tasklist_tagname, template_path

TESTED_ACTION = "Ec2ResizeInstance"
TEST_RESOURCES_TEMPLATE = "test_resources.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False

TEST_INSTANCE_TYPES = ["t2.micro", "t2.small", "t2.medium", "t2.large"]


class TestAction(unittest.TestCase):
    logger = None
    resource_stack = None
    task_runner = None
    ec2 = None
    instance_id = None
    original_tags = None

    def __init__(self, method_name):
        """
        Initialize a method.

        Args:
            self: (todo): write your description
            method_name: (str): write your description
        """
        unittest.TestCase.__init__(self, method_name)

    @classmethod
    def get_methods(cls):
        """
        Return a list of methods

        Args:
            cls: (todo): write your description
        """
        return [x for x, y in list(cls.__dict__.items()) if type(y) == FunctionType and x.startswith("test_")]

    @classmethod
    def setUpClass(cls):
        """
        Set aws ec2 instance

        Args:
            cls: (todo): write your description
        """
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")

        cls.logger = ConsoleLogger()

        cls.resource_stack = get_resource_stack(TESTED_ACTION,
                                                create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK,
                                                region_name=region())

        assert (cls.resource_stack is not None)

        cls.ec2 = Ec2(region())
        cls.instance_id = cls.resource_stack.stack_outputs["InstanceId"]
        if KEEP_AND_USE_EXISTING_ACTION_STACK:
            cls.ec2.start_instance(cls.instance_id)

        testing.tags.set_ec2_tag_to_delete(ec2_client=cls.ec2, resource_ids=[cls.instance_id])

        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK)

        tags = cls.ec2.get_instance_tags(cls.instance_id)
        cls.original_tags = {t: tags[t] for t in tags if not t.startswith("aws:")}

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        """
        Creates cloudformation stack.

        Args:
            cls: (callable): write your description
            resource_stack_name: (str): write your description
        """
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            ami = Ec2(region()).latest_aws_linux_image["ImageId"]
            resource_stack = Stack(resource_stack_name, region=region())
            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE),
                                        timeout=1200,
                                        iam_capability=True,
                                        params={
                                            "InstanceAmi": ami,
                                            "InstanceType": TEST_INSTANCE_TYPES[0]
                                        })
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    @classmethod
    def restore_tags(cls):
        """
        Restore tags from ec2

        Args:
            cls: (todo): write your description
        """
        cls.ec2.restore_instance_tags(cls.instance_id, cls.original_tags)

    def reset_instance_type(self, start_type):
        """
        Reset this ec2 instance

        Args:
            self: (todo): write your description
            start_type: (str): write your description
        """
        org_size = self.ec2.get_instance(instance_id=self.instance_id)["InstanceType"]
        if org_size != start_type:
            self.ec2.stop_instance(self.instance_id)
            self.ec2.resize_instance(self.instance_id, start_type)

    def do_test_resize(self,
                       test_method,
                       resize_mode,
                       start_type,
                       expected_type,
                       resized_types=None,
                       scaling_range=None,
                       unavailable_types=None,
                       tags=None,
                       assumed_type=None,
                       try_next_in_range=None,
                       running=True):
        """
        Perform a resize.

        Args:
            self: (todo): write your description
            test_method: (str): write your description
            resize_mode: (int): write your description
            start_type: (str): write your description
            expected_type: (todo): write your description
            resized_types: (int): write your description
            scaling_range: (todo): write your description
            unavailable_types: (str): write your description
            tags: (todo): write your description
            assumed_type: (todo): write your description
            try_next_in_range: (todo): write your description
            running: (todo): write your description
        """

        if not running:
            self.ec2.stop_instance(self.instance_id)

        self.reset_instance_type(start_type)

        if running:
            time.sleep(10)
            self.ec2.start_instance(self.instance_id)

        org_size = self.ec2.get_instance(instance_id=self.instance_id)["InstanceType"]

        try:

            instance_tags = {tasklist_tagname(TESTED_ACTION): test_method}
            if tags:
                instance_tags.update(tags)

            self.ec2.create_tags(self.instance_id, tags=instance_tags)

            parameters = {
                r.PARAM_RESIZED_INSTANCE_TAGS: testing.tags.common_placeholder_tags(placeholders=[
                    r.TAG_PLACEHOLDER_NEW_INSTANCE_TYPE,
                    r.TAG_PLACEHOLDER_ORG_INSTANCE_TYPE]),

                r.PARAM_RESIZE_MODE: resize_mode
            }

            if resize_mode == r.RESIZE_BY_SPECIFIED_TYPE:
                parameters[r.PARAM_INSTANCE_TYPES] = resized_types
            else:
                parameters[r.PARAM_SCALING_RANGE] = scaling_range
                parameters[r.PARAM_TAGFILTER_SCALE_UP] = "scaling=up"
                parameters[r.PARAM_TAGFILTER_SCALE_DOWN] = "scaling=down"
                if assumed_type is not None:
                    parameters[r.PARAM_ASSUMED_TYPE] = assumed_type
                parameters[r.PARAM_TRY_NEXT_IN_RANGE] = try_next_in_range

            if unavailable_types is not None:
                parameters[r.PARAM_TEST_UNAVAILABLE_TYPES] = unavailable_types if isinstance(unavailable_types, list) else [
                    unavailable_types]

            events = {
                handlers.ec2_tag_event_handler.EC2_TAG_EVENT_SOURCE: {
                    handlers.TAG_CHANGE_EVENT: [
                        handlers.ec2_tag_event_handler.EC2_CHANGED_INSTANCE_TAGS_EVENT
                    ]
                }
            }

            self.logger.test("Running task")
            self.task_runner.run(parameters,
                                 task_name=test_method,
                                 complete_check_polling_interval=15,
                                 events=events)

            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
            self.logger.test("[X] Task completed")

            # test instance type here
            instance = self.ec2.get_instance(self.instance_id)
            new_type = instance["InstanceType"]
            self.assertEqual(expected_type, new_type, "Expected instance type")

            self.assertTrue(self.ec2.get_instance_status(self.instance_id) == "running" if running else "stopped", "Instance state")
            self.assertEqual(expected_type, new_type, "Expected instance state")

            instance_tags = self.ec2.get_instance_tags(self.instance_id)
            if not self.task_runner.executed_tasks[0].ActionResult.get("not-resized", False):
                self.assertTrue(testing.tags.verify_placeholder_tags(instance_tags,
                                                                     action_placeholders={
                                                                         r.TAG_PLACEHOLDER_NEW_INSTANCE_TYPE: new_type,
                                                                         r.TAG_PLACEHOLDER_ORG_INSTANCE_TYPE: org_size
                                                                     }),
                                "All placeholder tags set on resized instance")
                self.logger.test("[X] Instance placeholder tags created")

            if resize_mode == r.RESIZE_BY_STEP:
                self.assertFalse("scaling" in instance_tags, 'Scaling tags removed')
                self.logger.test("[X] Scaling filter tags removed")

        finally:
            self.restore_tags()

    def test_resize_running(self):
        """
        Resize the state of the running process.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_SPECIFIED_TYPE,
                            start_type=TEST_INSTANCE_TYPES[0],
                            resized_types=[TEST_INSTANCE_TYPES[1]],
                            expected_type=TEST_INSTANCE_TYPES[1])

    def test_resize_stopped(self):
        """
        Resize the state of the resize.

        Args:
            self: (todo): write your description
        """

        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_SPECIFIED_TYPE,
                            start_type=TEST_INSTANCE_TYPES[0],
                            resized_types=[TEST_INSTANCE_TYPES[1]],
                            expected_type=TEST_INSTANCE_TYPES[1],
                            running=False)

    def test_no_resize(self):
        """
        Test for resize of the current instance.

        Args:
            self: (todo): write your description
        """
        current_size = self.ec2.get_instance(self.instance_id)["InstanceType"]
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_SPECIFIED_TYPE,
                            start_type=current_size,
                            resized_types=[current_size],
                            expected_type=current_size)

    def test_alternative_type(self):
        """
        Determine the alternative type.

        Args:
            self: (todo): write your description
        """

        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_SPECIFIED_TYPE,
                            start_type=TEST_INSTANCE_TYPES[0],
                            resized_types=TEST_INSTANCE_TYPES[1:],
                            unavailable_types=TEST_INSTANCE_TYPES[1:3],
                            expected_type=TEST_INSTANCE_TYPES[3])

    def test_no_alternative_avail_keep_org(self):
        """
        Test whether the keep only one of - rest of the method.

        Args:
            self: (todo): write your description
        """

        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_SPECIFIED_TYPE,
                            start_type=TEST_INSTANCE_TYPES[0],
                            resized_types=TEST_INSTANCE_TYPES[1:],
                            unavailable_types=TEST_INSTANCE_TYPES[1:],
                            expected_type=TEST_INSTANCE_TYPES[0])

    def test_step_up(self):
        """
        Resize the current step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type=TEST_INSTANCE_TYPES[0],
                            scaling_range=TEST_INSTANCE_TYPES,
                            expected_type=TEST_INSTANCE_TYPES[1])

    def test_step_up_already_at_largest(self):
        """
        Test for the state step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type=TEST_INSTANCE_TYPES[-1],
                            scaling_range=TEST_INSTANCE_TYPES,
                            expected_type=TEST_INSTANCE_TYPES[-1])

    def test_step_up_alternative_type(self):
        """
        Test if the step step step.

        Args:
            self: (todo): write your description
        """

        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type=TEST_INSTANCE_TYPES[0],
                            try_next_in_range=True,
                            scaling_range=TEST_INSTANCE_TYPES,
                            unavailable_types=TEST_INSTANCE_TYPES[1:2],
                            expected_type=TEST_INSTANCE_TYPES[2])

    def test_step_up_no_alternative_avail(self):
        """
        Resize the step step step.

        Args:
            self: (todo): write your description
        """

        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type=TEST_INSTANCE_TYPES[0],
                            try_next_in_range=True,
                            scaling_range=TEST_INSTANCE_TYPES,
                            unavailable_types=TEST_INSTANCE_TYPES[1:],
                            expected_type=TEST_INSTANCE_TYPES[0])

    def test_step_up_no_next(self):
        """
        Test if the next step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type=TEST_INSTANCE_TYPES[0],
                            try_next_in_range=False,
                            scaling_range=TEST_INSTANCE_TYPES,
                            unavailable_types=TEST_INSTANCE_TYPES[1:2],
                            expected_type=TEST_INSTANCE_TYPES[0])

    def test_step_up_assumed(self):
        """
        Resize the state of the step step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type="t2.nano",
                            assumed_type=TEST_INSTANCE_TYPES[0],
                            scaling_range=TEST_INSTANCE_TYPES,
                            expected_type=TEST_INSTANCE_TYPES[1])

    def test_up_not_assumed(self):
        """
        Test if the state of the test state of the test.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "up"},
                            start_type="t2.nano",
                            scaling_range=TEST_INSTANCE_TYPES,
                            expected_type="t2.nano")

    def test_step_down(self):
        """
        Resize the current step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "down"},
                            start_type=TEST_INSTANCE_TYPES[1],
                            scaling_range=TEST_INSTANCE_TYPES,
                            expected_type=TEST_INSTANCE_TYPES[0])

    def test_step_down_already_at_smallest(self):
        """
        Perform a single step of every step.

        Args:
            self: (todo): write your description
        """
        smallest_size = TEST_INSTANCE_TYPES[0]
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "down"},
                            start_type=smallest_size,
                            scaling_range=TEST_INSTANCE_TYPES,
                            expected_type=smallest_size)

    def test_step_down_alternative_type(self):
        """
        Resize the step step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "down"},
                            start_type=TEST_INSTANCE_TYPES[2],
                            try_next_in_range=True,
                            scaling_range=TEST_INSTANCE_TYPES,
                            unavailable_types=TEST_INSTANCE_TYPES[1:2],
                            expected_type=TEST_INSTANCE_TYPES[0])

    def test_step_down_no_next(self):
        """
        Test if the next step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "down"},
                            start_type=TEST_INSTANCE_TYPES[2],
                            try_next_in_range=False,
                            scaling_range=TEST_INSTANCE_TYPES,
                            unavailable_types=TEST_INSTANCE_TYPES[1:2],
                            expected_type=TEST_INSTANCE_TYPES[2])

    def test_step_down_no_available(self):
        """
        Resize the current step of the current step.

        Args:
            self: (todo): write your description
        """
        self.do_test_resize(test_method=inspect.stack()[0][3],
                            resize_mode=r.RESIZE_BY_STEP,
                            tags={"scaling": "down"},
                            start_type=TEST_INSTANCE_TYPES[2],
                            try_next_in_range=False,
                            scaling_range=TEST_INSTANCE_TYPES,
                            unavailable_types=TEST_INSTANCE_TYPES[0:2],
                            expected_type=TEST_INSTANCE_TYPES[2])

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the context manager.

        Args:
            cls: (todo): write your description
        """

        if cls.resource_stack is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

    def setUp(self):
        """
        Sets the result of this thread.

        Args:
            self: (todo): write your description
        """
        pass

    def tearDown(self):
        """
        Tear down the next callable.

        Args:
            self: (todo): write your description
        """
        pass


if __name__ == '__main__':
    unittest.main()
