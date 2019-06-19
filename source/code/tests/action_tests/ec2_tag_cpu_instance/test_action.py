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
import inspect
import time
import unittest
from datetime import timedelta

import actions.ec2_tag_cpu_instance_action as lt
import pytz
import services
import testing.tags
from actions import date_time_provider
from testing.cloudwatch_metrics import CloudwatchMetrics
from testing.console_logger import ConsoleLogger
from testing.ec2 import Ec2
from testing.stack import Stack
from tests.action_tests import get_resource_stack, get_task_runner, region, tasklist_tagname, template_path

INTERVAL_MINUTES = 5

TESTED_ACTION = "Ec2TagCpuInstance"
TEST_RESOURCES_TEMPLATE = "test_resources.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False


class TestAction(unittest.TestCase):
    logger = None
    resource_stack = None
    task_runner = None
    ec2 = None
    metrics_client = None

    def __init__(self, method_name):
        unittest.TestCase.__init__(self, method_name)
        self.account = services.get_aws_account()

    @classmethod
    def setUpClass(cls):

        cls.logger = ConsoleLogger()

        cls.resource_stack = get_resource_stack(TESTED_ACTION,
                                                create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK,
                                                region_name=region())
        assert (cls.resource_stack is not None)

        cls.instance_no_cpu_load = cls.resource_stack.stack_outputs["InstanceNoCPULoad"]
        cls.instance_cpu_load = cls.resource_stack.stack_outputs["InstanceCPULoad"]

        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK,
                                          interval="0/{} * * * ?".format(INTERVAL_MINUTES))

        cls.ec2 = Ec2(region())

        testing.tags.set_ec2_tag_to_delete(ec2_client=cls.ec2, resource_ids=[cls.instance_no_cpu_load, cls.instance_cpu_load])

        cls.metrics_client = CloudwatchMetrics(region())

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            ami = Ec2(region()).latest_aws_linux_image["ImageId"]
            resource_stack = Stack(resource_stack_name, region=region())

            stack_parameters = {
                "InstanceAmi": ami, "InstanceType": "t2.micro",
                "TaskListTagName": tasklist_tagname(TESTED_ACTION),
                "TaskListTagValueNoCPULoad": "test_instance_no_cpu_load",
                "TaskListTagValueCPULoad": "test_instance_cpu_load"
            }

            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE),
                                        iam_capability=True,
                                        params=stack_parameters)
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    def base_test(self,
                  test_method,
                  instance_id,
                  expect_under_utilized,
                  expect_over_utilized,
                  cpu_high=90,
                  cpu_low=10,
                  interval=None):

        parameters = {
            lt.PARAM_CPU_PERC_LOW: cpu_low,
            lt.PARAM_CPU_PERC_HIGH: cpu_high,
            lt.PARAM_CPU_LOW_TAGS: testing.tags.common_placeholder_tags() + ",UNDER-UTILIZED=TRUE",
            lt.PARAM_CPU_HIGH_TAGS: testing.tags.common_placeholder_tags() + ",OVER-UTILIZED=TRUE",
            lt.ACTION_PARAM_INTERVAL: interval
        }

        launch_time = self.ec2.get_instance(instance_id)["LaunchTime"].replace(tzinfo=pytz.utc)
        start = date_time_provider().utcnow().replace(tzinfo=pytz.utc, second=0, microsecond=0) - timedelta(
            minutes=INTERVAL_MINUTES + 5)
        if launch_time > start:
            wait_for = launch_time - start + timedelta(minutes=1)
            self.logger.test("Waiting {} for sufficient metrics data to become available for instance ", wait_for, instance_id)
            time.sleep(wait_for.total_seconds())

        self.logger.test("Running task")
        self.task_runner.run(parameters, task_name=test_method)
        self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")
        self.logger.test("[X] Task completed")

        check_result = getattr(self.task_runner.results[0], "ActionResult", {})

        self.assertEqual(check_result["instances-checked"], 1, "Instances checked must be 1")
        self.logger.test("[X] Expected number of instances checked")

        overutilized = check_result.get("overutilized-instances", [])
        self.assertEqual(1 if expect_over_utilized else 0, len(overutilized), "overutilized instance")
        self.logger.test("[X] Instance is {}overutilized", "" if expect_over_utilized else "not ")

        underutilized = check_result.get("underutilized-instances", [])
        self.assertEqual(1 if expect_under_utilized else 0, len(underutilized), "underutilized instance")
        self.logger.test("[X] Instance is {}underutilized", "" if expect_under_utilized else "not ")

        tags = self.ec2.get_instance_tags(instance_id)
        self.assertTrue(testing.tags.verify_placeholder_tags(tags), "Expected tags")

        self.assertEqual(tags.get("UNDER-UTILIZED", "FALSE"), "TRUE" if expect_under_utilized else "FALSE", "underutilized tag")
        self.assertEqual(tags.get("OVER-UTILIZED", "FALSE"), "TRUE" if expect_over_utilized else "FALSE", "overutilized tag")
        self.logger.test("[X] Tags created for instance")

    def test_instance_no_cpu_load(self):

        self.base_test(test_method=inspect.stack()[0][3],
                       instance_id=self.instance_no_cpu_load,
                       expect_under_utilized=True,
                       expect_over_utilized=False,
                       cpu_low=90,
                       cpu_high=99,
                       interval="0/{} * * * ?".format(INTERVAL_MINUTES))

    def test_instance_cpu_load(self):

        self.base_test(test_method=inspect.stack()[0][3],
                       instance_id=self.instance_cpu_load,
                       expect_under_utilized=False,
                       expect_over_utilized=True,
                       cpu_low=5,
                       cpu_high=10,
                       interval="0/{} * * * ?".format(INTERVAL_MINUTES))

    @classmethod
    def tearDownClass(cls):

        if cls.resource_stack is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
