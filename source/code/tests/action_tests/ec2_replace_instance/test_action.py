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
from types import FunctionType

import jmespath

import actions.ec2_replace_instance_action as replace_instance
import handlers.ec2_tag_event_handler
import services.ec2_service
import tagging
import testing.tags
from helpers import safe_json
from tagging import tag_key_value_list
from testing.console_logger import ConsoleLogger
from testing.ec2 import Ec2
from testing.elb import Elb
from testing.elbv2 import ElbV2
from testing.stack import Stack
from tests.action_tests import get_resource_stack, get_task_runner, region, tasklist_tagname, template_path

TESTED_ACTION = "Ec2ReplaceInstance"
TEST_RESOURCES_TEMPLATE = "test_resources.template"

KEEP_AND_USE_EXISTING_ACTION_STACK = False
KEEP_AND_USE_EXISTING_RESOURCES_STACK = False

TEST_INSTANCE_TYPES = ["t2.micro", "t2.small", "t2.medium", "t2.large"]

EXTRA_VOLUMES = 2

JMESPATH_CHECKED_INSTANCE_DATA = \
    "{" \
    "   ImageId:ImageId, " \
    "   KeyName:KeyName, " \
    "   Placement:Placement, " \
    "   SubnetId:SubnetId," \
    "   VpcId:VpcId, " \
    "   Devices:sort_by(BlockDeviceMappings,&DeviceName)[].DeviceName, " \
    "   IamInstanceProfileArn:IamInstanceProfile.Arn," \
    "   NetworkInterfaces:length(NetworkInterfaces)" \
    "}"


class TestAction(unittest.TestCase):
    logger = None
    task_runner = None
    resource_stack = None
    ec2 = None
    v1_elb_name = None
    v2_target_group_arn = None
    key_pair = None
    instance_profile = None
    ami_single_volume = None
    ami_multiple_volumes = None

    def __init__(self, method_name):
        unittest.TestCase.__init__(self, method_name)
        self.replaced_instance_id = None
        self.replaced_instance = None
        self.new_instance_id = None
        self.new_instance = None
        self.tags_on_replaced_instance = None
        self.start_type = None

    @classmethod
    def get_methods(cls):
        return [x for x, y in cls.__dict__.items() if type(y) == FunctionType and x.startswith("test_")]

    @classmethod
    def setUpClass(cls):

        cls.ec2 = Ec2(region())
        cls.elb = Elb(region())
        cls.elbv2 = ElbV2(region())
        cls.logger = ConsoleLogger()

        cls.resource_stack = get_resource_stack(TESTED_ACTION,
                                                create_resource_stack_func=cls.create_resource_stack,
                                                use_existing=KEEP_AND_USE_EXISTING_RESOURCES_STACK,
                                                region_name=region())

        assert (cls.resource_stack is not None)

        cls.v1_elb_name = cls.resource_stack.stack_outputs["V1LoadBalancerName"]
        cls.v2_target_group_arn = cls.resource_stack.stack_outputs["V2TargetGroupArn"]
        cls.instance_profile = cls.resource_stack.stack_outputs["InstanceProfile"]

        # noinspection PyPep8,PyBroadException
        try:
            cls.ec2.ec2_service.get(services.ec2_service.KEY_PAIRS, KeyNames=[TESTED_ACTION], region=region())
            cls.key_pair = TESTED_ACTION
        except:
            cls.key_pair = cls.ec2.ec2_client.create_key_pair(KeyName=TESTED_ACTION).get("KeyName")

        cls.ami_single_volume = cls.ec2.latest_aws_linux_image["ImageId"]

        cls.task_runner = get_task_runner(TESTED_ACTION, KEEP_AND_USE_EXISTING_ACTION_STACK)

    @classmethod
    def create_resource_stack(cls, resource_stack_name):
        try:
            cls.logger.test("Creating test resources stack {}", resource_stack_name)
            resource_stack = Stack(resource_stack_name, region=region())
            default_vpc_id = cls.ec2.get_default_vpc()
            assert default_vpc_id is not None
            resource_stack.create_stack(template_file=template_path(__file__, TEST_RESOURCES_TEMPLATE),
                                        timeout=1200,
                                        iam_capability=True,
                                        params={
                                            "VpcId": default_vpc_id["VpcId"],
                                        })
            return resource_stack
        except Exception as ex:
            cls.logger.test("Error creating stack {}, {}", resource_stack_name, ex)
            return None

    @classmethod
    def get_multi_volume_ami(cls):
        if cls.ami_multiple_volumes is None:
            instance = None
            instance_id = None
            volumes = []
            try:
                cls.logger.test("Building multi volume AMI based on AMI {}", cls.ami_single_volume)
                cls.logger.test("Starting temporary instance")
                instance = cls.ec2.create_instance(instance_type=TEST_INSTANCE_TYPES[0], image_id=cls.ami_single_volume)
                instance_id = instance["InstanceId"]
                cls.logger.test("Temporary instance {} created", instance_id)
                for i in range(0, EXTRA_VOLUMES):
                    vol = cls.ec2.add_instance_volume(instance_id=instance_id)
                    volumes.append(vol)
                    cls.logger.test("Adding volume {}", vol["VolumeId"])
                cls.logger.test("Stopping instance and creating multi volume AMI")
                cls.ec2.stop_instance(instance_id=instance_id)
                cls.ec2.set_all_volumes_to_delete_on_terminate(instance_id=instance_id)

                cls.ami_multiple_volumes = cls.ec2.create_image(instance_id=instance_id,
                                                                name=TESTED_ACTION + "-" + instance_id,
                                                                no_reboot=True)["ImageId"]

                cls.logger.test("AMI {} created", cls.ami_multiple_volumes)

            finally:
                cls.logger.test("Deleting temporary instance and volumes")
                if instance is not None:
                    cls.ec2.terminate_instance(instance_id=instance_id)
                for v in volumes:
                    cls.ec2.delete_volume(volume_id=v["VolumeId"], forced=True)

        return cls.ami_multiple_volumes

    def do_test_replace(self,
                        test_method,
                        load_balancing=False,
                        multiple_volumes=False,
                        same_volume_tags=False,
                        expected_new_type=None,
                        unavailable_types=None,
                        try_next_in_range=True,
                        same_type=False,
                        replace_if_same_type=False,
                        mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                        replaced_type=None,
                        tags=None,
                        assumed_type=None,
                        stopped_instance=False):

        def create_replaced_instance():

            def register_to_load_balancers():
                self.elb.register_instance(load_balancer_name=self.v1_elb_name, instance_id=self.replaced_instance_id)
                self.elbv2.register_instance(target_group_arn=self.v2_target_group_arn, instance_id=self.replaced_instance_id)

            def tag_instance_volumes():

                volumes = list(self.ec2.get_instance_volumes(self.replaced_instance_id))

                if same_volume_tags:
                    volume_ids = [v["VolumeId"] for v in volumes]
                    vol_tags = {"Tag": TESTED_ACTION}
                    self.ec2.ec2_client.create_tags(Resources=volume_ids, Tags=tag_key_value_list(vol_tags))
                    self.tags_on_replaced_instance = {v["Attachments"][0]["Device"]: vol_tags for v in volumes}

                else:
                    self.tags_on_replaced_instance = {}
                    for v in volumes:
                        dev = v["Attachments"][0]["Device"]
                        vol_tags = {"Tag": dev}
                        self.ec2.ec2_client.create_tags(Resources=[v["VolumeId"]], Tags=tag_key_value_list(vol_tags))
                        self.tags_on_replaced_instance[dev] = vol_tags

            ami = self.get_multi_volume_ami() if multiple_volumes else self.ami_single_volume
            self.logger.test("Start creating replaced instance from AMI {}", ami)

            if replaced_type is not None:
                self.start_type = replaced_type
            else:
                self.start_type = TEST_INSTANCE_TYPES[0 if mode == replace_instance.REPLACE_BY_SPECIFIED_TYPE else 1]

            instance_tags = {
                "Name": test_method,
                tasklist_tagname(TESTED_ACTION): test_method
            }

            if tags is not None:
                instance_tags.update(tags)

            self.replaced_instance = self.ec2.create_instance(self.start_type,
                                                              image_id=ami,
                                                              key_pair=self.key_pair,
                                                              role_name=self.instance_profile,
                                                              tags=instance_tags)

            self.replaced_instance_id = self.replaced_instance["InstanceId"]
            self.logger.test("Created instance {}", self.replaced_instance_id)
            tag_instance_volumes()
            self.new_instance_id = None
            if load_balancing:
                self.logger.test("Registering instance to load balancer(s)")
                register_to_load_balancers()

            if stopped_instance:
                self.ec2.stop_instance(self.replaced_instance_id)

        def check_instance_volumes_tags():
            volumes = list(self.ec2.get_instance_volumes(self.new_instance_id))
            actual_tags = {v["Attachments"][0]["Device"]: v.get("Tags", {}) for v in volumes}
            for v in volumes:
                dev = v["Attachments"][0]["Device"]
                if self.tags_on_replaced_instance.get(dev, {}) != actual_tags.get(dev):
                    self.fail("Tags  not set correctly on volume {}".format(v))

        def check_load_balancer_registrations():
            load_balancers = self.elb.get_instance_load_balancers(self.new_instance_id)
            target_groups = self.elbv2.get_instance_target_groups(self.new_instance_id)

            if load_balancing:
                if load_balancers != [self.v1_elb_name] or target_groups != [self.v2_target_group_arn]:
                    self.fail("Instance {} not registered to the expected load balancer {} or target group {}".format(
                        self.new_instance_id,
                        self.v1_elb_name,
                        self.v2_target_group_arn))
            else:
                if load_balancers != [] or target_groups != []:
                    self.fail("Instance {} must not be registered to the load balancer {} or target group {}".format(
                        self.new_instance_id,
                        self.v1_elb_name,
                        self.v2_target_group_arn))

        def validate_new_instance():

            replaced_state = self.ec2.get_instance(instance_id=self.replaced_instance_id)["State"]["Code"] & 0xFF

            if self.new_instance_id is None:

                if expected_new_type != self.replaced_instance["InstanceType"] or replace_if_same_type:
                    self.fail("Instance was expected to be replaced")

                if replaced_state in [replace_instance.EC2_STATE_TERMINATED, replace_instance.EC2_STATE_SHUTTING_DOWN,
                                      replace_instance.EC2_STATE_STOPPING]:
                    self.fail("Instance was {} not replaced but is not longer running".format(self.replaced_instance_id))

                self.logger.test("[X] Instance was not replaced as expected")
                return True

            if replaced_state == replace_instance.EC2_STATE_RUNNING:
                self.fail("Replaced instance {} is still running".format(self.replaced_instance_id))

            replaced_instance_checked_data = jmespath.search(JMESPATH_CHECKED_INSTANCE_DATA, self.replaced_instance)
            new_instance_checked_data = jmespath.search(JMESPATH_CHECKED_INSTANCE_DATA, self.new_instance)
            if replaced_instance_checked_data != new_instance_checked_data:
                self.fail("Instance data is {}, expected {}".format(
                    safe_json(new_instance_checked_data, indent=3),
                    safe_json(replaced_instance_checked_data, indent=3)))

            # instance id must be different if the type was not the same or replaced if same type
            if self.replaced_instance_id == self.new_instance_id:
                if not same_type or replace_if_same_type:
                    self.fail("InstanceId {} op new instance should not be the same as Id of replaces instance".format(
                        self.replaced_instance_id))

            new_instance_state = self.new_instance["State"]["Code"] & 0xFF
            if stopped_instance:
                if not replace_instance.Ec2ReplaceInstanceAction.is_in_stopping_or_stopped_state(new_instance_state):
                    self.fail("New Instance {} is not in stopping or stopped state, {}".format(self.new_instance_id,
                                                                                               new_instance_state))

            else:
                if replace_instance.EC2_STATE_RUNNING != new_instance_state:
                    self.fail("New instance {} is not in running, {}".format(self.new_instance_id, new_instance_state))

            check_load_balancer_registrations()

            instance_placeholders = {
                replace_instance.TAG_PLACEHOLDER_ORG_INSTANCE_ID: self.replaced_instance_id,
                replace_instance.TAG_PLACEHOLDER_NEW_INSTANCE_TYPE: self.new_instance["InstanceType"],
                replace_instance.TAG_PLACEHOLDER_ORG_INSTANCE_TYPE: self.start_type
            }

            new_instance_tags = self.new_instance.get("Tags", {})
            if not testing.tags.verify_placeholder_tags(
                    new_instance_tags, instance_placeholders, exclude_tags=["tag-" + tagging.TAG_VAL_TASK]) or \
                    new_instance_tags.get("Name") != test_method or \
                    new_instance_tags.get("scaling", None) is not None:
                self.fail("Expected tags not set on new instance")

            time.sleep(15)
            check_instance_volumes_tags()

            checked_type = expected_new_type if expected_new_type is not None else \
                self.get_new_instance_types(self.replaced_instance_id, same_type)[0]
            if self.new_instance["InstanceType"] != checked_type:
                self.fail("Type of instance {} is {} but expected type {}".format(
                    self.new_instance_id, self.new_instance["InstanceType"],
                    checked_type))

        create_replaced_instance()
        time.sleep(10)

        try:
            new_sizes = self.get_new_instance_types(self.replaced_instance_id, same_type)

            parameters = {
                replace_instance.PARAM_NEW_INSTANCE_TAGS: testing.tags.common_placeholder_tags(
                    placeholders=[replace_instance.TAG_PLACEHOLDER_NEW_INSTANCE_TYPE,
                                  replace_instance.TAG_PLACEHOLDER_ORG_INSTANCE_ID,
                                  replace_instance.TAG_PLACEHOLDER_ORG_INSTANCE_TYPE]) + ",scaling={delete}",
                replace_instance.PARAM_INSTANCE_TYPES: new_sizes,
                replace_instance.PARAM_COPIED_INSTANCE_TAGS: "*",
                replace_instance.PARAM_REPLACE_MODE: mode,
                replace_instance.PARAM_REPLACE_IF_SAME_TYPE: replace_if_same_type
            }

            if mode == replace_instance.REPLACE_BY_SPECIFIED_TYPE:
                parameters[replace_instance.PARAM_REPLACE_IF_SAME_TYPE] = replace_if_same_type
            else:
                parameters[replace_instance.PARAM_SCALING_RANGE] = TEST_INSTANCE_TYPES
                parameters[replace_instance.PARAM_TAGFILTER_SCALE_UP] = "scaling=up"
                parameters[replace_instance.PARAM_TAGFILTER_SCALE_DOWN] = "scaling=down"
                if assumed_type is not None:
                    parameters[replace_instance.PARAM_ASSUMED_TYPE] = assumed_type
                parameters[replace_instance.PARAM_TRY_NEXT_IN_RANGE] = try_next_in_range

            if unavailable_types is not None:
                parameters[replace_instance.PARAM_TEST_UNAVAILABLE_TYPES] = unavailable_types

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
                                 action_select_params={"InstanceIds": [self.replaced_instance_id]},
                                 events=events)
            self.assertTrue(self.task_runner.success(expected_executed_tasks=1), "Task executed successfully")

            self.logger.test("[X] Task completed")

            self.new_instance_id = self.task_runner.executed_tasks[0].ActionResult.get("new-instance")
            self.new_instance = self.ec2.get_instance(self.new_instance_id) if self.new_instance_id is not None else None

            validate_new_instance()
            self.logger.test("[X] Checked new EC2 instance")

        finally:
            for i in [self.replaced_instance_id, self.new_instance_id]:
                if i is not None:
                    self.ec2.terminate_instance(instance_id=i)

    def get_new_instance_types(self, inst_id, same_type=False):
        current_type = self.ec2.get_instance(instance_id=inst_id)["InstanceType"]
        if same_type:
            return [current_type]
        return [t for t in TEST_INSTANCE_TYPES if t != current_type]

    def test_scale_up(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             expected_new_type=TEST_INSTANCE_TYPES[2])

    def test_scale_up_already_largest(self):
        largest_size = TEST_INSTANCE_TYPES[-1]
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             replaced_type=largest_size,
                             expected_new_type=largest_size)

    def test_scale_up_not_avail_next(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             replaced_type=TEST_INSTANCE_TYPES[0],
                             unavailable_types=[TEST_INSTANCE_TYPES[1]],
                             expected_new_type=TEST_INSTANCE_TYPES[2])

    def test_scale_up_not_avail(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             replaced_type=TEST_INSTANCE_TYPES[0],
                             unavailable_types=[TEST_INSTANCE_TYPES[1:]],
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    def test_scale_up_not_avail_no_next(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             replaced_type=TEST_INSTANCE_TYPES[0],
                             unavailable_types=[TEST_INSTANCE_TYPES[1:]],
                             try_next_in_range=False,
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    def test_scale_up_assumed(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             replaced_type="t2.nano",
                             assumed_type=TEST_INSTANCE_TYPES[0],
                             expected_new_type=TEST_INSTANCE_TYPES[1])

    def test_scale_up_not_assumed(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "up"},
                             multiple_volumes=False,
                             replaced_type="t2.nano",
                             expected_new_type="t2.nano")

    def test_scale_down(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "down"},
                             multiple_volumes=False,
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    def test_scale_down_already_smallest(self):
        smallest_size = TEST_INSTANCE_TYPES[0]
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "down"},
                             multiple_volumes=False,
                             replaced_type=smallest_size,
                             expected_new_type=smallest_size)

    def test_scale_down_not_avail_next(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "down"},
                             multiple_volumes=False,
                             replaced_type=TEST_INSTANCE_TYPES[2],
                             unavailable_types=[TEST_INSTANCE_TYPES[1]],
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    def test_scale_down_not_avail_no_next(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "down"},
                             multiple_volumes=False,
                             try_next_in_range=False,
                             replaced_type=TEST_INSTANCE_TYPES[2],
                             unavailable_types=[TEST_INSTANCE_TYPES[1]],
                             expected_new_type=TEST_INSTANCE_TYPES[2])

    def test_scale_down_not_avail(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_STEP,
                             tags={"scaling": "down"},
                             multiple_volumes=False,
                             replaced_type=TEST_INSTANCE_TYPES[2],
                             unavailable_types=TEST_INSTANCE_TYPES[0:2],
                             expected_new_type=TEST_INSTANCE_TYPES[2])

    def test_not_load_balanced_single_volume(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=False,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             multiple_volumes=False)

    def test_load_balanced_single_volume(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             multiple_volumes=False)

    def test_load_balanced_multiple_volumes_mixed_tags(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             same_volume_tags=False,
                             multiple_volumes=True)

    def test_load_balanced_multiple_volumes_same_tags(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             same_volume_tags=True,
                             multiple_volumes=True)

    def test_load_balanced_alternative_type(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             unavailable_types=TEST_INSTANCE_TYPES[1:2],
                             expected_new_type=TEST_INSTANCE_TYPES[2])

    def test_load_balanced_no_alternative_types_available(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             unavailable_types=TEST_INSTANCE_TYPES[1:],
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    def test_load_balanced_same_size_no_replace(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             same_type=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             replace_if_same_type=False,
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    def test_load_balanced_same_size_replace(self):
        self.do_test_replace(test_method=inspect.stack()[0][3],
                             load_balancing=True,
                             same_type=True,
                             mode=replace_instance.REPLACE_BY_SPECIFIED_TYPE,
                             replace_if_same_type=False,
                             expected_new_type=TEST_INSTANCE_TYPES[0])

    @classmethod
    def tearDownClass(cls):

        if cls.resource_stack is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.resource_stack.delete_stack()

        if cls.task_runner is not None:
            cls.task_runner.cleanup(KEEP_AND_USE_EXISTING_ACTION_STACK)

        if cls.ami_multiple_volumes is not None:
            cls.ec2.delete_images([cls.ami_multiple_volumes])

        if cls.key_pair is not None and not KEEP_AND_USE_EXISTING_RESOURCES_STACK:
            cls.ec2.delete_key_pair(cls.key_pair)

    def setUp(self):
        pass

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
