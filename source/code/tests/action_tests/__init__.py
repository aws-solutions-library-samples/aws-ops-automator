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
import os
import os.path

import boto3

import actions
from actions.ec2_copy_snapshot_action import Ec2CopySnapshotAction
from helpers import safe_json, snake_to_pascal_case
from testing import action_stack_name, ENV_TEST_STACK_PREFIX
from testing.ec2 import Ec2
from testing.stack import Stack
from testing.task_test_runner import TaskTestRunner

TESTED_REGION = boto3.Session().region_name
TESTED_REMOTE_REGION = "eu-central-1"
assert (TESTED_REGION != TESTED_REMOTE_REGION)

TASKLIST_TAGNAME = "{}TestTaskList"
RESOURCE_STACK_NAME_TEMPLATE = "{}{}-TestResources"


def template_path(tested_module, template_name):
    return os.path.join(os.path.dirname(tested_module), template_name)


def tasklist_tagname(tested_action):
    return snake_to_pascal_case(TASKLIST_TAGNAME.format(tested_action))


def region():
    return TESTED_REGION


def remote_region():
    return TESTED_REMOTE_REGION


def resources_stack_name(tested_action):
    prefix = os.getenv(ENV_TEST_STACK_PREFIX, "")
    return snake_to_pascal_case(RESOURCE_STACK_NAME_TEMPLATE.format(prefix, tested_action))


def get_resource_stack(tested_action, create_resource_stack_func, use_existing=False, region_name=None):
    stack_region = region_name if region_name is not None else region()
    resource_stack_name = resources_stack_name(tested_action)
    resource_stack = Stack(resource_stack_name, owned=False, region=stack_region)
    if not use_existing or not resource_stack.is_stack_present():
        resource_stack = create_resource_stack_func(resource_stack_name)
        assert (resource_stack is not None)
        resource_stack.owned = True
    return resource_stack


# noinspection PyUnusedLocal
def get_task_runner(tested_action, use_existing_action_stack=False, interval=None):
    stack_name = action_stack_name(tested_action)
    if not use_existing_action_stack:
        action_stack = Stack(stack_name=stack_name, owned=True, region=region())
        if action_stack.is_stack_present():
            action_stack.delete_stack(1800)
    task_runner = TaskTestRunner(action_name=tested_action,
                                 action_stack_name=stack_name,
                                 task_list_tag_name=tasklist_tagname(tested_action),
                                 tested_region=region())
    task_runner.create_stack()
    return task_runner


def set_snapshot_sources_tags(snapshot_id, source_volume_id=None, source_snapshot_id=None, region_name=None):
    tags = {}
    if source_volume_id is not None:
        tags[actions.marker_snapshot_tag_source_source_volume_id()] = source_volume_id
    if source_snapshot_id is not None:
        tags[Ec2CopySnapshotAction.marker_tag_source_snapshot_id()] = source_snapshot_id
    if len(tags) > 0:
        ec2 = Ec2(region_name)
        ec2.create_tags([snapshot_id], tags)


def set_snapshot_copied_tag(snapshot_id, task_name, destination_region, copy_snapshot_id, region_name=None):
    copied_tag_name = Ec2CopySnapshotAction.marker_tag_copied_to(task_name)
    tags = {
        copied_tag_name: safe_json(
            {"region": destination_region,
             "snapshot-id": copy_snapshot_id})
    }
    ec2 = Ec2(region_name)
    ec2.create_tags(snapshot_id, tags)
