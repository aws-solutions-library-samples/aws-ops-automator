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

import os

import helpers

OPS_AUTOMATOR_ROLE_NAME = "OpsAutomatorRole"
ACTION_STACK_NAME_TEMPLATE = "{}{}-ActionResources"
ACTION_STACK_ROLE_NAME_TEMPLATE = "{}{}-TestRole"
ENV_TEST_STACK_PREFIX = "TEST_STACK_PREFIX"


def action_stack_name(tested_action):
    prefix = os.getenv(ENV_TEST_STACK_PREFIX, "")
    return helpers.snake_to_pascal_case(ACTION_STACK_NAME_TEMPLATE.format(prefix, tested_action))


def assumed_test_role_name(tested_action):
    prefix = os.getenv(ENV_TEST_STACK_PREFIX, "")
    return helpers.snake_to_pascal_case(ACTION_STACK_ROLE_NAME_TEMPLATE.format(prefix, tested_action))


TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "OpsAutomatorRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "RoleName": "",
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": ""
                            },
                            "Action": "sts:AssumeRole"
                        }

                    ]
                },
                "Policies": [
                    {
                        "PolicyName": "OpsAutomatorRolePolicy",
                        "PolicyDocument": {'Statement': []}}]
            }
        }
    }
}
