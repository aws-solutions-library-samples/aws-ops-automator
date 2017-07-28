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

import copy
import re
import uuid

import actions
import services

ACTIONS_CROSS_ACCOUNT_ROLE_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Resources": {
        "ActionRole": {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "Policies": [
                    {
                        "PolicyName": "SchedulerPolicy",
                        "PolicyDocument": {
                            "Version": "2012-10-17"
                        }
                    }
                ],
                "Path": "/"
            }
        }
    },
    "Outputs": {
        "CrossAccountRoleArn": {
            "Value": {
                "Fn::GetAtt": ["ActionRole", "Arn"]
            },
            "Description": "Cross account role for automation actions"
        },

    }
}


class CrossAccountRoleBuilder:
    """
    Creates cloudformation template for cross-account roles for actions
    """

    def __init__(self, assume_role_principal_arn):
        """
        Initializes the builder instance
        :param assume_role_principal_arn: The arn of the role that can assume the created cross-account role
        """
        self.template = None
        self.assume_role_principal_arn = assume_role_principal_arn

    def build_template(self, description, role_actions):
        """
        Builds a cloudformation template to allow cross-account access for the specified actions
        :param description: description to use in the template
        :param role_actions: actions to include in the policy document of the role created by the template
        :return: CloudFormation template as dictionary
        """

        self.template = copy.deepcopy(ACTIONS_CROSS_ACCOUNT_ROLE_TEMPLATE)
        self.template["Description"] = description
        self._add_assume_role_principal()
        self._add_actions_permissions(role_actions)

        return self.template

    def _add_assume_role_principal(self):
        """
        Adds principal of role to to AssumeRolePolicyDocument
        :return: 
        """
        action_role = self.template["Resources"]["ActionRole"]
        statement = action_role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]
        statement["Principal"] = {"AWS": self.assume_role_principal_arn}

    def _add_actions_permissions(self, role_actions):
        """
        Adds permissions to cross account role statement for list of actions
        :param role_actions: List of actions
        :return: 
        """

        def action_select_resources_permissions(action_props):
            return services.get_resource_describe_permissions(action_props[actions.ACTION_SERVICE],
                                                              action_props[actions.ACTION_RESOURCES])

        action_role = self.template["Resources"]["ActionRole"]
        policy_document = action_role["Properties"]["Policies"][0]["PolicyDocument"]
        policy_document_statement = []
        policy_document["Statement"] = policy_document_statement
        for action_name in role_actions:

            action_properties = actions.get_action_properties(action_name)
            if action_properties.get(actions.ACTION_INTERNAL, False):
                continue

            # get permissions from action properties
            action_permissions = action_properties.get(actions.ACTION_PERMISSIONS, [])
            # get the permissions to retrieve the resources for that action
            # with possible additional permissions to retrieve tags
            action_permissions += action_select_resources_permissions(action_properties)

            if len(action_permissions) is not 0:

                policy_document_statement.append({
                    "Sid": re.sub("[^0-9A-Za-z]", "", action_name + str(uuid.uuid4())),
                    "Effect": "Allow",
                    "Resource": "*",
                    "Action": list(set(action_permissions))
                })
