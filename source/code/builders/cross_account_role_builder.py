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
import copy
from collections import OrderedDict

import actions
import handlers
import services
import services.aws_service
from boto_retry import get_client_with_retries
from builders import group_name_from_action_name

PARAM_DESC_CUSTOM_ROLENAME = "By default Ops Automator stack \"{}\" will use the role \"{}\" to execute actions in external " \
                             "accounts. It is possible for actions to specify a custom role. Use this parameter to create " \
                             "a role with a custom name. Leave this parameter blank to create a role with the default " \
                             "name \"{}\". Permissions for selected actions will be included in the created cross account " \
                             "role that can be assumed by role {} in account {}"

ACTION_ROLE = "OpsAutomatorActionsRole"

NO = "No"
YES = "Yes"

POLICY_NAME = "P{:0>04d}"
CONDITION_NAME = "C{:0>04d}"

ACTIONS_CROSS_ACCOUNT_ROLE_TEMPLATE = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "",
    "Parameters": {
        "CustomRoleName":
            {
                "Type": "String",
                "Description": PARAM_DESC_CUSTOM_ROLENAME
            }
    },
    "Metadata": dict({
        "AWS::CloudFormation::Interface": {
            "ParameterGroups": [
                {
                    "Label": {
                        "default": "ADVANCED"
                    },
                    "Parameters": [
                        "CustomRoleName"
                    ]
                }
            ],
            "ParameterLabels": {
                "CustomRoleName": {
                    "default": "Custom Rolename"
                }

            }
        }
    }),
    "Conditions": {
        "CustomRoleNameCondition": {
            "Fn::Not": [{
                "Fn::Equals": [
                    {
                        "Ref": "CustomRoleName"
                    },
                    ""
                ]
            }
            ]
        }
    },
    "Resources": {
        ACTION_ROLE: {
            "Type": "AWS::IAM::Role",
            "Properties": {
                "RoleName": "",
                "AssumeRolePolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                "Path": "/"
            }
        }
    },
    "Outputs": {
        "CrossAccountRoleArn": {
            "Value": {
                "Fn::GetAtt": [ACTION_ROLE, "Arn"]
            },
            "Description": "Cross account role for automation actions"
        },

    }
}


class CrossAccountRoleBuilder(object):

    def __init__(self, assume_role_principal_arn, stack_name):
        """
        Initializes the builder instance
        :param assume_role_principal_arn: The arn of the role that can assume the created cross-account role
        """
        self.template = None
        self.assume_role_principal_arn = assume_role_principal_arn
        self.stack_name = stack_name
        self._stack_resources = None
        self.parameters = None
        self.parameter_groups = None
        self.parameter_labels = None
        self.conditions = None
        self.resources = None

    @property
    def stack_resources(self):
        if self._stack_resources is None:
            cfn = get_client_with_retries("cloudformation", ["list_stack_resources"])
            resp = cfn.list_stack_resources_with_retries(StackName=self.stack_name)
            self._stack_resources = {r["LogicalResourceId"]: r["PhysicalResourceId"] for r in resp.get("StackResourceSummaries", [])
                                     if r.get("PhysicalResourceId", None) is not None}
        return self._stack_resources

    def build_template(self, description, action_list, with_conditional_params=False):

        self.template = OrderedDict()
        template_elements = ["AWSTemplateFormatVersion", "Description", "Resources", "Outputs"]
        if with_conditional_params:
            template_elements = template_elements[0:2] + ["Parameters", "Metadata", "Conditions", ] + template_elements[2:]
        for a in template_elements:
            self.template[a] = copy.deepcopy(ACTIONS_CROSS_ACCOUNT_ROLE_TEMPLATE[a])

        self.resources = self.template["Resources"]

        if with_conditional_params:
            self.parameters = self.template["Parameters"]
            self.parameter_groups = self.template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"]
            self.parameter_labels = self.template["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"]

            self.conditions = self.template["Conditions"]
            # noinspection PyTypeChecker
            self.resources[ACTION_ROLE]["Properties"]['RoleName'] = {
                "Fn::If": ["CustomRoleNameCondition", {"Ref": "CustomRoleName"}, handlers.default_rolename_for_stack()]
            }

            name_param_description = self.parameters["CustomRoleName"]["Description"]
            # noinspection PyTypeChecker
            role = self.assume_role_principal_arn.split("/")[-1]
            acct = self.assume_role_principal_arn.split(":")[4]
            # noinspection PyTypeChecker
            self.parameters["CustomRoleName"]["Description"] = name_param_description.format(self.stack_name,
                                                                                             handlers.default_rolename_for_stack(),
                                                                                             handlers.default_rolename_for_stack(),
                                                                                             role, acct)
        else:
            del self.resources[ACTION_ROLE]["Properties"]["RoleName"]

        self.template["Description"] = description

        self._add_assume_role_principal()
        self._add_actions_permissions(action_list, with_conditional_params)

        return self.template

    def _add_assume_role_principal(self):

        action_role = self.template["Resources"][ACTION_ROLE]
        statement = action_role["Properties"]["AssumeRolePolicyDocument"]["Statement"][0]
        statement["Principal"] = {
            "AWS": self.assume_role_principal_arn
        }

    def _add_action_condition_parameter(self, action_name):

        action_properties = actions.get_action_properties(action_name)

        self.parameters[action_name] = {
            "Type": "String",
            "AllowedValues": [YES, NO],
            "Default": NO,
            "Description": "{}".format(action_name)
        }

        self.parameter_labels[action_name] = {"default": action_properties[actions.ACTION_TITLE]}

        group_name = group_name_from_action_name(action_name)

        group = [g for g in self.parameter_groups if g["Label"]["default"] == group_name]
        if not group:
            group = {
                "Label": {
                    "default": group_name
                },
                "Parameters": []
            }
            self.parameter_groups.insert(-1, group)
        else:
            group = group[0]

        group["Parameters"].append(action_name)

        self.conditions[action_name + "Condition"] = {"Fn::Equals": [{"Ref": action_name}, YES]}

    def _add_actions_permissions(self, role_actions, with_conditional_param=False):

        def action_select_resources_permissions(action_props):
            return services.get_resource_describe_permissions(action_props[actions.ACTION_SERVICE],
                                                              [action_props[actions.ACTION_RESOURCES]])

        for action_name in sorted(role_actions):

            action_properties = actions.get_action_properties(action_name)
            if action_properties.get(actions.ACTION_INTERNAL, False):
                continue

            if with_conditional_param:
                self._add_action_condition_parameter(action_name)

            # get permissions from action properties
            action_permissions = action_properties.get(actions.ACTION_PERMISSIONS, [])
            # get the permissions to retrieve the resources for that action
            # with possible additional permissions to retrieve tags
            action_permissions += action_select_resources_permissions(action_properties)

            if action_properties.get(
                    actions.ACTION_SERVICE) in services.aws_service.SERVICES_SUPPORTED_BY_RESOURCEGROUP_TAGGING_API:
                action_permissions.append("tag:GetResources")

            if len(action_permissions) > 0:
                policy = {
                    "Type": "AWS::IAM::Policy",
                    "Properties": {
                        "PolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [
                                {
                                    "Effect": "Allow",
                                    "Resource": "*",
                                    "Action": list(set(action_permissions))
                                }
                            ]
                        },
                        "PolicyName": action_name,
                        "Roles": [{"Ref": ACTION_ROLE}]
                    }
                }

                if with_conditional_param:
                    policy["Condition"] = action_name + "Condition"

                self.resources[action_name + "Policy"] = policy

    @staticmethod
    def compress_template(source_template):

        def build_policy(condition_actions, ops_automator_role, policy_number):
            policy = {
                "Type": "AWS::IAM::Policy",
                "Properties": {
                    "PolicyName": POLICY_NAME.format(policy_number),
                    "PolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Action": list(condition_actions[1]),
                                "Resource": "*",
                                "Effect": "Allow"
                            }
                        ]
                    },
                    "Roles": [
                        {
                            "Ref": ops_automator_role
                        }
                    ]
                }
            }
            return policy

        def condition_groups(condition_list):
            conditions = copy.deepcopy(condition_list)

            batch_list = []
            while len(conditions) > 0:
                i = conditions.pop()
                batch_list.append(i)
                if len(batch_list) == 10 or len(batch_list) == 9 and len(conditions) == 2:
                    yield batch_list
                    batch_list = []

            if len(batch_list) > 0:
                yield batch_list

        def build_compressed_condition_to_actions_map(actions_conditions, policies):
            for p in policies:

                policy_actions = policies[p]["Properties"]["PolicyDocument"]["Statement"][0]["Action"]

                # build dict of actions mapped to conditions
                for action in policy_actions:
                    if action not in actions_conditions:
                        actions_conditions[action] = set()
                    actions_conditions[action].add(policies[p]["Condition"])
            # build list of sets of conditions mapped to the actions
            conditions_actions = []
            for u in actions_conditions:
                conditions_actions.append((actions_conditions[u], {u}))
            # sort, longest conditions set first
            conditions_actions = sorted(conditions_actions, key=lambda l: len(l[0]), reverse=True)
            # compressed table,  combines condition sets and actions
            compressed_conditions_actions = []
            for ca in conditions_actions:
                for cca in compressed_conditions_actions:
                    if cca[0].issubset(ca[0]) or cca[0] == ca[0]:
                        cca[1].update(ca[1])
                        break
                else:
                    compressed_conditions_actions.append(ca)
            return compressed_conditions_actions

        template = copy.deepcopy(source_template)

        resources = OrderedDict()
        resources.update(template.get("Resources", {}))

        conditions = OrderedDict()
        conditions.update(template.get("Conditions", {}))

        policies = {r: resources[r] for r in resources if resources[r]["Type"] == "AWS::IAM::Policy"}

        ops_automator_role = [r for r in resources if resources[r]["Type"] == "AWS::IAM::Role"][0]

        # maps actions to conditions
        actions_conditions = {}

        compressed_conditions_actions = build_compressed_condition_to_actions_map(actions_conditions, policies)

        condition_number = 0
        policy_number = 0

        for p in policies:
            del resources[p]

        for condition_actions in compressed_conditions_actions:

            policy = build_policy(condition_actions, ops_automator_role, policy_number)

            if len(condition_actions[0]) == 1:
                policy["Condition"] = list(condition_actions[0])[0]

            else:

                conditions_groupings = [c for c in condition_groups(condition_actions[0])]
                for cg in conditions_groupings:
                    conditions[CONDITION_NAME.format(condition_number)] = {
                        "Fn::Or": [{"Condition": c} for c in cg]
                    }
                    condition_number += 1

                if len(conditions_groupings) > 1:
                    conditions[CONDITION_NAME.format(condition_number)] = {
                        "Fn::Or": [{"Condition": CONDITION_NAME.format(cg)} for cg in
                                   range(condition_number - len(conditions_groupings), condition_number)]
                    }
                    condition_number += 1

                policy["Condition"] = CONDITION_NAME.format(condition_number - 1)

            resources[POLICY_NAME.format(policy_number)] = policy

            policy_number += 1

        template["Resources"] = resources
        template["Conditions"] = conditions

        return template
