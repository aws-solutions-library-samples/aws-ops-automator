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
import json
import sys

import boto3


def add_me_to_role(stack, principal):
    """
    Add role to role

    Args:
        stack: (todo): write your description
        principal: (str): write your description
    """
    role_resource = boto3.client("cloudformation").describe_stack_resource(
        StackName=stack, LogicalResourceId="OpsAutomatorLambdaRole").get("StackResourceDetail", None)

    role_name = role_resource["PhysicalResourceId"]

    role = boto3.client("iam").get_role(RoleName=role_name).get("Role", {})
    assume_role_policy_document = role.get("AssumeRolePolicyDocument", {})
    statement = assume_role_policy_document.get("Statement", [])

    for s in statement:
        if s["Principal"].get("AWS", "") == principal:
            break
    else:
        statement.append({"Action": "sts:AssumeRole", "Effect": "Allow", "Principal": {"AWS": principal}})
        boto3.client("iam").update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(assume_role_policy_document)
        )
        print(("Principal {} can now assume role {}".format(principal, role_name)))


if __name__ == "__main__":

    if len(sys.argv) < 1:
        print("No stack name argument passed as first parameter")
        exit(1)

    principal = boto3.client("sts").get_caller_identity()["Arn"]
    print(("Adds {} to AssumeRolePolicyDocument of Ops Automator role defined in stack {} for local debugging".format(
          principal, sys.argv[1])))

    add_me_to_role(sys.argv[1], principal)

    print("Done...")
    exit(0)
