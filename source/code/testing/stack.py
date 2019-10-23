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
import time

import boto3

import services
import services.cloudformation_service
from helpers.timer import Timer
from testing.s3 import S3


class Stack(object):

    def __init__(self, stack_name, region=None, owned=True):
        self._stack_id = None
        self._stack_outputs = None
        self._stack_resources = None
        self.region = region if region is not None else boto3.Session().region_name
        self.cfn_client = boto3.Session().client("cloudformation", region_name=self.region)
        self.cnf_service = services.create_service("cloudformation")

        self.owned = owned
        self.stack_name = stack_name

    @property
    def stack_id(self):
        if self._stack_id is None:
            stacks = self.cnf_service.describe(services.cloudformation_service.STACKS_SUMMARY)
            self._stack_id = [s for s in stacks
                              if s.get("StackName", "") == self.stack_name and s["StackStatus"] != "DELETE_COMPLETE"][0]["StackId"]
        return self._stack_id

    @property
    def stack_outputs(self):
        if self._stack_outputs is None:
            self._stack_outputs = {}
            for i in self.cfn_client.describe_stacks(StackName=self.stack_name)['Stacks'][0]['Outputs']:
                self._stack_outputs[i["OutputKey"]] = i["OutputValue"]
        return self._stack_outputs

    @property
    def stack_resources(self):
        if self._stack_resources is None:
            self._stack_resources = {}

            args = {"StackName": self.stack_id}
            while True:
                cfn_resp = self.cfn_client.list_stack_resources(**args)
                for res in cfn_resp.get("StackResourceSummaries", []):
                    self._stack_resources[res["LogicalResourceId"]] = res

                if "NextToken" in cfn_resp:
                    args["NextToken"] = cfn_resp["NextToken"]
                else:
                    break
        return self._stack_resources

    def create_stack(self, template_body=None, template_file=None, iam_capability=False, timeout=600, tags=None, params=None,
                     empty_existing_buckets=True):

        assert (len([t for t in [template_body, template_file] if t is not None]) == 1)

        if template_file is not None:
            with open(template_file, "rt") as f:
                template = "".join(f.readlines())
        else:
            template = template_body

        self.delete_stack(empty_bucket_resources=empty_existing_buckets)

        args = {
            "StackName": self.stack_name,
            "TemplateBody": template,
            "Parameters": [] if params is None else [{"ParameterKey": p, "ParameterValue": params[p]} for p in params],
            "Capabilities": ["CAPABILITY_NAMED_IAM"] if iam_capability else [],
            "Tags": [{"Key": t, "Value": tags[t]} for t in tags] if tags is not None else []

        }

        try:
            self._stack_id = self.cfn_client.create_stack(**args)["StackId"]
        except Exception as ex:
            print(ex)

        with Timer(timeout_seconds=timeout, start=True) as timer:
            while self.is_stack_in_status("CREATE_IN_PROGRESS") is True:
                time.sleep(20)
                if timer.timeout:
                    raise Exception("Timeout creating stack {}".format(self.stack_name))

        if self.is_stack_in_status("CREATE_COMPLETE") is True:
            self.owned = True
            return
        else:
            raise ValueError("Stack did not create successfully")

    def is_stack_in_status(self, status):
        return self.get_stack_status() == status

    def get_stack(self):
        # noinspection PyBroadException,PyPep8
        try:
            return self.cfn_client.describe_stacks(StackName=self.stack_name).get("Stacks", [None])[0]
        except:
            return None

    def get_stack_tags(self):
        stack = self.get_stack()
        if stack is None:
            return None

        return {t["Key"]: t["Value"] for t in stack.get("Tags", [])}

    def get_stack_status(self):
        stack = self.get_stack()
        if stack is None:
            return None
        return stack['StackStatus']

    def is_stack_present(self):

        try:
            resp = self.cfn_client.describe_stacks(StackName=self.stack_name)
            stacks = resp.get("Stacks", [])
            # double check for deleted stacks in case a stack id was used
            return any([s["StackStatus"] != "DELETE_COMPLETE" for s in stacks])
        except Exception as ex:
            if ex.message.endswith("does not exist"):
                return False
            raise ex

    def get_stack_policy(self):
        return self.cfn_client.get_stack_policy(StackName=self.stack_name).get("StackPolicyBody", None)

    def wait_until_not_longer_in_status(self, status, timeout=900):
        current_status = self.get_stack_status()
        with Timer(timeout_seconds=timeout, start=True) as timer:
            while True:
                if type(status) == list:
                    if current_status not in status:
                        break
                else:
                    if current_status != status:
                        break
                if timer.timeout:
                    raise Exception("Timeout waiting stack {} to get out of status {}".format(self.stack_name, status))
                time.sleep(20)

    def delete_stack(self, timeout=900, empty_bucket_resources=False):
        if self.owned:
            if not self.is_stack_present():
                return True
            else:
                if empty_bucket_resources:
                    s3 = S3()
                    buckets = [r["PhysicalResourceId"] for r in
                               self.cnf_service.describe(services.cloudformation_service.STACK_RESOURCES,
                                                         StackName=self.stack_name,
                                                         region=self.region) if r["ResourceType"] == "AWS::S3::Bucket"]
                    for bucket in buckets:
                        s3.empty_bucket(bucket)
                with Timer(timeout_seconds=timeout, start=True) as timer:
                    self.cfn_client.delete_stack(StackName=self.stack_name)
                    while self.is_stack_present() is True:
                        if timer.timeout:
                            raise Exception("Timeout deleting stack {}".format(self.stack_name))
                        time.sleep(20)
                    return True
