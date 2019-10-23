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

import boto3

import services.elbv2_service


class ElbV2(object):

    def __init__(self, region=None, session=None):
        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.elbv2_client = self.session.client("elbv2", region_name=self.region)
        self.elbv2_service = services.elbv2_service.Elbv2Service(session=self.session)

    def register_instance(self, target_group_arn, instance_id, port=None, availability_zone=None):
        target = {
            "Id": instance_id
        }
        if port is not None:
            target["Port"] = port

        if availability_zone is not None:
            target["AvailabilityZone"] = availability_zone

        self.elbv2_client.register_targets(TargetGroupArn=target_group_arn, Targets=[target])

    def get_instance_target_groups(self, instance_id):

        result = []

        args = {
            "service_resource": services.elbv2_service.TARGET_GROUPS,
            "region": self.region,

        }

        target_groups = list(self.elbv2_service.describe(**args))
        for target_group in target_groups:
            target_group_healths = list(self.elbv2_service.describe(services.elbv2_service.TARGET_HEALTH,
                                                                    TargetGroupArn=target_group["TargetGroupArn"]))
            for target_group_health in target_group_healths:
                target = target_group_health["Target"]
                if target["Id"] != instance_id:
                    continue
                result.append(target_group.get("TargetGroupArn"))

        return result
