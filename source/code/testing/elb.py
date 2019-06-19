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


import boto3

import services.elb_service


class Elb(object):

    def __init__(self, region=None, session=None):
        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.elb_client = self.session.client("elb", region_name=self.region)
        self.elb_service = services.elb_service.ElbService(session=self.session)

    def register_instance(self, load_balancer_name, instance_id):
        self.elb_client.register_instances_with_load_balancer(LoadBalancerName=load_balancer_name,
                                                              Instances=[
                                                                  {
                                                                      "InstanceId": instance_id
                                                                  },
                                                              ])

    def get_instance_load_balancers(self, instance_id):

        result = []

        args = {
            "service_resource": services.elb_service.LOAD_BALANCERS,
            "region": self.region,
        }

        for lb in self.elb_service.describe(**args):
            for inst in lb.get("Instances", []):
                if inst["InstanceId"] != instance_id:
                    continue
                if instance_id not in result:
                    result.append(lb["LoadBalancerName"])
                    break

        return result
