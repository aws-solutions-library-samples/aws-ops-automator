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

import boto_retry
from services.aws_service import AwsService

LOAD_BALANCERS = "LoadBalancers"
ACCOUNT_LIMITS = "AccountLimits"
INSTANCE_HEALTH = "InstanceHealth"
LOAD_BALANCER_ATTRIBUTES = "LoadBalancerAttributes"
LOAD_BALANCER_POLICIES = "LoadBalancerPolicies"
LOAD_BALANCER_POLIC_TYPES = "LoadBalancerPolicyTypes"
TAGS = "Tags"

MAPPED_PARAMETERS = {
    "MaxResults": "PageSize"
}

NEXT_TOKEN_ARGUMENT = "Marker"
NEXT_TOKEN_RESULT = "NextMarker"

CUSTOM_RESULT_PATHS = {
    LOAD_BALANCERS: "LoadBalancerDescriptions",
    ACCOUNT_LIMITS: "Limits",
    INSTANCE_HEALTH: "InstanceStates",
    LOAD_BALANCER_POLICIES: "PolicyDescriptions",
    LOAD_BALANCER_POLIC_TYPES: "PolicyTypeDescriptions",
    TAGS: "TagDescriptions"
}

RESOURCE_NAMES = [
    LOAD_BALANCERS,
    ACCOUNT_LIMITS,
    INSTANCE_HEALTH,
    LOAD_BALANCER_ATTRIBUTES,
    LOAD_BALANCER_POLICIES,
    LOAD_BALANCER_POLIC_TYPES,
    TAGS
]

RESOURCES_WITH_TAGS = [
    LOAD_BALANCERS
]


class ElbService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name="elb",
                            resource_names=RESOURCE_NAMES,
                            resources_with_tags=RESOURCES_WITH_TAGS,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            as_named_tuple=as_named_tuple,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            mapped_parameters=MAPPED_PARAMETERS,
                            next_token_argument=NEXT_TOKEN_ARGUMENT,
                            next_token_result=NEXT_TOKEN_RESULT,
                            service_retry_strategy=service_retry_strategy)

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        Tags are not supported for this service.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :return: Tags
        """
        if self._resource_name not in RESOURCES_WITH_TAGS:
            raise ValueError("Resource type {] does not support tags".format(self._resource_name))

        if self._service_retry_strategy is not None:
            if getattr(self._service_client, "list_tags_for_resource" + boto_retry.DEFAULT_SUFFIX, None) is None:
                boto_retry.make_method_with_retries(boto_client_or_resource=self._service_client,
                                                    name="describe_tags",
                                                    service_retry_strategy=self._service_retry_strategy)
            tag_list = client.describe_tags_with_retries(LoadBalancerNames=[resource["LoadBalancerName"]]).get("TagDescriptions")

        else:
            tag_list = client.describe_tags(LoadBalancerNames=[resource["LoadBalancerName"]]).get("TagDescriptions")

        if len(tag_list) > 0:
            return [{"Key": t["Key"], "Value": t["Value"]} for t in tag_list[0].get("Tags", [])]
        else:
            return {}


