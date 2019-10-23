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
import boto_retry
from services.aws_service import AwsService

ALLOWED_NODE_TYPE_MODIFICATIONS = "AllowedNodeTypeModifications"
CACHE_CLUSTERS = "CacheClusters"
CACHE_ENGINE_VERSIONS = "CacheEngineVersions"
CACHE_PARAMETER_GROUPS = "CacheParameterGroups"
CACHE_PARAMETERS = "CacheParameters"
CACHE_SECURITY_GROUPS = "CacheSecurityGroups"
CACHE_SUBNET_GROUPS = "CacheSubnetGroups"
ENGINE_DEFAULT_PARAMETERS = "EngineDefaultParameters"
EVENTS = "Events"
REPLICATION_GROUPS = "ReplicationGroups"
RESERVED_CACHE_NODES = "ReservedCacheNodes"
RESERVED_CACHE_NODES_OFFERINGS = "ReservedCacheNodesOfferings"
SNAPSHOTS = "Snapshots"
TAGS_FOR_RESOURCE = "TagsForResource"

MAPPED_PARAMETERS = {
    "MaxResults": "MaxRecords"
}

NEXT_TOKEN_ARGUMENT = "NextToken"
NEXT_TOKEN_RESULT = "NextToken"

CUSTOM_RESULT_PATHS = {
    CACHE_PARAMETERS: "{Parameters:@.Parameters, CacheNodeTypeSpecificParameters:@. CacheNodeTypeSpecificParameters}",
    ENGINE_DEFAULT_PARAMETERS: "EngineDefaults",
    ALLOWED_NODE_TYPE_MODIFICATIONS: "ScaleUpModifications"
}

RESOURCE_NAMES = [
    ALLOWED_NODE_TYPE_MODIFICATIONS,
    CACHE_CLUSTERS,
    CACHE_ENGINE_VERSIONS,
    CACHE_PARAMETER_GROUPS,
    CACHE_PARAMETERS,
    CACHE_SECURITY_GROUPS,
    CACHE_SUBNET_GROUPS,
    ENGINE_DEFAULT_PARAMETERS,
    EVENTS,
    REPLICATION_GROUPS,
    RESERVED_CACHE_NODES,
    RESERVED_CACHE_NODES_OFFERINGS,
    SNAPSHOTS
]

RESOURCES_WITH_TAGS = [
    CACHE_CLUSTERS,
    SNAPSHOTS
]

ARN = "arn:aws:elasticache:{}:{}:{}:{}"


class ElasticacheService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name='elasticache',
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

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name=resource_name)

        if resource_name in [ALLOWED_NODE_TYPE_MODIFICATIONS, TAGS_FOR_RESOURCE]:
            s = s.replace("describe_", "list_")
        return s

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

        if self._resource_name == CACHE_CLUSTERS:
            if resource["CacheClusterStatus"] not in ["available"]:
                return []
            arn_name = resource["CacheClusterId"]
            arn_resource = "cluster"
        else:
            if resource["SnapshotStatus"] in ["creating"]:
                return []
            arn_name = resource["SnapshotName"]
            arn_resource = "snapshot"

        arn = ARN.format(client.meta.region_name, self.aws_account, arn_resource, arn_name)

        if self._service_retry_strategy is not None:
            if getattr(self._service_client, "list_tags_for_resource" + boto_retry.DEFAULT_SUFFIX, None) is None:
                boto_retry.make_method_with_retries(boto_client_or_resource=self._service_client,
                                                    name="list_tags_for_resource",
                                                    service_retry_strategy=self._service_retry_strategy)
            return client.list_tags_for_resource_with_retries(ResourceName=arn).get("TagList", [])

        return client.list_tags_for_resource(ResourceName=arn).get("TagList", [])

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        return TAGS_FOR_RESOURCE
