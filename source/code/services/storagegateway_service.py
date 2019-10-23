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

FILE_SHARES = "FileShares"
GATEWAYS = "Gateways"
LOCAL_DISKS = "LocalDisks"
VOLUME_RECOVERY_POINTS = "VolumeRecoveryPoints"
TAGS_FOR_RESOURCE = "TagsForResource"
TAPES = "Tapes"
VOLUME_INITIATORS = "VolumeInitiators"
VOLUMES = "Volumes"

CUSTOM_RESULT_PATHS = {
    TAGS_FOR_RESOURCE: "Tags",
    FILE_SHARES: "FileShareInfoList",
    LOCAL_DISKS: "Disks",
    TAPES: "TapeInfos",
    VOLUME_INITIATORS: "Initiators",
    VOLUME_RECOVERY_POINTS: "VolumeRecoveryPointInfos",
    VOLUMES: "VolumeInfos"
}

RESOURCE_NAMES = [
    FILE_SHARES,
    GATEWAYS,
    LOCAL_DISKS,
    VOLUME_RECOVERY_POINTS,
    TAGS_FOR_RESOURCE,
    TAPES,
    VOLUME_INITIATORS,
    VOLUMES
]

RESOURCES_WITH_TAGS = [
    GATEWAYS,
    VOLUMES,
    TAPES
]

NEXT_TOKEN_ARGUMENT = "Marker"
NEXT_TOKEN_RESULT = "NextMarker"

MAPPED_PARAMETERS = {}


class StoragegatewayService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self, service_name='storagegateway',
                            resource_names=RESOURCE_NAMES,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            as_named_tuple=as_named_tuple,
                            resources_with_tags=RESOURCES_WITH_TAGS,
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
        s = AwsService.describe_resources_function_name(self, resource_name)
        return s.replace("describe_", "list_")

    @staticmethod
    def use_cached_tags(resource, tags_to_retrieve):
        return tags_to_retrieve > 1

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :return: Tags
        """
        arn_property_name = "{}ARN".format(self._resource_name[0:-1])
        arn = resource[arn_property_name]

        if self._use_cached_tags:
            return self.cached_tags(self._resource_name, resource['Region']).get(arn, {})

        if self._service_retry_strategy is not None:
            if getattr(self._service_client, "list_tags_for_resource" + boto_retry.DEFAULT_SUFFIX, None) is None:
                boto_retry.make_method_with_retries(boto_client_or_resource=self._service_client,
                                                    name="list_tags_for_resource",
                                                    service_retry_strategy=self._service_retry_strategy)
                resp = client.list_tags_for_resource_with_retries(ResourceARN=arn)
                return resp.get("Tags", [])

        return client.list_tags_for_resource(ResourceARN=arn).get("Tags", [])

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        return TAGS_FOR_RESOURCE
