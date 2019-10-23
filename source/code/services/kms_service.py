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
from services.aws_service import AwsService

KEY_POLICY = "KeyPolicy"
KEY_ROTATION_STATUS = "KeyRotationStatus"
PARAMETERS_FOR_INPUT = "ParametersForInput"

ALIASES = "Aliases"
GRANTS = "Grants"
KEY_POLICIES = "KeyPolicies"
KEYS = "Keys"
KEY = "Key"
RESOURCE_TAGS = "ResourceTags"
RETIRABLE_GRANTS = "RetirableGrants"

CUSTOM_RESULT_PATHS = {
    KEY_POLICY: "{Policy:@.Policy}",
    KEY_ROTATION_STATUS: "{KeyRotationEnabled:@.KeyRotationEnabled}",
    PARAMETERS_FOR_INPUT: "{ParametersValidTo:@.ParametersValidTo, PublicKey:@.PublicKey, KeyId:@.KeyId,ImportToken:@.ImportToken}",
    KEY_POLICIES: "{PolicyNames:@.PolicyNames}",
    RESOURCE_TAGS: "Tags",
    RETIRABLE_GRANTS: "Grants",
    KEY: "KeyMetadata"
}

RESOURCE_NAMES = [
    ALIASES,
    GRANTS,
    KEY_POLICIES,
    KEY_POLICY,
    KEY_ROTATION_STATUS,
    KEY,
    KEYS,
    PARAMETERS_FOR_INPUT,
    RESOURCE_TAGS,
    RETIRABLE_GRANTS
]

RESOURCES_WITH_TAGS = [
    KEYS,
    KEY
]

NEXT_TOKEN_ARGUMENT = "Marker"
NEXT_TOKEN_RESULT = "NextMarker"

MAPPED_PARAMETERS = {
    "MaxResults": "Limit"
}


class KmsService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name='kms',
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
        s = AwsService.describe_resources_function_name(self, resource_name)
        if resource_name in [KEY]:
            return s
        if resource_name in [KEY_POLICY, KEY_ROTATION_STATUS, PARAMETERS_FOR_INPUT, KEY]:
            return s.replace("describe_", "get_")
        return s.replace("describe_", "list_")

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :return: Tags
        """
        tags = self.describe(RESOURCE_TAGS, region=client.meta.region_name, tags_as_dict=False, KeyId=resource["KeyId"])
        return [{"Key": t["TagKey"], "Value": t["TagValue"]} for t in tags]

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        return RESOURCE_TAGS
