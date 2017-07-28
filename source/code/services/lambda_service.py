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

from services.aws_service import AwsService

ACCOUNT_SETTINGS = "AccountSettings"
ALIAS = "Alias"
ALIASES = "Aliases"
EVENT_SOURCE_MAPPING = "EventSourceMapping"
EVENT_SOURCE_MAPPINGS = "EventSourceMappings"
FUNCTION = "Function"
FUNCTION_CONFIGURATION = "FunctionConfiguration"
FUNCTIONS = "Functions"
POLICY = "Policy"
TAGS = "Tags"
VERSIONS_BY_FUNCTION = "VersionsByFunction"

CUSTOM_RESULT_PATHS = {
    ALIAS: "",
    EVENT_SOURCE_MAPPING: "",
    FUNCTION: "@.{Configuration:Configuration,Tags:Tags, Code:Code}",
    FUNCTION_CONFIGURATION: "",
    POLICY: "",
    VERSIONS_BY_FUNCTION: "Versions"
}

RESOURCE_NAMES = [
    ACCOUNT_SETTINGS,
    ALIAS,
    ALIASES,
    EVENT_SOURCE_MAPPING,
    EVENT_SOURCE_MAPPINGS,
    FUNCTION,
    FUNCTION_CONFIGURATION,
    FUNCTIONS,
    POLICY,
    VERSIONS_BY_FUNCTION
]

NEXT_TOKEN_ARGUMENT = "Marker"
NEXT_TOKEN_RESULT = "NextMarker"

MAPPED_PARAMETERS = {"MaxResults": "MaxItems"}


class LambdaService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self, service_name='lambda',
                            resource_names=RESOURCE_NAMES,
                            role_arn=role_arn, session=session,
                            resources_with_tags=[FUNCTION,FUNCTIONS],
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
        if resource_name in [FUNCTION, ALIAS, EVENT_SOURCE_MAPPING, FUNCTION_CONFIGURATION, POLICY]:
            return s.replace("describe_", "get_")
        return s.replace("describe_", "list_")

    def _get_tags_for_resource(self, client, resource, resource_name):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags. Most likely this
        method needs to be overwritten for specific services/resources
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :param resource_name: Name of the resource type
        :return: Tags for the specified resource
        """
        if resource_name == FUNCTION:
            tags = resource.get("Tags", {})
        else:
            tags = client.list_tags( Resource = resource["FunctionArn"]).get("Tags")
        return [{"Key": t, "Value": tags[t]} for t in tags]

    def _get_tag_resource(self, resource_name):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :param resource_name: Type name of the resource
        :return: Name of the resource that will be used to retrieve the tags
        """
        if resource_name == FUNCTIONS:
            return TAGS
        else:
            return None



