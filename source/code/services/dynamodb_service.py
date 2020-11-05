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

TABLES = "Tables"
LIMITS = "Limits"
TABLE = "Table"
BACKUP = "Backup"
BACKUPS = "Backups"
TAGS_OF_RESOURCE = "TagsOfResource"

CUSTOM_RESULT_PATHS = {
    TABLES: "TableNames[*].{TableName:@}",
    BACKUP: "BackupDescription",
    BACKUPS: "BackupSummaries"
}

RESOURCE_NAMES = [
    TABLES,
    LIMITS,
    TABLE,
    BACKUP,
    BACKUPS
]

RESOURCES_WITH_TAGS = [
    TABLES,
    TABLE
]

NEXT_TOKEN_ARGUMENT = "ExclusiveStartTableName"
NEXT_TOKEN_RESULT = "LastEvaluatedTableName"

MAPPED_PARAMETERS = {
    "MaxResults": "Limit"
}


class DynamodbService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """
        AwsService.__init__(self, service_name="dynamodb",
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

    @staticmethod
    def is_regional():
        """
        Determine if a regional regional object

        Args:
        """
        return True

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name)

        return s.replace("describe_", "list_") if resource_name in [TAGS_OF_RESOURCE, TABLES, BACKUPS] else s

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :return: Tags
        """

        arn = "arn:aws:dynamodb:{}:{}:table/{}".format(client.meta.region_name, self.aws_account,
                                                       resource["TableName"])

        if self._service_retry_strategy is not None:

            if getattr(self._service_client, "list_tags_of_resource" + boto_retry.DEFAULT_SUFFIX, None) is None:
                boto_retry.make_method_with_retries(boto_client_or_resource=self._service_client,
                                                    name="list_tags_of_resource",
                                                    service_retry_strategy=self._service_retry_strategy)
            return client.list_tags_of_resource_with_retries(ResourceArn=arn).get("Tags", [])

        return client.list_tags_of_resource(ResourceArn=arn).get("Tags", [])

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        return TAGS_OF_RESOURCE
