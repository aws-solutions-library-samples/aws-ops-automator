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

ACCOUNT_LIMITS = "AccountLimits"
CHANGE_SET = "ChangeSet"
CHANGE_SETS_SUMMARY = "ChangeSetsSummary"
RESOURCES_SUMMARY = "StackResourcesSummary"
STACK_EVENTS = "StackEvents"
STACK_RESOURCE = "StackResource"
STACK_RESOURCES = "StackResources"
STACKS = "Stacks"
STACKS_SUMMARY = "StacksSummary"

CUSTOM_RESULT_PATHS = {
    CHANGE_SET: "",
    STACK_RESOURCE: "StackResourceDetail",
    CHANGE_SETS_SUMMARY: "Summaries",
    RESOURCES_SUMMARY: "StackResourceSummaries",
    STACKS_SUMMARY: "StackSummaries"
}

RESOURCE_NAMES = [ACCOUNT_LIMITS,
                  CHANGE_SET,
                  STACK_EVENTS,
                  STACK_RESOURCE,
                  STACK_RESOURCES,
                  STACKS,
                  CHANGE_SETS_SUMMARY,
                  RESOURCES_SUMMARY,
                  STACKS_SUMMARY]

RESOURCES_WITH_TAGS = [STACKS]


class CloudformationService(AwsService):
    """
    CloudFormation service
    """

    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """
        AwsService.__init__(self,
                            service_name='cloudformation',
                            resource_names=RESOURCE_NAMES,
                            resources_with_tags=RESOURCES_WITH_TAGS,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            as_named_tuple=as_named_tuple,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            service_retry_strategy=service_retry_strategy)

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name=resource_name)

        if resource_name in [CHANGE_SETS_SUMMARY, RESOURCES_SUMMARY, STACKS_SUMMARY]:
            s = s.replace("describe_", "list_")[0:-len("_Summary")]

        return s
