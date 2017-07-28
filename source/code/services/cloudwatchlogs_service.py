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


DESTINATIONS = "Destinations"
EXPORT_TASKS = "ExportTasks"
LOG_EVENTS = "LogEvents"
LOG_GROUPS = "LogGroups"
LOG_STREAMS = "LogStreams"
METRIC_FILTERS = "MetricFilters"
SUBSCRIPTION_FILTERS = "SubscriptionFilters"

MAPPED_PARAMETERS = {"MaxResults": "limit"}

NEXT_TOKEN_ARGUMENT = "nextToken"
NEXT_TOKEN_RESULT = NEXT_TOKEN_ARGUMENT

RESOURCE_NAMES = [DESTINATIONS,
                  EXPORT_TASKS,
                  LOG_GROUPS,
                  LOG_STREAMS,
                  METRIC_FILTERS,
                  SUBSCRIPTION_FILTERS,
                  LOG_EVENTS]


class CloudwatchlogsService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: Retry strategy for service
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        custom_resource_paths = {r: r[0].lower() + r[1:] for r in RESOURCE_NAMES}
        custom_resource_paths[LOG_EVENTS] = "events"

        AwsService.__init__(self,
                            service_name='logs',
                            resource_names=RESOURCE_NAMES,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            as_named_tuple=as_named_tuple,
                            custom_result_paths=custom_resource_paths,
                            mapped_parameters=MAPPED_PARAMETERS,
                            next_token_argument=NEXT_TOKEN_ARGUMENT,
                            next_token_result=NEXT_TOKEN_RESULT,
                            service_retry_strategy=service_retry_strategy)

    def _tuple_name_func(self, name):
        """
        Returns the name of the tuple for resources returned as named tuple
        :param name:
        :return:
        """
        return name[0].upper() + name[1:]

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name=resource_name)

        if resource_name == LOG_EVENTS:
            s = s.replace("describe_", "filter_")
        return s

    def _map_describe_function_parameters(self, resources, args):
        """
        Maps the parameter names passed to the service class describe call to names used to make the call the the boto
        service client describe call
        :param resources: Name of the resource type
        :param args: parameters to be mapped
        :return: mapped parameters
        """
        if len(args) == 0:
            return args
        temp = AwsService._map_describe_function_parameters(self, resources, args)
        # for this service arguments start with lowercase
        translated = {b[0].lower() + b[1:]: temp[b] for b in temp}
        return translated


