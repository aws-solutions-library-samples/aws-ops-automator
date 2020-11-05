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


CLUSTERS = "Clusters"
CLUSTERS_ARNS = "ClustersArns"
CONTAINER_INSTANCES = "ContainerInstances"
CONTAINER_INSTANCES_ARNS = "ContainerInstancesArns"
SERVICES = "Services"
SERVICES_ARNS = "ServicesArns"
TASK_ARNS = "TaskArns"
TASK_DEFINITION_FAMILIES = "TaskDefinitionFamilies"
TASK_DEFINITIONS = "TaskDefinitions"
TASK_DEFINITIONS_ARNS = "TaskDefinitionsArns"
TASKS = "Tasks"

CUSTOM_RESULT_PATHS = {
    CLUSTERS: "clusters",
    CLUSTERS_ARNS: "clusterArns",
    CONTAINER_INSTANCES: "containerInstances",
    CONTAINER_INSTANCES_ARNS: "containerInstanceArns",
    SERVICES: "services",
    SERVICES_ARNS: "serviceArns",
    TASK_DEFINITION_FAMILIES: "families",
    TASK_DEFINITIONS: "taskDefinition",
    TASK_DEFINITIONS_ARNS: "taskDefinitionArns",
    TASKS: "tasks",
    TASK_ARNS: "taskArns"
}

RESOURCE_NAMES = [
    CLUSTERS,
    CLUSTERS_ARNS,
    CONTAINER_INSTANCES,
    CONTAINER_INSTANCES_ARNS,
    SERVICES,
    SERVICES_ARNS,
    TASK_DEFINITION_FAMILIES,
    TASK_DEFINITIONS,
    TASK_DEFINITIONS_ARNS,
    TASKS,
    TASK_ARNS
]

NEXT_TOKEN_ARGUMENT = "nextToken"
NEXT_TOKEN_RESULT = "nextToken"

MAPPED_PARAMETERS = {}


class EcsService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self, service_name='ecs',
                            resource_names=RESOURCE_NAMES,
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
        if resource_name in [CLUSTERS_ARNS,
                             CONTAINER_INSTANCES_ARNS,
                             SERVICES_ARNS,
                             TASK_DEFINITION_FAMILIES,
                             TASK_DEFINITIONS_ARNS,
                             TASK_ARNS]:
            return s.replace("describe_", "list_")
        return s

    def _map_describe_function_parameters(self, resources, args):
        """
        Map a map of parameters to a map of parameters.

        Args:
            self: (todo): write your description
            resources: (todo): write your description
        """
        if len(args) == 0:
            return args
        temp = AwsService._map_describe_function_parameters(self, resources, args)
        # for this service arguments start with lowercase
        translated = {b[0].lower() + b[1:]: temp[b] for b in temp}
        return translated


