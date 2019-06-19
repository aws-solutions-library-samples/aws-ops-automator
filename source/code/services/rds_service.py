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

ACCOUNT_ATTRIBUTES = "AccountAttributes"
CERTIFICATES = "Certificates"
DB_CLUSTER_PARAMETER_GROUPS = "DbClusterParameterGroups"
DB_CLUSTER_PARAMETERS = "DbClusterParameters"
DB_CLUSTER_SNAPSHOT_ATTRIBUTES = "DbClusterSnapshotAttributes"
DB_CLUSTER_SNAPSHOTS = "DbClusterSnapshots"
DB_CLUSTERS = "DbClusters"
DB_ENGINE_VERSIONS = "DbEngineVersions"
DB_INSTANCES = "DbInstances"
DB_LOG_FILES = "DbLogFiles"
DB_PARAMETER_GROUPS = "DbParameterGroups"
DB_PARAMETERS = "DbParameters"
DB_SECURITY_GROUPS = "DbSecurityGroups"
DB_SNAPSHOT_ATTRIBUTES = "DbSnapshotAttributes"
DB_SNAPSHOTS = "DbSnapshots"
DB_SUBNET_GROUPS = "DbSubnetGroups"
ENGINE_DEFAULT_CLUSTER_PARAMETERS = "EngineDefaultClusterParameters"
ENGINE_DEFAULT_PARAMETERS = "EngineDefaultParameters"
EVENT_CATEGORIES = "EventCategories"
EVENT_SUBSCRIPTIONS = "EventSubscriptions"
EVENTS = "Events"
OPTION_GROUP_OPTIONS = "OptionGroupOptions"
OPTION_GROUPS = "OptionGroups"
ORDERABLE_DB_INSTANCE_OPTIONS = "OrderableDbInstanceOptions"
PENDING_MAINTENANCE_ACTIONS = "PendingMaintenanceActions"
RESERVED_DB_INSTANCES = "ReservedDbInstances"
RESERVED_DB_INSTANCES_OFFERINGS = "ReservedDbInstancesOfferings"
SOURCE_REGIONS = "SourceRegions"
TAGS_FOR_RESOURCE = "TagsForResource"

CUSTOM_RESULT_PATHS = {
    ACCOUNT_ATTRIBUTES: "AccountQuotas",
    DB_CLUSTER_PARAMETERS: "Parameters",
    DB_LOG_FILES: "DescribeDBLogFiles",
    DB_PARAMETERS: "Parameters",
    EVENT_CATEGORIES: "EventCategoriesMapList",
    EVENT_SUBSCRIPTIONS: "EventSubscriptionsList",
    OPTION_GROUPS: "OptionGroupsList",
    ORDERABLE_DB_INSTANCE_OPTIONS: "OrderableDBInstanceOptions",
    RESERVED_DB_INSTANCES: "ReservedDBInstances",
    RESERVED_DB_INSTANCES_OFFERINGS: "ReservedDBInstancesOfferings"
}

RESOURCE_NAMES = [
    ACCOUNT_ATTRIBUTES,
    CERTIFICATES,
    DB_CLUSTER_PARAMETER_GROUPS,
    DB_CLUSTER_PARAMETERS,
    DB_CLUSTER_SNAPSHOT_ATTRIBUTES,
    DB_CLUSTER_SNAPSHOTS,
    DB_CLUSTERS,
    DB_ENGINE_VERSIONS,
    DB_INSTANCES,
    DB_LOG_FILES,
    DB_PARAMETER_GROUPS,
    DB_PARAMETERS,
    DB_SECURITY_GROUPS,
    DB_SNAPSHOT_ATTRIBUTES,
    DB_SNAPSHOTS,
    DB_SUBNET_GROUPS,
    ENGINE_DEFAULT_CLUSTER_PARAMETERS,
    ENGINE_DEFAULT_PARAMETERS,
    EVENT_CATEGORIES,
    EVENT_SUBSCRIPTIONS,
    EVENTS,
    OPTION_GROUP_OPTIONS,
    OPTION_GROUPS,
    ORDERABLE_DB_INSTANCE_OPTIONS,
    PENDING_MAINTENANCE_ACTIONS,
    RESERVED_DB_INSTANCES,
    RESERVED_DB_INSTANCES_OFFERINGS,
    SOURCE_REGIONS
]

RESOURCES_WITH_TAGS = [
    DB_CLUSTERS,
    DB_INSTANCES,
    EVENT_SUBSCRIPTIONS,
    OPTION_GROUPS,
    DB_PARAMETER_GROUPS,
    DB_CLUSTER_PARAMETER_GROUPS,
    RESERVED_DB_INSTANCES,
    DB_SECURITY_GROUPS,
    DB_SNAPSHOTS,
    DB_CLUSTER_SNAPSHOTS,
    DB_SUBNET_GROUPS]

for name in RESOURCE_NAMES:
    if name.startswith("Db") and name not in CUSTOM_RESULT_PATHS:
        CUSTOM_RESULT_PATHS[name] = result = "DB" + name[2:]


class RdsService(AwsService):
    def __init__(self, session=None, role_arn=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """
        AwsService.__init__(self,
                            session=session,
                            service_name='rds',
                            resource_names=RESOURCE_NAMES,
                            resources_with_tags=RESOURCES_WITH_TAGS,
                            role_arn=role_arn,
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
        s = AwsService.describe_resources_function_name(self, resource_name)
        if resource_name == TAGS_FOR_RESOURCE:
            return s.replace("describe_", "list_")
        return s

    def _get_tags_for_resource(self, client, resource, resource_name):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :param resource_name: Name of the resource type
        :return: Tags
        """
        arn_property_name = "{}Arn".format(resource_name[0:-1])
        if arn_property_name[0:2].lower() == "db":
            arn_property_name = "DB{}".format(arn_property_name[2:])
        arn = resource[arn_property_name]
        return client.list_tags_for_resource(ResourceName=arn).get("TagList", [])

    def _get_tag_resource(self, resource_name):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :param resource_name: Type name of the resource
        :return: Name of the resource that will be used to retrieve the tags
        """
        return TAGS_FOR_RESOURCE
