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

CLUSTER_PARAMETER_GROUPS = "ClusterParameterGroups"
CLUSTER_PARAMETERS = "ClusterParameters"
CLUSTER_SECURITY_GROUPS = "ClusterSecurityGroups"
CLUSTER_SNAPSHOTS = "ClusterSnapshots"
CLUSTER_SUBNET_GROUPS = "ClusterSubnetGroups"
CLUSTER_VERSIONS = "ClusterVersions"
CLUSTERS = "Clusters"
DEFAULT_CLUSTER_PARAMETERS = "DefaultClusterParameters"
EVENT_CATEGORIES = "EventCategories"
EVENT_SUBSCRIPTIONS = "EventSubscriptions"
EVENTS = "Events"
HSM_CLIENT_CERTIFICATES = "HsmClientCertificates"
HSM_CONFIGURATIONS = "HsmConfigurations"
LOGGING_STATUS = "LoggingStatus"
ORDERABLE_CLUSTER_OFFERINGS = "OrderableClusterOptions"
RESERVED_NODE_OFFERINGS = "ReservedNodeOfferings"
RESERVED_NODES = "ReservedNodes"
RESIZE = "Resize"
SNAPSHOT_COPY_GRANTS = "SnapshotCopyGrants"
TABLE_RESTORE_STATUS = "TableRestoreStatus"
TAGS = "Tags"

MAPPED_PARAMETERS = {"MaxResults": "MaxRecords"}

NEXT_TOKEN_ARGUMENT = "MaxRecords"
NEXT_TOKEN_RESULT = "Marker"

CUSTOM_RESULT_PATHS = {
    CLUSTER_PARAMETER_GROUPS: "ParameterGroups",
    CLUSTER_PARAMETERS: "Parameters",
    CLUSTER_SNAPSHOTS: "Snapshots",
    EVENT_CATEGORIES: "EventCategoriesMapList",
    EVENT_SUBSCRIPTIONS: "EventCategoriesMapList",
    LOGGING_STATUS: "",
    RESIZE: "",
    TABLE_RESTORE_STATUS: "TableRestoreStatusDetails",
    TAGS: "TaggedResources"
}

RESOURCE_NAMES = [
    CLUSTER_PARAMETER_GROUPS,
    CLUSTER_PARAMETERS,
    CLUSTER_SECURITY_GROUPS,
    CLUSTER_SNAPSHOTS,
    CLUSTER_SUBNET_GROUPS,
    CLUSTER_VERSIONS,
    CLUSTERS,
    DEFAULT_CLUSTER_PARAMETERS,
    EVENT_CATEGORIES,
    EVENT_SUBSCRIPTIONS,
    EVENTS,
    HSM_CLIENT_CERTIFICATES,
    HSM_CONFIGURATIONS,
    LOGGING_STATUS,
    ORDERABLE_CLUSTER_OFFERINGS,
    RESERVED_NODE_OFFERINGS,
    RESERVED_NODES,
    RESIZE,
    SNAPSHOT_COPY_GRANTS,
    TABLE_RESTORE_STATUS,
    TAGS
]

ARN_REDSHIFT = "arn:aws:redshift:{}:{}:"

RESOURCES_WITH_TAGS = {
    CLUSTER_PARAMETER_GROUPS: [ARN_REDSHIFT + "parametergroup:{}", "ParameterGroupName"],
    CLUSTER_SNAPSHOTS: [ARN_REDSHIFT + "snapshot:{}/{}", "ClusterIdentifier", "SnapshotIdentifier"],
    CLUSTER_SUBNET_GROUPS: [ARN_REDSHIFT + "subnetgroup:{}", "ClusterSubnetGroupName"],
    CLUSTERS: [ARN_REDSHIFT + "cluster:{}", "ClusterIdentifier"],
    HSM_CLIENT_CERTIFICATES: [ARN_REDSHIFT + "hsmconfiguration:{}", "HsmConfigurationIdentifier"],
    SNAPSHOT_COPY_GRANTS: [ARN_REDSHIFT + "snapshotcopygrant:{}", "SnapshotCopyGrantName"]
}


# region,account
class RedshiftService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name='redshift',
                            resource_names=RESOURCE_NAMES,
                            resources_with_tags=RESOURCES_WITH_TAGS.keys(),
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            as_named_tuple=as_named_tuple,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            mapped_parameters=MAPPED_PARAMETERS,
                            next_token_argument=NEXT_TOKEN_ARGUMENT,
                            next_token_result=NEXT_TOKEN_RESULT,
                            service_retry_strategy=service_retry_strategy)

    def _get_tags_for_resource(self, client, resource, resource_name):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :param resource_name: Name of the resource type
        :return: Tags
        """
        arn_data = [client.meta.region_name, self.aws_account]
        for i in RESOURCES_WITH_TAGS[resource_name][1:]:
            arn_data.append(resource[i])
        arn = RESOURCES_WITH_TAGS[resource_name][0].format(*arn_data)

        resp = client.describe_tags(ResourceName=arn)
        return [t["Tag"] for t in resp.get("TaggedResources", [])]

    def _get_tag_resource(self, resource_name):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :param resource_name: Type name of the resource
        :return: Name of the resource that will be used to retrieve the tags
        """
        return TAGS
