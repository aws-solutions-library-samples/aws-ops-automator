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
import os

import boto_retry
import botocore.exceptions
import handlers
import services
from outputs import raise_exception
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

RESOURCES_WITH_TAGS = {
    DB_CLUSTERS: "cluster",
    DB_INSTANCES: "db",
    EVENT_SUBSCRIPTIONS: "es",
    OPTION_GROUPS: "og",
    DB_PARAMETER_GROUPS: "pg",
    DB_CLUSTER_PARAMETER_GROUPS: "cluster-pg",
    RESERVED_DB_INSTANCES: "db",
    DB_SECURITY_GROUPS: "secgrp",
    DB_SNAPSHOTS: "snapshot",
    DB_CLUSTER_SNAPSHOTS: "cluster-snapshot",
    DB_SUBNET_GROUPS: "subgrp"
}

NEXT_TOKEN_RESULT = "Marker"
NEXT_TOKEN_ARGUMENT = NEXT_TOKEN_RESULT

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
                            next_token_result=NEXT_TOKEN_RESULT,
                            next_token_argument=NEXT_TOKEN_ARGUMENT,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            service_retry_strategy=service_retry_strategy)
        self._tag_session = None
        self._tag_rds_client = None
        self._tag_account = None

    def _extract_resources(self, resp, select):
        # the RDS API returns an ARN in the DBSnapshotIdentifierfield for shared snapshots. This overloaded method will
        # extract the expected DBSnapshotIdentifier from the property
        resources = AwsService._extract_resources(self, resp, select)
        if self._resource_name != DB_SNAPSHOTS:
            return resources

        for r in resources:
            if "DBSnapshotIdentifier" in r and r["DBSnapshotIdentifier"].startswith("arn:aws:rds:"):
                r["DBSnapshotIdentifier"] = r["DBSnapshotIdentifier"].split(":")[-1]

        return sorted(resources, key=lambda rc: rc.get("DBSnapshotArn", ""))

    @staticmethod
    def use_cached_tags(resource, tags_to_retrieve):
        return tags_to_retrieve > 1

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

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :return: Tags
        """

        # get the name of the proprty that holds the arn of the resource
        arn_property_name = "{}Arn".format(self._resource_name[0:-1])
        if arn_property_name[0:2].lower() == "db":
            arn_property_name = "DB{}".format(arn_property_name[2:])
        # get the arn of the resource
        resource_arn = resource[arn_property_name]

        # owner of the resource (could be other account for shared sbapshots)
        resource_owner_account = resource_arn.split(":")[4]
        resource_region = resource_arn.split(":")[3]

        if resource_owner_account == self.aws_account:
            # sane account, can use same session as used to retrieve the resource
            if self._use_cached_tags:
                self._tag_session = self.session

            # make sure the client has retries
            if getattr(self._service_client, "list_tags_for_resource" + boto_retry.DEFAULT_SUFFIX, None) is None:
                boto_retry.make_method_with_retries(boto_client_or_resource=client,
                                                    name="list_tags_for_resource",
                                                    service_retry_strategy=self._service_retry_strategy)
                self._tag_rds_client = client
        else:
            # resource is from other account, get a session to get the tags from that account as these are not
            # visible for shared rds resources
            if self._tag_account != resource_owner_account or self._tag_session is None:
                self._tag_account = resource_owner_account
                used_tag_role = None
                if self._tag_roles is not None:
                    # see if there is a role for the owner account
                    for role in self._tag_roles:
                        if role is not None and services.account_from_role_arn(role) == resource_owner_account:
                            used_tag_role = role
                            break
                    else:
                        # if there is no role and the account is the ops automator account use the default role
                        # in other cases it is not possible to retrieve the tags
                        if resource_owner_account != os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT):
                            return {}
                self._tag_session = services.get_session(role_arn=used_tag_role)

            if not self._use_cached_tags:
                self._tag_rds_client = boto_retry.get_client_with_retries("rds", methods=["list_tags_for_resource"],
                                                                          context=self._context, region=resource_region)

        if self._use_cached_tags:
            return self.cached_tags(session=self._tag_session,
                                    resource_name=RESOURCES_WITH_TAGS[resource["ResourceTypeName"]],
                                    region=resource_region).get(resource_arn, {})

        try:
            resp = self._tag_rds_client.list_tags_for_resource_with_retries(ResourceName=resource_arn)
            return resp.get("TagList", [])
        except botocore.exceptions.ClientError as ex:
            if getattr(ex, "response", {}).get("Error", {}).get("Code", "") == "InvalidParameterValue":
                return []
            raise_exception("Can not list rds tags for resource {}, {}", resource_arn, ex)

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        return TAGS_FOR_RESOURCE
