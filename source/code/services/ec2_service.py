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

import boto3

from boto_retry import get_client_with_retries
from services.aws_service import AwsService

WARN_NO_PRICING_API_ACCESS = "Valid instance types not available from pricing API {}, using values from Boto3 EC2 service model {}"

ENV_EC2_VALID_INSTANCE_TYPES = "EC2_VALID_INSTANCE_TYPES"

ADDRESSES = "Addresses"
ATTRIBUTES = "AccountAttributes"
AVAILABILITY_ZONES = "AvailabilityZones"
BUNDLE_TASKS = "BundleTasks"
CLASSIC_LINK = "VpcClassicLink"
CLASSIC_LINK_DNS_SUPPORT = "VpcClassicLinkDnsSupport"
CLASSIC_LINK_INSTANCES = "ClassicLinkInstances"
CONVERSION_TASKS = "ConversionTasks"
CUSTOMER_GATEWAYS = "CustomerGateways"
DHCP_OPTIONS = "DhcpOptions"
EXPORT_TASKS = "ExportTasks"
FLEET_REQUEST_HISTORY = "SpotFleetRequestHistory"
FLOW_LOGS = "FlowLogs"
GROUP_REFERENCES = "SecurityGroupReferences"
HOST_RESERVATION_OFFERINGS = "HostReservationOfferings"
HOST_RESERVATIONS = "HostReservations"
HOSTS = "Hosts"
ID_FORMAT = "IdFormat"
IDENTITY_ID_FORMAT = "IdentityIdFormat"
IMAGE_ATTRIBUTE = "ImageAttribute"
IMAGES = "Images"
IMPORT_IMAGE_TASKS = "ImportImageTasks"
IMPORT_SNAPSHOT_TASKS = "ImportSnapshotTasks"
INSTANCE_ATTRIBUTE = "InstanceAttribute"
INSTANCE_AVAILABILITY = "ScheduledInstanceAvailability"
INSTANCE_CREDIT_SPECIFICATIONS = "InstanceCreditSpecifications"
INSTANCE_STATUS = "InstanceStatus"
INSTANCES = "Instances"
INTERFACE_ATTRIBUTE = "NetworkInterfaceAttribute"
INTERNET_GATEWAYS = "InternetGateways"
KEY_PAIRS = "KeyPairs"
MOVING_ADDRESSES = "MovingAddresses"
NAT_GATEWAYS = "NatGateways"
NETWORK_ACLS = "NetworkAcls"
NETWORK_INTERFACES = "NetworkInterfaces"
PLACEMENT_GROUPS = "PlacementGroups"
PREFIX_LISTS = "PrefixLists"
REGIONS = "Regions"
RESERVED_INSTANCE_LISTINGS = "ReservedInstancesListings"
RESERVED_INSTANCE_MODIFICATIONS = "ReservedInstancesModifications"
RESERVED_INSTANCES = "ReservedInstances"
RESERVED_INSTANCES_OFFERINGS = "ReservedInstancesOfferings"
ROUTE_TABLES = "RouteTables"
SCHEDULED_INSTANCES = "ScheduledInstances"
SECURITY_GROUPS = "SecurityGroups"
STALE_SECURITY_GROUPS = "StaleSecurityGroups"
SNAPSHOT_ATTRIBUTE = "SnapshotAttribute"
SNAPSHOTS = "Snapshots"
SPOT_DATAFEED_SUBSCRIPTION = "SpotDatafeedSubscription"
SPOT_FLEET_INSTANCES = "SpotFleetInstances"
SPOT_FLEET_REQUESTS = "SpotFleetRequests"
SPOT_INSTANCE_REQUESTS = "SpotInstanceRequests"
SPOT_PRICE_HISTORY = "SpotPriceHistory"
SUBNETS = "Subnets"
TAGS = "Tags"
VOLUME_ATTRIBUTE = "VolumeAttribute"
VOLUME_STATUS = "VolumeStatus"
VOLUMES = "Volumes"
VPC_ATTRIBUTE = "VpcAttribute"
VPC_ENDPOINT_SERVICES = "VpcEndpointServices"
VPC_ENDPOINTS = "VpcEndpoints"
VPC_PEERING_CONNECTIONS = "VpcPeeringConnections"
VPCS = "Vpcs"
VPN_CONNECTIONS = "VpnConnections"
VPN_GATEWAYS = "VpnGateways"

CUSTOM_RESULT_PATHS = custom_result_paths = {
    CLASSIC_LINK_INSTANCES: INSTANCES,
    HOST_RESERVATION_OFFERINGS: "OfferingSet",
    HOST_RESERVATIONS: "HostReservationSet",
    ID_FORMAT: "Statuses",
    IDENTITY_ID_FORMAT: "Statuses",

    IMAGE_ATTRIBUTE: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "BlockDeviceMappings",
        "Description",
        "ImageId",
        "KernelId",
        "LaunchPermissions",
        "ProductCodes",
        "RamdiskId",
        "SriovNetSupport"
    ]]) + "}",

    INSTANCE_ATTRIBUTE: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "BlockDeviceMappings",
        "Description",
        "DisableApiTermination",
        "EbsOptimized"
        "EnaSupport",
        "Groups",
        "InstanceId",
        "InstanceInitiatedShutdownBehavior",
        "InstanceType",
        "KernelId",
        "ProductCodes",
        "RamdiskId",
        "RootDeviceName",
        "SourceDestCheck",
        "SriovNetSupport",
        "UserData"
    ]]) + "}",

    INSTANCES: "Reservations[*].Instances[]",
    INSTANCE_STATUS: "InstanceStatuses",
    MOVING_ADDRESSES: "MovingAddressStatuses",
    INTERFACE_ATTRIBUTE: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "Attachment",
        "Description",
        "Groups",
        "NetworkInterfaceId",
        "SourceDestCheck"
    ]]) + "}",

    GROUP_REFERENCES: "SecurityGroupReferenceSet",
    INSTANCE_AVAILABILITY: "ScheduledInstanceAvailabilitySet",
    SCHEDULED_INSTANCES: "ScheduledInstanceSet",
    SNAPSHOT_ATTRIBUTE: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "CreateVolumePermissions",
        "ProductCodes",
        "SnapshotId"
    ]]) + "}",

    SPOT_FLEET_INSTANCES: "ActiveInstances",
    FLEET_REQUEST_HISTORY: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "HistoryRecords",
        "LastEvaluatedTime"
        "SpotFleetRequestId",
        "StartTime"
    ]]) + "}",

    SPOT_FLEET_REQUESTS: "SpotFleetRequestConfigs",
    VOLUME_ATTRIBUTE: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "VolumeId",
        "AutoEnableIO",
        "ProductCodes"
    ]]) + "}",

    VOLUME_STATUS: "VolumeStatuses",

    VPC_ATTRIBUTE: "{" + ",".join(['"{}":{}'.format(i, i) for i in [
        "VpcId",
        "EnableDnsSupport",
        "EnableDnsHostnames"
    ]]) + "}",

    CLASSIC_LINK: VPCS,
    CLASSIC_LINK_DNS_SUPPORT: VPCS,
    VPC_ENDPOINT_SERVICES: "ServiceNames"
}

RESOURCE_NAMES = [
    ATTRIBUTES,
    ADDRESSES,
    AVAILABILITY_ZONES,
    BUNDLE_TASKS,
    CLASSIC_LINK_INSTANCES,
    CONVERSION_TASKS,
    CUSTOMER_GATEWAYS,
    DHCP_OPTIONS,
    EXPORT_TASKS,
    FLOW_LOGS,
    HOST_RESERVATION_OFFERINGS,
    HOST_RESERVATIONS,
    HOSTS,
    ID_FORMAT,
    IDENTITY_ID_FORMAT,
    IMAGE_ATTRIBUTE,
    IMAGES,
    IMPORT_IMAGE_TASKS,
    IMPORT_SNAPSHOT_TASKS,
    INSTANCE_ATTRIBUTE,
    INSTANCE_CREDIT_SPECIFICATIONS,
    INSTANCE_STATUS,
    INSTANCES,
    INTERNET_GATEWAYS,
    KEY_PAIRS,
    MOVING_ADDRESSES,
    NAT_GATEWAYS,
    NETWORK_ACLS,
    INTERFACE_ATTRIBUTE,
    NETWORK_INTERFACES,
    PLACEMENT_GROUPS,
    PREFIX_LISTS,
    REGIONS,
    RESERVED_INSTANCES,
    RESERVED_INSTANCE_LISTINGS,
    RESERVED_INSTANCE_MODIFICATIONS,
    RESERVED_INSTANCES_OFFERINGS,
    ROUTE_TABLES,
    INSTANCE_AVAILABILITY,
    SCHEDULED_INSTANCES,
    GROUP_REFERENCES,
    SECURITY_GROUPS,
    SNAPSHOT_ATTRIBUTE,
    SNAPSHOTS,
    SPOT_DATAFEED_SUBSCRIPTION,
    SPOT_FLEET_INSTANCES,
    FLEET_REQUEST_HISTORY,
    SPOT_FLEET_REQUESTS,
    SPOT_INSTANCE_REQUESTS,
    SPOT_PRICE_HISTORY,
    SECURITY_GROUPS,
    SUBNETS,
    TAGS,
    VOLUME_ATTRIBUTE,
    VOLUME_STATUS,
    VOLUMES,
    VPC_ATTRIBUTE,
    CLASSIC_LINK,
    CLASSIC_LINK_DNS_SUPPORT,
    VPC_ENDPOINT_SERVICES,
    VPC_ENDPOINTS,
    VPC_PEERING_CONNECTIONS,
    VPCS,
    VPN_CONNECTIONS,
    VPN_GATEWAYS
]

RESOURCES_WITH_TAGS = [
    ADDRESSES,
    CUSTOMER_GATEWAYS,
    DHCP_OPTIONS,
    IMAGES,
    INSTANCES,
    INTERNET_GATEWAYS,
    NETWORK_ACLS,
    NETWORK_INTERFACES,
    RESERVED_INSTANCES,
    ROUTE_TABLES,
    SECURITY_GROUPS,
    SNAPSHOTS,
    SPOT_INSTANCE_REQUESTS,
    SUBNETS,
    VOLUMES,
    VPC_PEERING_CONNECTIONS,
    VPCS,
    VPN_CONNECTIONS,
    VPN_GATEWAYS
]

_valid_instance_types = None


class Ec2Service(AwsService):
    _valid_instance_types = None

    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name='ec2',
                            resource_names=RESOURCE_NAMES,
                            resources_with_tags=RESOURCES_WITH_TAGS,
                            role_arn=role_arn,
                            session=session,
                            as_named_tuple=as_named_tuple,
                            tags_as_dict=tags_as_dict,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            service_retry_strategy=service_retry_strategy)

    def _transform_returned_resource(self, client, resource, use_cached_tags=False):

        if self._resource_name in [INSTANCE_ATTRIBUTE,
                                   IMAGE_ATTRIBUTE,
                                   INTERFACE_ATTRIBUTE,
                                   SNAPSHOT_ATTRIBUTE,
                                   FLEET_REQUEST_HISTORY,
                                   VOLUME_ATTRIBUTE,
                                   VPC_ATTRIBUTE]:
            temp = {r: resource[r] for r in resource if resource[r] is not None}
            for r in temp:
                if isinstance(temp[r], dict) and "Value" in temp[r]:
                    temp[r] = temp[r]["Value"]
        else:
            temp = resource

        return AwsService._transform_returned_resource(self, client, temp)

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags.
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :return: Tags
        """
        return resource.get(TAGS, [])

    @staticmethod
    def valid_instance_types(logger=None):

        # first check if the environment variable is set
        ec2_types = os.getenv(ENV_EC2_VALID_INSTANCE_TYPES, "").strip()
        if ec2_types != "":
            return [e.strip() for e in ec2_types.split(",")]

        # if the types are not in the environment variable fetch the types from the pricing service api
        global _valid_instance_types
        if _valid_instance_types is None:
            _valid_instance_types = []
            # noinspection PyPep8
            try:
                pricing = get_client_with_retries("pricing", ["get_attribute_values"], region="us-east-1")

                args = {
                    "ServiceCode": "AmazonEC2",
                    "AttributeName": "instanceType",
                    "_expected_boto3_exceptions_": ["AccessDeniedException"]
                }
                while True:
                    sc = pricing.get_attribute_values_with_retries(**args)
                    for a in sc.get("AttributeValues"):
                        if len(a["Value"].split(".")) > 1:
                            _valid_instance_types.append(a["Value"])
                    if "NextToken" in sc:
                        args["NextToken"] = sc["NextToken"]
                    else:
                        break
            except Exception as ex:
                ec2 = boto3.Session().client("ec2")
                # noinspection PyProtectedMember
                _valid_instance_types = ec2._service_model._service_description["shapes"]["InstanceType"]["enum"]
                version = ec2._service_model.api_version
                if logger is not None:
                    logger.warning(WARN_NO_PRICING_API_ACCESS, ex, version)
        return _valid_instance_types
