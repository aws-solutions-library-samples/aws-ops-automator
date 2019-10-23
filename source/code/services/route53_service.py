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

ACCOUNT_LIMIT = "AccountLimit"
CHANGE_INFO = "ChangeInfo"
GEO_LOCATION = "GeoLocation"
HEALTH_CHECK = "HealthCheck"
HEALTH_CHECK_COUNT = "HealthCheckCount"
HEALTH_CHECK_LAST_FAILURE_REASON = "HealthCheckLastFailureReason"
HEALTH_CHECK_STATUS = "HealthCheckStatus"
HOSTED_ZONE = "HostedZone"
HOSTED_ZONE_COUNT = "HostedZoneCount"
HOSTED_ZONE_LIMIT = "HostedZoneLimit"
QUERY_LOGGING_CONFIG = "QueryLoggingConfig"
REUSABLE_DELEGATION_SET = "ReusableDelegationSet"
RESOURCE_RECORD_SET_LIMIT = "ReusableDelegationSetLimit"
TRAFFIC_POLICY = "TrafficPolicy"
TRAFFIC_POLICY_INSTANCE = "TrafficPolicyInstance"
TRAFFIC_POLICY_INSTANCE_COUNT = "TrafficPolicyInstanceCount"

GEO_LOCATIONS = "GeoLocations"
HEALTH_CHECKS = "HealthChecks"
HOSTED_ZONES = "HostedZones"
HOSTED_ZONES_BY_NAME = "HostedZonesByName"
QUERY_LOGGING_CONFIGS = "QueryLoggingConfigs"
RESOURCE_RECORD_SETS = "ResourceRecordSets"
REUSABLE_DELEGATION_SET_LIMIT = "ReusableDelegationSetLimit"
REUSABLE_DELEGATION_SETS = "ReusableDelegationSets"
TAGS_FOR_RESOURCE = "TagsForResource"
TAGS_FOR_RESOURCES = "TagsForResources"
TRAFFIC_POLICIES = "TrafficPolicies"
TRAFFIC_POLICIES_INSTANCES = "TrafficPolicyInstances"
TRAFFIC_POLICY_INSTANCES_BY_HOSTED_ZONE = "TrafficPolicyInstancesByHostedZone"
TRAFFIC_POLICY_INSTANCES_BY_POLICY = "TrafficPolicyInstancesByPolicy"
TRAFFIC_POLICY_VERSIONS = "TrafficPolicyVersions"
VPC_ASSOCIATION_AUTHORIZATION = "VpcAssociationAuthorizations"

NEXT_TOKEN_ARGUMENT = "Marker"
NEXT_TOKEN_RESULT = "NextMarker"

RESOURCE_NAMES = [
    ACCOUNT_LIMIT,
    CHANGE_INFO,
    GEO_LOCATION,
    HEALTH_CHECK,
    HEALTH_CHECK_COUNT,
    HEALTH_CHECK_LAST_FAILURE_REASON,
    HEALTH_CHECK_STATUS,
    HOSTED_ZONE,
    HOSTED_ZONE_COUNT,
    HOSTED_ZONE_LIMIT,
    QUERY_LOGGING_CONFIG,
    REUSABLE_DELEGATION_SET,
    RESOURCE_RECORD_SET_LIMIT,
    TRAFFIC_POLICY,
    TRAFFIC_POLICY_INSTANCE,
    TRAFFIC_POLICY_INSTANCE_COUNT,
    GEO_LOCATIONS,
    HEALTH_CHECKS,
    HOSTED_ZONES,
    HOSTED_ZONES_BY_NAME,
    QUERY_LOGGING_CONFIGS,
    RESOURCE_RECORD_SETS,
    REUSABLE_DELEGATION_SET_LIMIT,
    REUSABLE_DELEGATION_SETS,
    TAGS_FOR_RESOURCE,
    TAGS_FOR_RESOURCES,
    TRAFFIC_POLICIES,
    TRAFFIC_POLICIES_INSTANCES,
    TRAFFIC_POLICY_INSTANCES_BY_HOSTED_ZONE,
    TRAFFIC_POLICY_INSTANCES_BY_POLICY,
    TRAFFIC_POLICY_VERSIONS,
    VPC_ASSOCIATION_AUTHORIZATION
]

RESOURSES_WITH_TAGS = [
    HEALTH_CHECK,
    HEALTH_CHECKS,
    HOSTED_ZONE,
    HOSTED_ZONES]

CUSTOM_RESULT_PATHS = {
    ACCOUNT_LIMIT: "{Limit:@.Limit, Count:@.Count}",
    CHANGE_INFO: "{" + ",".join(['"{}":@.ChangeInfo.{}'.format(i, i) for i in ["Id", "Status", "SubmittedAt", "Comment"]]) + "}",
    GEO_LOCATION: "{" + ",".join(['"{}":@.GeoLocationDetails.{}'.format(i, i) for i in
                                  ["ContinentCode", "ContinentName""CountryCode", "CountryName", "SubdivisionCode",
                                   "SubdivisionName"]]) + "}",
    HEALTH_CHECK_COUNT: "{HealthCheckCount:HealthCheckCount}",
    HEALTH_CHECK_LAST_FAILURE_REASON: "HealthCheckObservations",
    HEALTH_CHECK_STATUS: "HealthCheckObservations",
    HOSTED_ZONE: "{" + ",".join(['"{}":{}'.format(i, i) for i in ["HostedZone", "DelegationSet", "VPCs"]]) + "}",
    HOSTED_ZONE_COUNT: "{HostedZoneCount:HostedZoneCount}",
    HOSTED_ZONE_LIMIT: "{Limit:@.Limit, Count:@.Count}",
    QUERY_LOGGING_CONFIG: "{" + ",".join(
        ['"{}":@.QueryLoggingConfig.{}'.format(i, i) for i in ["Id", "HostedZoneId", "CloudWatchLogsLogGroupArn"]]) + "}",
    REUSABLE_DELEGATION_SET: "{" + ",".join(
        ['"{}":@.DelegationSet.{}'.format(i, i) for i in ["Id", "CallerReference", "NameServers"]]) + "}",
    REUSABLE_DELEGATION_SET_LIMIT: "{Limit:@.Limit, Count:@.Count}",
    TRAFFIC_POLICY: "{" + ",".join(
        ['"{}":@.TrafficPolicy.{}'.format(i, i) for i in ["Id", "Version", "Name", "Type", "Document", "Comment"]]) + "}",
    TRAFFIC_POLICY_INSTANCE: "{" + ",".join(['"{}":@.TrafficPolicyInstance.{}'.format(i, i) for i in
                                             ["Id", "HostedZoneId", "Name", "TTL", "State", "Message", "TrafficPolicyId",
                                              "TrafficPolicyVersion", "TrafficPolicyType"]]) + "}",
    TRAFFIC_POLICY_INSTANCE_COUNT: "{TrafficPolicyInstanceCount:TrafficPolicyInstanceCount}",
    GEO_LOCATIONS: "GeoLocationDetailsList",
    HOSTED_ZONES_BY_NAME: HOSTED_ZONES,
    REUSABLE_DELEGATION_SETS: "DelegationSets",
    TAGS_FOR_RESOURCE: "ResourceTagSet",
    TAGS_FOR_RESOURCES: "ResourceTagSets",
    TRAFFIC_POLICIES: "TrafficPolicySummaries",
    TRAFFIC_POLICY_INSTANCES_BY_HOSTED_ZONE: TRAFFIC_POLICIES_INSTANCES,
    TRAFFIC_POLICY_INSTANCES_BY_POLICY: TRAFFIC_POLICIES_INSTANCES,
    TRAFFIC_POLICY_VERSIONS: TRAFFIC_POLICIES,
    VPC_ASSOCIATION_AUTHORIZATION: "{HostedZoneId:@.HostedZoneId, VPCs:@.VPCs}"

}

MULTI_ELEMENT_CONTINUATION_MARKERS = {
    GEO_LOCATIONS: [
        ("StartContinentCode", "NextContinentCode"),
        ("StartCountryCode", "NextCountryCode"),
        ("StartSubdivisionCode", "NextSubdivisionCode")
    ],
    RESOURCE_RECORD_SETS: [
        ("StartRecordName", "NextRecordName"),
        ("StartRecordType", "NextRecordType"),
        ("StartRecordIdentifier", "NextRecordIdentifier")
    ],
    TRAFFIC_POLICIES_INSTANCES: [
        ("HostedZoneIdMarker", "HostedZoneIdMarker"),
        ("TrafficPolicyInstanceNameMarker", "TrafficPolicyInstanceNameMarker"),
        ("TrafficPolicyInstanceTypeMarker", "TrafficPolicyInstanceTypeMarker")
    ],
    TRAFFIC_POLICY_INSTANCES_BY_HOSTED_ZONE: [
        ("TrafficPolicyInstanceNameMarker", "TrafficPolicyInstanceNameMarker"),
        ("TrafficPolicyInstanceTypeMarker", "TrafficPolicyInstanceTypeMarker")
    ],
    TRAFFIC_POLICY_INSTANCES_BY_POLICY: [
        ("HostedZoneIdMarker", "HostedZoneIdMarker"),
        ("TrafficPolicyInstanceNameMarker", "TrafficPolicyInstanceNameMarker"),
        ("TrafficPolicyInstanceTypeMarker", "TrafficPolicyInstanceTypeMarker")
    ]
}


class Route53Service(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: Retry strategy for service
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name='route53',
                            resource_names=RESOURCE_NAMES,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            resources_with_tags=RESOURSES_WITH_TAGS,
                            as_named_tuple=as_named_tuple,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            mapped_parameters={},
                            next_token_argument=NEXT_TOKEN_ARGUMENT,
                            next_token_result=NEXT_TOKEN_RESULT,
                            service_retry_strategy=service_retry_strategy)

    @staticmethod
    def is_regional():
        return False

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name=resource_name)
        if resource_name in [
            ACCOUNT_LIMIT,
            CHANGE_INFO,
            GEO_LOCATION,
            HEALTH_CHECK,
            HEALTH_CHECK_COUNT,
            HEALTH_CHECK_LAST_FAILURE_REASON,
            HEALTH_CHECK_STATUS,
            HOSTED_ZONE,
            HOSTED_ZONE_COUNT,
            HOSTED_ZONE_LIMIT,
            QUERY_LOGGING_CONFIG,
            REUSABLE_DELEGATION_SET,
            RESOURCE_RECORD_SET_LIMIT,
            TRAFFIC_POLICY,
            TRAFFIC_POLICY_INSTANCE,
            TRAFFIC_POLICY_INSTANCE_COUNT
        ]:
            s = s.replace("describe_", "get_")

        else:
            s = s.replace("describe_", "list_")

        return s

    def _get_tags_for_resource(self, client, resource):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags. Most likely this
        method needs to be overwritten for specific services/resources
        :param client: Client that can be used to make the boto call to retrieve the tags
        :return: Tags for the specified resource
        """
        tags = client.list_tag_for_resource(ResourceId=resource["ResourceId"],
                                            ResourceType=self._resource_name.lower()).get("ResourceTagSet", {}).get("Tags", {})
        return [
            {
                "Key": t, "Value": tags[t]
            } for t in tags
        ]

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        if self._resource_name in [
            HOSTED_ZONE,
            HOSTED_ZONES,
            HEALTH_CHECK,
            HEALTH_CHECKS
        ]:
            return TAGS_FOR_RESOURCE
        else:
            return None

    def _next_token_argument_name(self, resources):
        """
        Returns the name of the continuation token parameter to be used in the describe call for a specific resource. Most likely
        needs to be overwritten in inherited service class for service/resource specific parameter names
        :param resources: Name of the resource type
        :return: Name of the continuation token parameter
        """
        if resources == TRAFFIC_POLICIES:
            return "TrafficPolicyIdMarker"

        if resources == TRAFFIC_POLICY_VERSIONS:
            return "TrafficPolicyVersionMarker"

        if resources == VPC_ASSOCIATION_AUTHORIZATION:
            return "NextToken"

        return self._nexttoken_argument

    def _next_token_result_name(self, resources):
        """
       Return the name of the continuation token attribute from the result of the describe response for a specific resource. Most
       likely needs to be overwritten in inherited service class for service/resource specific attribute names
       :param resources: Name of the resource type
       :return: Name of the continuation token attribute
       """
        if resources in [
            GEO_LOCATIONS,
            RESOURCE_RECORD_SETS
        ]:
            return "IsTruncated"

        if resources == TRAFFIC_POLICIES:
            return "TrafficPolicyIdMarker"

        if resources == TRAFFIC_POLICY_VERSIONS:
            return "TrafficPolicyVersionMarker"

        if resources == VPC_ASSOCIATION_AUTHORIZATION:
            return "NextToken"

        return self._nexttoken_result

    def set_continuation_call_parameters(self, function_args, next_token, resp):
        if self._resource_name in MULTI_ELEMENT_CONTINUATION_MARKERS:
            for marker in MULTI_ELEMENT_CONTINUATION_MARKERS[self._resource_name]:
                if marker[1] in resp:
                    function_args[marker[0]] = resp[marker[1]]
                else:
                    function_args.pop(marker[0], None)
        else:
            AwsService.set_continuation_call_parameters(self, function_args, next_token, resp)
