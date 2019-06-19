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


ACCESS_KEYS = "AccessKeys"
ACCOUNT_ALIASES = "AccountAliases"
ATTACHED_GROUP_POLICIES = "AttachedGroupPolicies"
ATTACHED_ROLE_POLICIES = "AttachedRolePolicies"
ATTACHED_USER_POLICIES = "AttachedUserPolicies"
ENTITIES_FOR_POLICY = "EntitiesForPolicy"
GROUP_POLICIES = "GroupPolicies"
GROUPS = "Groups"
GROUPS_FOR_USER = "GroupsForUser"
INSTANCE_PROFILES = "InstanceProfiles"
INSTANCE_PROFILES_FOR_ROLE = "InstanceProfilesForRole"
MFA_DEVICES = "MfaDevices"
OPEN_ID_CONNECT_PROVIDERS = "OpenIdConnectProviders"
POLICIES = "Policies"
POLICY_VERSIONS = "PolicyVersions"
ROLE_POLICIES = "RolePolicies"
ROLES = "Roles"
SAML_PROVIDERS = "SamlProviders"
SERVER_CERTIFICATES = "ServerCertificates"
SIGNING_CERTIFICATES = "SigningCertificates"
SSH_PUBLIC_KEYS = "SshPublicKeys"
USER_POLICIES = "UserPolicies"
USERS = "Users"
VIRTUAL_MFA_DEVICES = "VirtualMfaDevices"

CUSTOM_RESULT_PATHS = {
    ACCESS_KEYS: "AccessKeyMetadata",
    ATTACHED_GROUP_POLICIES: "AttachedPolicies",
    ATTACHED_ROLE_POLICIES: "AttachedPolicies",
    ATTACHED_USER_POLICIES: "AttachedPolicies",
    ENTITIES_FOR_POLICY: "PolicyGroups",
    GROUP_POLICIES: "PolicyNames",
    GROUPS_FOR_USER: GROUPS,
    INSTANCE_PROFILES_FOR_ROLE: INSTANCE_PROFILES,
    MFA_DEVICES: "MFADevices",
    OPEN_ID_CONNECT_PROVIDERS: "OpenIDConnectProviderList",
    POLICY_VERSIONS: "Versions",
    ROLE_POLICIES: "PolicyNames",
    SAML_PROVIDERS: "SamlProviderList",
    SERVER_CERTIFICATES: "ServerCertificateMetadataList",
    SIGNING_CERTIFICATES: "Certificates",
    SSH_PUBLIC_KEYS: "SSHPublicKeys",
    USER_POLICIES: "PolicyNames",
    VIRTUAL_MFA_DEVICES: "VirtualMFADevices"
}

RESOURCE_NAMES = [
    ACCESS_KEYS,
    ACCOUNT_ALIASES,
    ATTACHED_GROUP_POLICIES,
    ATTACHED_ROLE_POLICIES,
    ATTACHED_USER_POLICIES,
    ENTITIES_FOR_POLICY,
    GROUP_POLICIES,
    GROUPS,
    GROUPS_FOR_USER,
    INSTANCE_PROFILES,
    INSTANCE_PROFILES_FOR_ROLE,
    MFA_DEVICES,
    OPEN_ID_CONNECT_PROVIDERS,
    POLICIES,
    POLICY_VERSIONS,
    ROLE_POLICIES,
    ROLES,
    SAML_PROVIDERS,
    SERVER_CERTIFICATES,
    SIGNING_CERTIFICATES,
    SSH_PUBLIC_KEYS,
    USER_POLICIES,
    USERS,
    VIRTUAL_MFA_DEVICES

]
NEXT_TOKEN_ARGUMENT = "Marker"
NEXT_TOKEN_RESULT = NEXT_TOKEN_ARGUMENT

MAPPED_PARAMETERS = {"MaxResults": "MaxItems"}


class IamService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self,
                            service_name='iam',
                            resource_names=RESOURCE_NAMES,
                            role_arn=role_arn, session=session,
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
        Returns False because IAM is a global service
        :return:  False
        """
        return False

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name)
        # IAM prefix for function name is list_ instead of describe_
        return s.replace("describe_", "list_")


