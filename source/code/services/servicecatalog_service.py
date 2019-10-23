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

ACCEPTED_PORTFOLIO_SHARES = "AcceptedPortfolioShares"
CONSTRAINT = "Constraint"
CONSTRAINTS_FOR_PORTFOLIO = "ConstraintsForPortfolio"
LAUNCH_PATHS = "LaunchPaths"
PORTFOLIO = "Portfolio"
PORTFOLIO_ACCESS = "PortfolioAccess"
PORTFOLIOS = "Portfolios"
PORTFOLIOS_FOR_PRODUCT = "PortfoliosForProduct"
PRODUCT = "Product"
PRODUCT_AS_ADMIN = "ProductAsAdmin"
PRODUCTS = "Products"
PRODUCTS_AS_ADMIN = "ProductsAsAdmin"
PROVISIONED_PRODUCTS = "ProvisionedProducts"
PROVISIONING_ARTIFACT = "ProvisioningArtifact"
PROVISIONING_ARTIFACTS = "ProvisioningArtifacts"
PROVISIONING_PARAMETERS = "ProvisioningParameters"
RECORD = "Record"
RECORD_HISTORY = "RecordHistory"

PRINCIPALS_FOR_PORTFOLIO = "PrincipalsForPortfolio"
PRODUCT_VIEW = "ProductView"
CUSTOM_RESULT_PATHS = {
    ACCEPTED_PORTFOLIO_SHARES: "PortfolioDetails",
    CONSTRAINT: "",
    CONSTRAINTS_FOR_PORTFOLIO: "ConstraintDetails",
    LAUNCH_PATHS: "LaunchPathSummaries",
    PORTFOLIO: "",
    PORTFOLIO_ACCESS: "AccountIds",
    PORTFOLIOS: "PortfolioDetails",
    PORTFOLIOS_FOR_PRODUCT: "PortfolioDetails",
    PRINCIPALS_FOR_PORTFOLIO: "Principals",
    PRODUCT: "",
    PRODUCT_AS_ADMIN: "ProductViewDetail",
    PRODUCTS: "ProductViewSummaries",
    PRODUCTS_AS_ADMIN: "ProductViewDetails",
    PRODUCT_VIEW: "",
    PROVISIONING_ARTIFACT: "",
    PROVISIONING_ARTIFACTS: "ProvisioningArtifactDetails",
    PROVISIONING_PARAMETERS: "",
    RECORD: "",
    RECORD_HISTORY: "RecordDetails"
}

MAPPED_PARAMETERS = {
    "MaxResults": "PageSize",
    "PortfolioId": "PathId",
    "Version": "ProvisioningArtifactId"
}

NEXT_TOKEN_ARGUMENT = "PageToken"
NEXT_TOKEN_RESULT = "NextPageToken"

RESOURCE_NAMES = [
    ACCEPTED_PORTFOLIO_SHARES,
    CONSTRAINT,
    CONSTRAINTS_FOR_PORTFOLIO,
    LAUNCH_PATHS,
    PORTFOLIO,
    PORTFOLIO_ACCESS,
    PORTFOLIOS,
    PORTFOLIOS_FOR_PRODUCT,
    PRINCIPALS_FOR_PORTFOLIO,
    PRODUCT,
    PRODUCT_AS_ADMIN,
    PRODUCTS,
    PRODUCTS_AS_ADMIN,
    PRODUCT_VIEW,
    PROVISIONED_PRODUCTS,
    PROVISIONING_ARTIFACT,
    PROVISIONING_ARTIFACTS,
    PROVISIONING_PARAMETERS,
    RECORD,
    RECORD_HISTORY,
]


class ServiceCatalogService(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self, service_name='servicecatalog',
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
        s = AwsService.describe_resources_function_name(self, resource_name=resource_name)
        if resource_name in [PRODUCTS,
                             PRODUCTS_AS_ADMIN]:
            s = s.replace("describe_", "search_")

        elif resource_name in [ACCEPTED_PORTFOLIO_SHARES,
                               CONSTRAINTS_FOR_PORTFOLIO,
                               LAUNCH_PATHS,
                               PORTFOLIO_ACCESS,
                               PORTFOLIOS,
                               PORTFOLIOS_FOR_PRODUCT,
                               PRINCIPALS_FOR_PORTFOLIO,
                               PROVISIONING_ARTIFACTS,
                               RECORD_HISTORY]:
            s = s.replace("describe_", "list_")

        elif resource_name == PROVISIONED_PRODUCTS:
            s = s.replace("describe_", "scan_")

        return s

    def _transform_returned_resource(self, client, resource, use_cached_tags=False):
        """
        This method takes the resource from the boto "describe" method and transforms them into the requested
        output format of the service class describe function
        :param client: boto client for the service that can be used to retrieve additional attributes, eg tags
        :param resource: The resource returned from the boto call
        :return: The transformed resources
        """

        temp = {i: resource[i] for i in resource if i not in ["ResponseMetadata", "nextToken"]}
        return AwsService._transform_returned_resource(self, client, temp)
