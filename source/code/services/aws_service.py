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


import copy
import re
import uuid

import boto3
import jmespath

import boto_retry
from boto_retry import get_client_with_retries
from util.named_tuple_builder import as_namedtuple

ERR_UNEXPECTED_MULIPLE_RESULTS = "Requested a single resource result but there are multiple resources in the result"
ERR_NO_BOTO_SERVICE_METHOD = "Service client for service \"{}\" has no method named \"{}\""

DEFAULT_NEXT_TOKEN = "NextToken"


class AwsService:
    """
    Base class for implementing AWS service classes
    """

    def __init__(self, service_name,
                 resource_names,
                 resources_with_tags=None,
                 role_arn=None,
                 session=None,
                 tags_as_dict=True,
                 as_named_tuple=False,
                 custom_result_paths=None,
                 mapped_parameters=None,
                 next_token_result=None,
                 next_token_argument=None,
                 service_retry_strategy=None):
        """
        :param service_name: Name of the service
        :param resource_names: Resources that van be retrieved for the service
        :param resources_with_tags: resources that support tags
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param custom_result_paths: Dictionary with custom JMES paths to retrieve the results from the "describe" method call
        :param mapped_parameters: Dictionary with parameter named that are translated before calling the "method" call
        :param next_token_result: Name of the next token in the response to use in a next "describe" call to retrieve remaining
        results
        :param next_token_argument: Name of the parameter to pass the next token value from a previous "describe" call as a
        starting point to retrieve remaining results
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        self._region = None
        self._service_client = None
        self._assumed_role = None

        # use session, role or none (non used default session)
        self._session = session
        if session is not None and role_arn is not None:
            raise ValueError("role parameter cannot be used in combination with session parameter")
        self.role_arn = role_arn

        # name of the services
        self.service_name = service_name
        # used to retrieve resources without case sensitivity
        self._resource_names = {name.lower(): name for name in resource_names}
        # resources that have tags
        self._resources_with_tags = resources_with_tags

        self._as_tuple = as_named_tuple
        self._tags_as_dict = tags_as_dict

        # attributes are excluded from implicit conversion to named tuples
        self._tuple_excludes = ["Tags"]
        # attribute names for tags that will be converted to dictionaries
        self._converted_tags = ["Tags"]

        # default continuation parameter and result attribute
        self._nexttoken_result = next_token_result if next_token_result is not None else DEFAULT_NEXT_TOKEN
        self._nexttoken_argument = next_token_argument if next_token_argument is not None else DEFAULT_NEXT_TOKEN

        # default custom result paths
        self._custom_result_paths = custom_result_paths if custom_result_paths is not None else {}
        # default translated parameters
        self._mapped = mapped_parameters if mapped_parameters is not None else {}

        self._sts_client = None
        self._aws_account = None

        self._service_retry_strategy = service_retry_strategy

    @staticmethod
    def is_regional():
        """
        Returns True for regional services, False for global services. Overwrite in inherited services if service is global
        :return: True for regional services, False for global services
        """
        return True

    @staticmethod
    def account_from_role_arn(role_arn):
        """
        Extracts an account number from a role arn, raises a ValueException if the arn does not match a valid arn format
        :param role_arn: The arn
        :return: The extracted account number
        """
        role_elements = role_arn.split(":")
        if len(role_elements) < 5:
            raise ValueError("Role \"%s\" is not a valid role arn", role_arn)
        return role_elements[4]

    @staticmethod
    def get_session(role_arn=None, sts_client=None):
        """
        Created a session for the specified role
        :param role_arn: Role arn
        :param sts_client: Optional sts client, if not specified a (cache) sts client instance is used
        :return: Session for the specified role
        """

        if role_arn is not None:
            sts = sts_client if sts_client is not None else boto3.client("sts")
            account = AwsService.account_from_role_arn(role_arn)
            token = sts.assume_role(RoleArn=role_arn, RoleSessionName="{}-{}".format(account, str(uuid.uuid4())))
            credentials = token["Credentials"]
            return boto3.Session(aws_access_key_id=credentials["AccessKeyId"],
                                 aws_secret_access_key=credentials["SecretAccessKey"],
                                 aws_session_token=credentials["SessionToken"])
        else:
            return boto3.Session()

    @property
    def session(self):
        """
        Returns a (cached) session for the service class instance, use role if an arf for that role was provided, otherwise the
        default boto3 session is used
        :return: Session
        """
        if self._session is None:
            self._session = AwsService.get_session(role_arn=self.role_arn,
                                                   sts_client=self.sts_client if self.role_arn is not None else None)
        return self._session

    @property
    def sts_client(self):
        """
        Returns a (cached) sts client
        :return: Sts client
        """
        if self._sts_client is None:
            self._sts_client = boto3.client("sts")
        return self._sts_client

    def service_regions(self):
        """
        Returns all regions in which a service is available
        :return:  all regions in which the service is available
        """
        return boto3.Session().get_available_regions(service_name=self.service_name)

    def service_client(self, region=None, method_names=None):
        """
        Returns a client for the service using the session/role/region of the service class instance
        :param region:
        :param method_names: names of function to create wrapper methods for with retry logic
        :return: client for making the call to describe the resources
        """

        if region is None:
            region = boto3.client(self.service_name).meta.config.region_name

        if self._service_client is None or self._service_client.meta.config.region_name != region:

            args = {
                "service_name": self.service_name,
                "region_name": region
            }

            self._service_client = self.session.client(**args)

            if self._service_retry_strategy is not None and method_names is not None:
                for method in method_names:
                    boto_retry.make_method_with_retries(boto_client_or_resource=self._service_client, name=method,
                                                        service_retry_strategy=self._service_retry_strategy)

        return self._service_client

    @staticmethod
    def get_aws_account(sts=None):
        """
        Returns the current AWS account
        :param sts: Optional sts reused sts client
        :return:
        """
        client = sts if sts is not None else get_client_with_retries("sts", ["get_caller_identity"])
        if getattr(client, "get_caller_identity_with_retries", None):
            return client.get_caller_identity_with_retries()["Account"]
        else:
            return client.get_caller_identity()["Account"]

    @property
    def aws_account(self):
        """
        Returns the (cached) AWS account, using the role if one was specified, otherwise the account is retrieved by getting the
        caller identity from the STS service
        :return: Current AWS account number for the instance of the service class
        """
        if self._aws_account is None:
            if self.role_arn is not None:
                self._aws_account = AwsService.account_from_role_arn(self.role_arn)
            else:
                self._aws_account = AwsService.get_aws_account(self.sts_client)

        return self._aws_account

    @property
    def assumed_role(self):
        """
        Returns the assumed role for the instance of the service class, None if it used the default boto session
        :return:
        """
        if self.role_arn is not None:
            return self.role_arn
        if self._assumed_role is None:
            arn = self.sts_client.get_caller_identity()["Arn"]
            if re.match("^arn:aws:iam::\\d{12}:role/", arn):
                self._assumed_role = "/".join(
                    arn.replace("arn:aws:sts::", "arn:aws:iam::").replace(":assumed-role/", ":role/").split("/")[0:-1])
        return self._assumed_role

    @staticmethod
    def get_tag_value(obj, tagname):
        """
        Get the value for a tag from a returned resource, assuming the tags attribute is named "Tags"
        :param obj: resource object
        :param tagname: Name of the tag
        :return: Value of the tag, None if the tag does not exists or is empty
        """
        tags = obj.get("Tags")
        if tags is None:
            return None

        value = [t["Value"] for t in tags if t["Key"] == tagname]
        if len(value) == 0:
            return None

        return value[0]

    def _resource_name(self, resource_name):
        """
        Returns a normalized resource name.
        :param resource_name: Name of the resource, not case sensitive and can be in camel as snake case. Raises a Value
        exception if there is no resource with that name in the service
        :return: Normalized resource name
        """
        parts = resource_name.split('_')
        name = "".join(part[0].upper() + part[1:] for part in parts)
        if name.lower() not in self._resource_names:
            raise ValueError("{} is not a valid resource for service {}, valid resources are {}".format(
                resource_name, self.service_name, ", ".join(sorted(self.resources))))
        return self._resource_names[name.lower()]

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource. Note that this method most likely
        needs to be overwritten for inherited services for specific methods that are named differently than "describe"ResourceName
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        # assume describe_<resource_name> as a default
        return "describe_" + resource_name[0].lower() + "".join(
            [i if i.islower() or i == "_" else "_" + i.lower() for i in resource_name[1:]])

    def required_describe_resource_permissions(self, resource_name):
        """
        Returns the IAM permission to describe the specified resource type. If tags need to be retrieved with an explicit method
        call then the most likely overwritten _get_tag_resource method call is made to get the name of the tags resource that is
        used for an additional permission.
        :param resource_name:
        :return:
        """

        def snake_to_camel_case(s):
            result = ""
            s = s.strip("_").capitalize()
            i = 0

            while i < len(s):
                if s[i] == "_":
                    i += 1
                    result += s[i].upper()
                else:
                    result += s[i]
                i += 1

            return result

        if not resource_name:
            return []

        permissions = ["{}:{}".format(self.service_name, snake_to_camel_case(self.describe_resources_function_name(resource_name)))]

        # need to retrieve tags in an explicit call?
        if resource_name in self.resources_with_tags:
            tagging_resource_name = self._get_tag_resource(resource_name)
            if tagging_resource_name:
                get_tags_function_name = snake_to_camel_case(self.describe_resources_function_name(tagging_resource_name))
                permissions.append("{}:{}".format(self.service_name, get_tags_function_name))

        return permissions

    def _next_token_argument_name(self, resources):
        """
        Returns the name of the continuation token parameter to be used in the describe call for a specific resource. Most likely
        needs to be overwritten in inherited service class for service/resource specific parameter names
        :param resources: Name of the resource type
        :return: Name of the continuation token parameter
        """
        return self._nexttoken_argument

    def _next_token_result_name(self, resources):
        """
       Return the name of the continuation token attribute from the result of the describe response for a specific resource. Most
       likely needs to be overwritten in inherited service class for service/resource specific attribute names
       :param resources: Name of the resource type
       :return: Name of the continuation token attribute
       """
        return self._nexttoken_result

    def _map_describe_function_parameters(self, resources, args):
        """
        Maps the parameter names passed to the service class describe call to names used to make the call the the boto
        service client describe call
        :param resources: Name of the resource type
        :param args: parameters to be mapped
        :return: mapped parameters
        """

        if len(self._mapped) == 0:
            return args

        mapped_args = args.copy()
        for arg in self._mapped:
            if arg in mapped_args:
                mapped_args[self._mapped[arg]] = args[arg]
                del mapped_args[arg]

        return mapped_args

    def _tuple_name_func(self, name):
        """
        Method that returns the name of the named tuple for a specific resource. Overwrite if the name of the named tuple must be
        different than the name of the resource type
        :param name: Name of the resource
        :return: Name of the named tuple
        """
        return name

    def _extract_resources(self, resourcename, resp, select):
        """
        Extracts the resources from the response from the boto client "describe" call
        :param resourcename: Name of the resource
        :param resp: Response from boto client
        :param select: JMES path to filter returned resources and/or map/select attributes
        :return: Selected resources
        """
        if select is not None:
            expression = select
        else:
            expression = self._custom_result_paths.get(resourcename, resourcename)
        if expression != "":
            resources = jmespath.search(expression, resp)
        else:
            resources = resp

        if resources is None:
            resources = []
        elif not isinstance(resources, list):
            resources = [resources]

        return resources

    def _convert_tags_to_dictionaries(self, resource):
        """
        Converts the tags attribute from Key:Value combinations into python dictionaries
        :param resource: Service resource
        :return: Resource with tags as dictionary
        """
        if self._tags_as_dict:
            # make copy for modified result
            result = copy.copy(resource)
            for t in self._converted_tags:
                if t in result:
                    tags = result.get(t, []) or []
                    result[t] = {tag["Key"]: tag["Value"] for tag in tags}
            return result
        return resource

    def _get_tags_for_resource(self, client, resource, resource_name):
        """
        Returns the tags for specific resources that require additional boto calls to retrieve their tags. Most likely this
        method needs to be overwritten for specific services/resources
        :param client: Client that can be used to make the boto call to retrieve the tags
        :param resource: The resource for which to retrieve the tags
        :param resource_name: Name of the resource type
        :return: Tags for the specified resource
        """
        return resource.get("Tags", [])

    def _get_tag_resource(self, resource_name):
        """
        Returns the name of the service/resource specific resource that is used to explicitly retrieve the tags for that
        resource. Most likely this method needs to be overwritten for specific services/resources
        :param resource_name:
        :return: Resource name for the tags
        """
        return ""

    def _transform_returned_resource(self, client, resource, resource_name, tags, tags_as_dict, use_tuple, **kwargs):
        """
        This method takes the resource from the boto "describe" method and transforms them into the requested
        output format of the service class describe function
        :param client: boto client for the service that can be used to retrieve additional attributes, eg tags
        :param resource: The resource returned from the boto call
        :param resource_name: The name of the resource type
        :param tags: Set true true if the tags must be retrieved for this resource
        :param tags_as_dict: Set to true to convert the tags into Python dictionaries
        :param use_tuple: Set to true to return the resources as un-mutable named tuples instead of dictionaries
        :param kwargs: Additional service specific arguments for the transformation
        :return: The transformed service resource
        """

        # get tags for the resource
        if tags:
            if self._resources_with_tags is None:
                raise Exception("Service {} does not support tags".format(self.service_name))
            if resource_name not in self._resources_with_tags:
                raise Exception("Resource {} for service {} does not support tags".format(resource_name, self.service_name))
            resource["Tags"] = self._get_tags_for_resource(client, resource, resource_name)

        # convert tags to dictionaries
        if tags_as_dict:
            resource = self._convert_tags_to_dictionaries(resource)

        # convert resource to named tuple
        if use_tuple:
            return as_namedtuple(resource_name, resource, deep=True, namefunc=self._tuple_name_func, exludes=self._tuple_excludes)
        else:
            return resource

    def describe(self, service_resource, region=None, tags=False, tags_as_dict=None, as_tuple=None, select=None, **describe_args):
        """
        This method is used to retrieve service resources, specified by their name, from a service
        :param service_resource: Name of the service resource, not case sensitive, use camel or snake case
        :param region: Region from where resources are retrieved, if None then the current region is used
        :param tags: Set to True to return tags with the resource
        :param tags_as_dict: Set to True to return tags as python dictionaries
        :param as_tuple: Set to true to return results as immutable named dictionaries instead of dictionaries
        :param select: JMES path to select resources and select/transform attributes of returned resources
        :param describe_args: Parameters passed to the boto "describe" function
        :return: Service resources of the specified resource type for the service.
        """

        def use_tuple():
            """
            Tests if resources should be returned as named tuples
            :return: True for tuples, False for dictionaries
            """
            return (as_tuple is not None and as_tuple) or (as_tuple is None and self._as_tuple)

        def tags_as_dictionary():
            """
            Tests if tags should be returned as python dictionaries
            :return: True for dictionaries, False for original tag format
            """
            return tags_as_dict if tags_as_dict is not None else self._tags_as_dict

        # normalize resource name
        resource_name = self._resource_name(service_resource)
        # get the name of the boto3 method to retrieve this resource type
        describe_func_name = self.describe_resources_function_name(resource_name)

        # get additional parameters for boto describe method and map parameter names
        if describe_args is None:
            function_args = {}
        else:
            function_args = self._map_describe_function_parameters(resource_name, describe_args)

        # get method from boto service client
        client = self.service_client(region=region, method_names=[describe_func_name])
        describe_func = getattr(client, describe_func_name, None)
        if describe_func is None:
            raise ValueError(ERR_NO_BOTO_SERVICE_METHOD.format(self.service_name, describe_func_name))

        # use the method with retry logic if a retry strategy was used
        if self._service_retry_strategy is not None:
            describe_func = getattr(client, describe_func_name + boto_retry.DEFAULT_SUFFIX)

        done = False
        while not done:

            # call boto method to retrieve until no more resources are retrieved
            resp = describe_func(**function_args)

            # extract resources from result and transform to requested output format
            for obj in self._extract_resources(resourcename=resource_name, resp=resp, select=select):
                # annotate with additional account and region attributes
                obj["AwsAccount"] = self.aws_account
                obj["Region"] = self.service_client(region).meta.region_name if self.is_regional() else None

                # yield the transformed resource
                yield self._transform_returned_resource(self.service_client(region=region),
                                                        resource=obj,
                                                        resource_name=resource_name,
                                                        tags=tags,
                                                        tags_as_dict=tags_as_dictionary(),
                                                        use_tuple=use_tuple(),
                                                        kwargs=describe_args)

            # test if more resources are available
            next_token = self._next_token_result_name(resource_name)
            # if there are set the continuation token parameter for the next call to the value of the results continuation token
            if next_token in resp and resp[next_token] != "":
                next_token_argument = self._next_token_argument_name(resource_name)
                function_args[next_token_argument] = resp[next_token]
            else:
                # all resources retrieved
                done = True

    def get(self, service_resource, region=None, tags_as_dict=None, tags=False, as_tuple=None, select=None, **describe_args):
        """
        Alternative for describe method in cases where only a single specific resource is expected. An exception is raised when
        multiple resources are returned from the service
        :param service_resource: Name of the service resource, not case sensitive, use camel or snake case
        :param region: Region from where resources are retrieved, if None then the current region is used
        :param tags: Set to True to return tags with the resource
        :param tags_as_dict: Set to True to return tags as python dictionaries
        :param as_tuple: Set to true to return results as immutable named dictionaries instead of dictionaries
        :param select: JMES path to select resources and select/transform attributes of returned resources
        :param describe_args: Parameters passed to the boto "describe" function
        :return: Service resource of the specified resource type for the service, None if the resource was not available.
        """

        # get resources
        results = self.describe(service_resource=service_resource,
                                region=region,
                                tags=tags,
                                tags_as_dict=tags_as_dict,
                                as_tuple=as_tuple,
                                select=select,
                                **describe_args)

        try:
            # get the first returned resource
            result = results.next()
            try:
                # if there is more than one result, raise Exception
                results.next()
                raise Exception(ERR_UNEXPECTED_MULIPLE_RESULTS)
            except StopIteration:
                # Expected exception as there should be only one result
                return result
        except StopIteration:
            return None

    @property
    def resources(self):
        """
        Returns names of all available resources for this service
        :return: names of all available resources for this service
        """
        return self._resource_names.values()

    @property
    def resources_with_tags(self):
        return self._resources_with_tags if self._resources_with_tags else []

    @property
    def resource_method_mapping(self):
        """
        Returns a list of how service resources are mapped to the corresponding boto method calls
        :return: Resource to boto client method mapping
        """
        return [(r, self.describe_resources_function_name(r)) for r in self.resources]
