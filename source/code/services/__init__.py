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

import importlib
import inspect
import os
import sys
import uuid
from os import listdir
from os.path import isfile, join

import boto3
import botocore.exceptions

from helpers import pascal_to_snake_case

ERR_ASSUME_ROLE_FOR_ARN = "Can not assume role {}, {}"
ERR_NO_MODULE_FOR_SERVICE = "Can not load module {} for service {}, available services are {}, ex"
ERR_UNEXPECTED_SERVICE_CLASS_IN_MODULE = "Unable to load class {0}Service for service {0} from module {1}, " \
                                         "action class found in module was {2}"

SERVICES = "services"
SERVICE_MODULE_NAME = SERVICES + ".{}"
SERVICES_PATH = "./" + SERVICES

SERVICE = "Service"
SERVICE_CLASS = "{}" + SERVICE

ENV_ROLE_ARN = "ROLE_ARN"

__services = {}


def _get_service_class(service_module):
    """
    Gets the service class from the module using naming pattern, the name of the class must end with "Service"
    :param service_module: The service class from the module, None if no service class was found
    :return:
    """
    for cls in inspect.getmembers(service_module, inspect.isclass):
        if cls[1].__module__ != service_module.__name__ or not cls[1].__name__.endswith(SERVICE):
            continue
        return cls
    return None


def _get_module(module_name):
    """
    Gets a module by its name
    :param module_name: Name of the module
    :return: The loaded module
    """
    the_module = sys.modules.get(module_name)
    if the_module is None:
        the_module = importlib.import_module(module_name)
    return the_module


def get_module_for_service(service_name):
    """
    Gets the module for a service using naming convention. First the name of the service is capitalized and appended by the
    string "Service". Then it is converted from camel to snake case to get the name of the module that will be loaded. Raises an
    ImportError exception if no module is found for the constructed module name
    :param service_name:
    :return:
    """

    name = service_name.capitalize()
    class_name = SERVICE_CLASS.format(name)
    module_name = SERVICE_MODULE_NAME.format(pascal_to_snake_case(class_name))
    try:
        return _get_module(module_name)
    except Exception as ex:
        raise ImportError(ERR_NO_MODULE_FOR_SERVICE.format(module_name, name, ", ".join(all_services())), ex)


def all_services():
    """
    Return as list of all supported service names
    :return: list of all supported service names
    """
    result = []
    for f in listdir(SERVICES_PATH):
        if isfile(join(SERVICES_PATH, f)) and f.endswith("_{}.py".format(SERVICE.lower())):
            module_name = SERVICE_MODULE_NAME.format(f[0:-len(".py")])
            service_module = _get_module(module_name)
            cls = _get_service_class(service_module)
            if cls is not None:
                service_name = cls[0][0:-len(SERVICE)]
                if service_name.lower() != "aws":
                    result.append(service_name)
    return result


def get_service_class(service_name):
    """
    Gets the class that implements a service
    :param service_name: Name of the service
    :return: Class that implements the service
    """
    name = service_name.capitalize()

    if name not in __services:
        service_module = get_module_for_service(service_name)
        cls = _get_service_class(service_module)
        if cls is None or cls[0][0:-len(SERVICE)] != name:
            raise ImportError(ERR_UNEXPECTED_SERVICE_CLASS_IN_MODULE.format(name, service_module, cls[0] if cls else "None"))
        __services[name] = cls
    return __services[name][1]


def create_service(service_name, **kwargs):
    """
    Creates an instance of the class for the specified service name. An ImportError exception is raises if there is no module
    that implements the class for the requested service.
    :param service_name: name of the service
    :param kwargs: Optional arguments passed to the constructor of the class
    :return: Instance of the class for the requested service
    """
    return get_service_class(service_name)(**kwargs)


def resources_for_service(service_name):
    """
    Returns the resources that can be retrieved for a service
    :param service_name:
    :return: List of resource type for the specified service
    """
    service_module = get_module_for_service(service_name)
    resource_names = getattr(service_module, "RESOURCE_NAMES", None)
    if resource_names is None:
        raise ValueError("RESOURCE_NAMES not defined in module module")
    return resource_names


def get_resource_describe_permissions(service_name, resource_names):
    """
    Returns a list of permissions needed to retrieve resources from a service
    :param service_name: Name of the service
    :param resource_names: Names of the resources
    :return:
    """
    service = create_service(service_name)
    if len([r for r in resource_names if r != ""]) == 0:
        resource_names = []
    permissions = []
    for res in resource_names:
        permissions += service.required_describe_resource_permissions(res)
    return set(permissions)


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


def get_session(role_arn=None, sts_client=None, logger=None):
    if role_arn not in [None, ""]:
        sts = sts_client if sts_client is not None else boto3.client("sts")
        account = account_from_role_arn(role_arn)
        try:
            token = sts.assume_role(RoleArn=role_arn, RoleSessionName="{}-{}".format(account, str(uuid.uuid4())))
        except botocore.exceptions.ClientError as ex:
            if logger is not None:
                logger.error(ERR_ASSUME_ROLE_FOR_ARN, role_arn, ex)
            raise ex
        credentials = token["Credentials"]
        return boto3.Session(aws_access_key_id=credentials["AccessKeyId"],
                             aws_secret_access_key=credentials["SecretAccessKey"],
                             aws_session_token=credentials["SessionToken"])
    else:
        role = os.getenv(ENV_ROLE_ARN)
        if role is not None:
            return get_session(role, sts_client)
        return boto3.Session()


def get_aws_account(sts=None):
    """
    Returns the current AWS account
    :param sts: Optional sts reused sts client
    :return:
    """
    client = sts if sts is not None else get_session().client("sts")
    return client.get_caller_identity()["Account"]
