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

import decimal
import os
import re

import boto3

import actions
import boto_retry
import configuration
import handlers
import pytz
import services
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from scheduling.cron_expression import CronExpression
from services.aws_service import AwsService

VALID_ARN_REGEX = r"^arn:aws:iam::\d{12}:role\/[a-zA-Z0-9=,_.@-]{1,64}$"

DEFAULT_TIMEZONE = "UTC"

INF_READ_ARN_RESULT = "Read {} cross account arn's for task with name {}{}"
INF_READING_OBJECT = "Reading task cross account roles file {}"

MSG_ACTION_ONLY_INTERNAL = "Action {} is marked as an internal action and can only be used in internal tasks"
MSG_ARN_FORMAT_INVALID = "arn format for cross account role \"{}\" is not valid"
MSG_BAD_REGION = "Region \"{}\" is not a valid region for service \"{}\", available regions are {}"
MSG_EVENT_SERVICE_NOT_ACTION_SERVICE = "Service \"{}\" for action {} does not match service \"{}\" for event {}"
MSG_INVALID_ACTION_NAME = "Action with name \"{}\" does not exist, available actions are {}"
MSG_INVALID_BOOL = "\"{}\" is not a valid boolean value"
MSG_IVALID_TASK_INTERVAL = "Interval cron expression \"{}\" for task {} must have 5 fields"
MSG_NO_TAG_FILTER = "Resource type \"{}\" for task does not support tags, tag-filer \"{}\" not allowed for action \"{}\""
MSG_NO_TASK_ACTION_NAME = "Action name not specified"
MSG_NO_WILDCARS_TAG_FILTER_ALLOWED = "Tag wildcard filter \"{}\" is not allowed for name of tag in tagfilter for action \"{}\""

WARN_NOT_REGIONAL_SERVICE = "One or more regions ({}) are specified but service \"{}\" or action {} is not a regional service"
WARN_OVERLAPPING_ROLES = "Account {} in cross account role \"{}\" is overlapping with account other role or scheduler account"
WARN_NO_ENV_CONFIG_BUCKET = "No configuration bucket defined in environment variable \"{}\""
WARN_READING_TASK_ROLES = "Error reading roles from {} in bucket {}, ({})"

ERR_ERR_IN_CONFIG_ITEM = "Error in configuration item : {} ({})"
ERR_INVALID_CRON_EXPRESSION = "{} is not a valid cron expression, ({})"
ERR_INVALID_PARAMETER = "Parameter \"{}\" is not a valid parameter for action \"{}\", valid parameters are {}"
ERR_INVALID_TIMEZONE = "\"{}\" is not a valid timezone"
ERR_MAX_LEN = "Value \"{}\" is longer than maximum length {} for parameter {}"
ERR_MAX_VALUE = "Value {} is higher than maximum value {} for parameter {}"
ERR_MIN_LEN = "Value \"{}\" is shorter than minimum length {} for parameter {}"
ERR_MIN_VALUE = "Value {} is less than minimum value {} for parameter {}"
ERR_MISSING_REQUIRED_PARAMETER = "error: parameter \"{}\" must exists and can not be empty"
ERR_NO_CROSS_ACCOUNT_OPERATIONS = "Action {} does not support cross account operations"
ERR_NO_PARAMETERS = "Parameter \"{}\" is not a valid parameter, action \"{}\" has no parameters"
ERR_NOT_ALLOWED_VALUE = "Value \"{}\" is not in list of allowed values ({}) for parameter {}"
ERR_PATTERN_VALUE = "Value \"{}\" does not match allowed pattern \"{}\" for parameter \"{}\""
ERR_REQUIRED_PARAM_MISSING = "Required parameter \"{}\" is missing"
ERR_THIS_ACCOUNT_MUST_BE_TRUE = "Action {} only supports operations in this account, parameter \"{}\"must be set to value true"
ERR_UNKNOWN_PARAMETER = "error: parameter \"{}\" is not a valid parameter, valid parameters are {}"
ERR_WRONG_PARAM_TYPE = "Type of parameter \"{}\" must be \"{}\", current type for value {} is {}"
ERR_TIMEOUT_NOT_ALLOWED = "Action {} has no completion handling, timeout parameter not allowed"
ERR_INVALID_NUMERIC_TIMEOUT = "{} is not a valid numeric value for timeout parameter ({})"
ERR_TIMEOUT_MUST_BE_GREATER_0 = "Timeout parameter value must be > 0, current value is {}"

_checked_timezones = dict()
_invalid_timezones = set()

_service_regions = {}
_service_is_regional = {}


class TaskConfiguration:
    """
    Task configuration actions
    """

    def __init__(self, context=None, logger=None):
        """
        Initializes the instance
        :param context: Lambda context
        :param logger: Optional logger for warnings, if None then warnings are printed to console
        """
        self._logger = logger
        self._this_account = None
        self._context = context
        self._all_timezones = {tz.lower(): tz for tz in pytz.all_timezones}
        self._all_actions = actions.all_actions()
        self._s3_client = None
        self._s3_configured_cross_account_roles = None

    @property
    def config_table(self):
        """
        Returns the configuration table
        :return: the configuration table
        """
        tablename = os.getenv(configuration.ENV_CONFIG_TABLE)

        table = boto3.resource("dynamodb").Table(tablename)
        add_retry_methods_to_resource(table, ["scan", "get_item", "delete_item", "put_item"], context=self._context)

        return table

    @property
    def s3_client(self):
        """
        Returns S3 client for handling configuration files
        :return: S3 client
        """
        if self._s3_client is None:
            # IMPORTANT, list_objects and list_objects_v2 require s3:ListBucket permission !!
            self._s3_client = get_client_with_retries("s3", ["list_objects_v2", "get_object"])
        return self._s3_client

    @property
    def s3_configured_cross_account_roles(self):
        """
        Reads and buffers cross account roles for tasks configures in the task roles files
        :return:
        """

        def role_objects(config_bucket):
            args = {"Bucket": config_bucket, "Prefix": configuration.TASK_ROLES_FOLDER}
            while True:
                resp = self.s3_client.list_objects_v2_with_retries(**args)
                for key in [o["Key"] for o in resp.get("Contents", [])]:
                    if key != configuration.TASK_ROLES_FOLDER:
                        yield key

                if not resp["IsTruncated"]:
                    break

                args["ContinuationToken"] = resp["NextContinuationToken"]

        def read_roles_from_object(config_bucket, key):
            s3_configured = []
            try:
                resp = self.s3_client.get_object_with_retries(Bucket=config_bucket, Key=key)
                lines = resp["Body"].read().decode('utf-8').split("\n")
                for line in lines:
                    line = line.strip()
                    if line == "" or line.startswith("#"):
                        continue
                    s3_configured.append(line)
                return s3_configured

            except Exception as ex:
                self._warn(WARN_READING_TASK_ROLES, key, bucket, str(ex))
                return []

        if self._s3_configured_cross_account_roles is None:
            self._s3_configured_cross_account_roles = {}
            bucket = os.getenv(configuration.ENV_CONFIG_BUCKET, None)
            if bucket is None:
                self._warn(WARN_NO_ENV_CONFIG_BUCKET, configuration.ENV_CONFIG_BUCKET)
            else:
                for obj_key in role_objects(bucket):
                    task_name = obj_key[len(configuration.TASK_ROLES_FOLDER):]
                    self._info(INF_READING_OBJECT, obj_key)
                    roles = read_roles_from_object(bucket, obj_key)
                    self._s3_configured_cross_account_roles[task_name] = roles
                    self._info(INF_READ_ARN_RESULT, len(roles), task_name, "\n" + "\n".join(roles) if len(roles) else "")

        return self._s3_configured_cross_account_roles

    @staticmethod
    def config_table_exists():
        tablename = os.environ[configuration.ENV_CONFIG_TABLE]
        for t in boto3.resource("dynamodb").tables.all():
            if t.table_name == tablename:
                return True
        return False

    def _warn(self, msg, *args):
        if self._logger:
            self._logger.warn(msg, *args)
        else:
            print(msg.format(*args))

    def _info(self, msg, *args):
        if self._logger:
            self._logger.info(msg, *args)
        else:
            print(msg.format(*args))

    def config_items(self, include_internal=False):
        """
        Returns all items from the configuration table
        :return: all items from the configuration table
        """

        scan_args = {
        }

        while True:
            scan_resp = self.config_table.scan_with_retries(**scan_args)
            for item in scan_resp.get("Items", []):
                if not item.get(configuration.CONFIG_INTERNAL, False) or include_internal:
                    yield item
            if "LastEvaluatedKey" in scan_resp:
                scan_args["ExclusiveStartKey"] = scan_resp["LastEvaluatedKey"]
            else:
                break

    def get_config_item(self, name):
        """
        Reads a specific item from the configuration using its name as the key
        :param name: name of the task
        :return: The item for the task, None if it does not exist
        """

        query_resp = self.config_table.get_item_with_retries(Key={configuration.CONFIG_TASK_NAME: name}, ConsistentRead=True)
        return query_resp.get("Item", None)

    def delete_config_item(self, name):
        """
        Deletes a task item from the configuration table
        :param name: name of the task
        :return:
        """

        self.config_table.delete_item_with_retries(Key={configuration.CONFIG_TASK_NAME: name})

    def put_config_item(self, **kwargs):
        """
        Writes a task item to the table after validating the input arguments. If the item with the name as specified in the
        task argument already exists it is overwritten
        :param kwargs:
        :return:
        """
        config_item = self._verify_configuration_item(**kwargs)
        self.config_table.put_item_with_retries(Item=config_item)
        return config_item

    @classmethod
    def service_regions(cls, service_name):
        """
        Returns available regions for a service
        :param service_name: Name of the service
        :return: list of regions in which the service is available
        """
        available_regions = _service_regions.get(service_name)
        if available_regions is not None:
            return available_regions

        available_regions = services.create_service(service_name).service_regions()
        _service_regions[service_name] = available_regions
        return available_regions

    @classmethod
    def service_is_regional(cls, service_name):
        """
        Returns if a service is a regional service
        :param service_name: Name of the service
        :return: True if service is regional
        """
        is_regional = _service_is_regional.get(service_name)
        if is_regional is not None:
            return is_regional

        service_class = services.get_service_class(service_name)
        is_regional = service_class.is_regional()
        _service_is_regional[service_name] = is_regional
        return is_regional

    @property
    def this_account(self):
        """
        Returns the AWS account number
        :return: AWS account number
        """
        if self._this_account is None:
            client = boto_retry.get_client_with_retries("sts", ["get_caller_identity"], context=self._context)
            self._this_account = client.get_caller_identity_with_retries()["Account"]
        return self._this_account

    @staticmethod
    def is_valid_role_arn(arn):
        """
        Checks if the format of a role arn is valid
        :param arn: Tested arn
        :return: True if the format of the role arn is valid, else False
        """
        return re.match(VALID_ARN_REGEX, arn) is not None

    def validate_action(self, action_name):
        """
        Tests if an action name is a known action name
        :param action_name: The name of the action
        :return: The name of the action if it is valid, if not an exception is raised
        """
        if action_name is None or action_name.strip() == "":
            raise ValueError(MSG_NO_TASK_ACTION_NAME)
        result = action_name.strip()
        if result not in self._all_actions:
            raise ValueError(MSG_INVALID_ACTION_NAME.format(result, ",".join(self._all_actions)))
        return result

    @staticmethod
    def validate_tagfilter(tag_filter, action_name):
        """
        Tests if tags are supported by the resources for the action. If this is nit the case then the use of tag filters is
        not possible and an exception is raised
        :param tag_filter: Tag filter value
        :param action_name: Name of the action
        :return: Filter if tags are supported and the filter can be used, otherwise an exception is raised
        """

        if tag_filter is None or tag_filter == "":
            return None

        action_properties = actions.get_action_properties(action_name)
        resource_supports_tags = action_properties.get(actions.ACTION_RESOURCES) in services.create_service(
            action_properties[actions.ACTION_SERVICE]).resources_with_tags

        # resource does not allow tags, so tagfilters can not be used
        if not resource_supports_tags:
            raise ValueError(MSG_NO_TAG_FILTER.format(action_properties[actions.ACTION_RESOURCES], tag_filter, action_name))

        # destructive actions can deny use of wildcards for tag name
        if not action_properties.get(actions.ACTION_ALLOW_TAGFILTER_WILDCARD, True):
            if "".join([s.strip() for s in tag_filter.split("=")[0:1]]) in ["*", "**", "*="]:
                raise ValueError(MSG_NO_WILDCARS_TAG_FILTER_ALLOWED.format(tag_filter, action_name))

        return tag_filter

    @staticmethod
    def verify_task_parameters(task_parameters, action_name):
        """
        Validates parameter set and values for the specified action.
        A ValueException is raised in the following cases:
        -Required parameter is not available and there is no default specified for the action
        -Unknown parameter found
        -Type of parameter is wrong and value can task_parameters not be converted to the required type
        -Value is out of range specified by min and max value for numeric parameters
        -Value is too long, too short or does not mats the allowed pattern for string parameters
        :param task_parameters: Dictionary of parameters, keys are name of the parameters, value is parameter value
        :param action_name: Name of the action
        :return: Dictionary of validated parameters, missing non required parameters are set to default if specified in action
        implementation
        """
        validated_parameters = {}

        def verify_numeric_parameter(value, action_param):
            if type(value) in [int, float, long, complex, decimal]:
                if actions.PARAM_MIN_VALUE in action_param and value < action_param[actions.PARAM_MIN_VALUE]:
                    raise ValueError(ERR_MIN_VALUE.format(value, action_param[actions.PARAM_MIN_VALUE], param_name))
                if actions.PARAM_MAX_VALUE in action_param and value > action_param[actions.PARAM_MAX_VALUE]:
                    raise ValueError(ERR_MAX_VALUE.format(value, action_param[actions.PARAM_MAX_VALUE], param_name))

        def verify_string_parameter(value, action_param):
            if type(value) in [str, unicode]:
                if actions.PARAM_MIN_LEN in action_param and len(value) < action_param[actions.PARAM_MIN_LEN]:
                    raise ValueError(ERR_MIN_LEN.format(value, action_param[actions.PARAM_MIN_LEN], param_name))
                if actions.PARAM_MAX_LEN in action_param and len(value) > action_param[actions.PARAM_MAX_LEN]:
                    raise ValueError(ERR_MAX_LEN.format(value, action_param[actions.PARAM_MAX_LEN], param_name))
                if actions.PARAM_PATTERN in action_param and not re.match(action_param[actions.PARAM_PATTERN], value):
                    raise ValueError(ERR_PATTERN_VALUE.format(value, action_param[actions.PARAM_PATTERN], param_name))

        def verify_known_parameter(parameters, action_params):
            # test for unknown parameters in the task definition
            for tp in parameters:
                if tp not in action_params:
                    if len(action_params) > 0:
                        raise ValueError(ERR_INVALID_PARAMETER.format(tp, action_name, ", ".join(action_params)))
                    else:
                        raise ValueError(ERR_NO_PARAMETERS.format(tp, action_name))

        def verify_parameter_type(value, action_param):
            parameter_type = action_param.get(actions.PARAM_TYPE)
            if parameter_type is not None:
                if type(value) != parameter_type:
                    try:
                        # value does not match type, try to convert
                        if parameter_type == bool:
                            return TaskConfiguration.as_boolean(str(value))
                        return parameter_type(value)
                    except Exception:
                        # not possible to convert to required type
                        raise ValueError(
                            ERR_WRONG_PARAM_TYPE.format(param_name, str(parameter_type), parameter_value, type(parameter_value)))
            return value

        def verify_allowed_values(value, action_param):
            if actions.PARAM_ALLOWED_VALUES in action_param and value not in action_param[actions.PARAM_ALLOWED_VALUES]:
                raise ValueError(
                    ERR_NOT_ALLOWED_VALUE.format(str(parameter_value), ",".join(action_param[actions.PARAM_ALLOWED_VALUES]),
                                                 param_name))

        def verify_required_parameter_available(parameter_name, action_params, parameters):
            if action_params[parameter_name].get(actions.PARAM_REQUIRED, False) and parameter_name not in parameters:
                raise ValueError(ERR_REQUIRED_PARAM_MISSING.format(parameter_name))

        def get_param_value(name, action_param, parameters):
            value = parameters.get(name)
            if value is None:
                value = action_param.get(actions.PARAM_DEFAULT)
            return value

        def action_class_parameter_check(parameters, name):
            # get the class that implements the action and test if there is a static method for additional checks of the parameters
            action_class = actions.get_action_class(name)
            validate_params_method = getattr(action_class, actions.ACTION_VALIDATE_PARAMETERS_METHOD, None)
            # if the method exists then validate the parameters using the business logic for that class
            if validate_params_method is not None:
                return validate_params_method(parameters)
            return parameters

        action_properties = actions.get_action_properties(action_name)
        action_parameters = action_properties.get(actions.ACTION_PARAMETERS, {})

        verify_known_parameter(task_parameters, action_parameters)

        for param_name in action_parameters:

            verify_required_parameter_available(param_name, action_parameters, task_parameters)

            action_parameter = action_parameters[param_name]
            parameter_value = get_param_value(param_name, action_parameter, task_parameters)

            if parameter_value is not None:
                parameter_value = verify_parameter_type(parameter_value, action_parameter)
                verify_allowed_values(parameter_value, action_parameter)
                verify_numeric_parameter(parameter_value, action_parameter)
                verify_string_parameter(parameter_value, action_parameter)
                validated_parameters[param_name] = parameter_value

        validated_parameters = action_class_parameter_check(parameters=validated_parameters, name=action_name)

        return validated_parameters

    @staticmethod
    def validate_events(events, action_name):
        # get properties for action for the task and the actions parameter definitions
        action_properties = actions.get_action_properties(action_name)

        action_service = action_properties[actions.ACTION_SERVICE].lower()
        for event in events:
            event_service = event.split(":")[0].lower()
            if event_service != action_service:
                raise ValueError(MSG_EVENT_SERVICE_NOT_ACTION_SERVICE.format(action_service, action_name, event_service, event))
        return events

    def validate_regions(self, regions, action_name, ):
        action_properties = actions.get_action_properties(action_name)
        service_name = action_properties[actions.ACTION_SERVICE]

        if self.service_is_regional(service_name) and action_properties.get(actions.ACTION_MULTI_REGION, True):
            if regions is None or len(regions) == 0:
                return [boto3.Session().region_name]
            else:
                available_regions = self.service_regions(service_name)
                if len(regions) == 1 and list(regions)[0] == "*":
                    return available_regions

                for region in regions:
                    if region not in available_regions:
                        raise ValueError(MSG_BAD_REGION.format(region, service_name, ",".join(available_regions)))
                return list(regions)
        else:
            if regions is not None and len(regions) != 0:
                msg = WARN_NOT_REGIONAL_SERVICE.format(",".join(regions), service_name, action_name)
                self._warn(msg)
        return []

    def verified_timezone(self, tz_name):
        tz_lower = str(tz_name).lower()
        if tz_lower in _checked_timezones:
            return str(_checked_timezones[tz_lower])

        if tz_lower in _invalid_timezones:
            return None

        validated = self._all_timezones.get(tz_lower, None)
        if validated is not None:
            # keep list off approved timezones to make next checks much faster
            _checked_timezones[tz_lower] = pytz.timezone(validated)
            return validated
        else:
            _invalid_timezones.add(tz_lower)
            raise ValueError(ERR_INVALID_TIMEZONE.format(tz_name))

    @staticmethod
    def as_boolean(val):
        if val is not None:
            if type(val) == bool:
                return val
            s = str(val.lower())
            if s in configuration.BOOLEAN_TRUE_VALUES:
                return True
            if s in configuration.BOOLEAN_FALSE_VALUES:
                return False
        raise ValueError(MSG_INVALID_BOOL.format(str(val)))

    @staticmethod
    def verify_internal(internal, action_name):
        """
        Tests if the tasks that are not internal do not use actions that are marked as internal. If an internal
        action is used for a task that is not internal an exception is raised.
        :param internal: Value of task internal attribute
        :param action_name: name of the action
        :return: Validated internal setting
        """
        action_properties = actions.get_action_properties(action_name)
        action_is_internal = action_properties.get(actions.ACTION_INTERNAL, False)
        if not internal and action_is_internal:
            raise ValueError(MSG_ACTION_ONLY_INTERNAL, action_name)
        return internal

    def verify_cross_account_roles(self, this_account, roles, action_name):
        """
        Tests if cross account roles have a valid format and removes roles with duplicated account numbers
        :param this_account: The account that is used for the scheduler, none of resources are not processed for that account
        :param roles: List of cross account role arns
        :param action_name: Name of the action
        :return: List of verified roles
        """
        result = []
        accounts = [this_account] if this_account is not None else []

        action_properties = actions.get_action_properties(action_name)
        if not action_properties.get(actions.ACTION_CROSS_ACCOUNT, True):
            if len(roles) > 0:
                raise ValueError(ERR_NO_CROSS_ACCOUNT_OPERATIONS.format(action_name))
            if this_account is None:
                raise ValueError(ERR_THIS_ACCOUNT_MUST_BE_TRUE.format(action_name, configuration.CONFIG_THIS_ACCOUNT))

        for role in set(roles):
            if not TaskConfiguration.is_valid_role_arn(role):
                raise ValueError(MSG_ARN_FORMAT_INVALID.format(role))
            account = AwsService.account_from_role_arn(role)
            if account not in accounts:
                accounts.append(account)
                result.append(role)
            else:
                msg = WARN_OVERLAPPING_ROLES.format(account, role)
                self._warn(msg)

        return result

    @staticmethod
    def verify_interval(interval):
        """
        Verifies the cron interval, raises an error if the expression is not valid
        :param interval: The tested interval expression
        :return: Verified interval expression
        """
        try:
            expression = CronExpression(interval)
            expression.validate()
        except Exception as ex:
            raise ValueError(ERR_INVALID_CRON_EXPRESSION.format(interval, str(ex)))
        return interval

    @staticmethod
    def verify_timeout(action_name, timeout):
        completion_method = getattr(actions.get_action_class(action_name), handlers.COMPLETION_METHOD, None)
        if completion_method is None and timeout is not None:
            raise ValueError(ERR_TIMEOUT_NOT_ALLOWED.format(action_name))

        if completion_method is None:
            return None

        if timeout is None:
            action_properties = actions.get_action_properties(action_name)
            return action_properties.get(actions.ACTION_COMPLETION_TIMEOUT_MINUTES,
                                         actions.DEFAULT_COMPLETION_TIMEOUT_MINUTES_DEFAULT)

        try:
            result = int(str(timeout).partition(".")[0])
            if result > 0:
                return result
            else:
                raise ValueError(ERR_TIMEOUT_MUST_BE_GREATER_0.format(result))
        except ValueError as ex:
            raise ValueError(ERR_INVALID_NUMERIC_TIMEOUT.format(timeout, ex))

    def configuration_item_to_task(self, item):
        """
        Processes a configuration item into an internally used task specification. The method verifies the attributes from the
        configuration and sets defaults for missing items.
        :param item: Task configuration item
        :return: Task item
        """

        action_name = self.validate_action(item.get(configuration.CONFIG_ACTION_NAME))

        process_this_account = TaskConfiguration.as_boolean(item.get(configuration.CONFIG_THIS_ACCOUNT, True))
        account = self.this_account if process_this_account else None
        task_name = item[configuration.CONFIG_TASK_NAME]

        cross_account_roles = item.get(configuration.CONFIG_CROSS_ACCOUNT_ROLES, [])
        s3_roles = self.s3_configured_cross_account_roles.get(task_name, [])
        cross_account_roles += s3_roles

        try:
            result = {
                handlers.TASK_NAME: task_name,
                handlers.TASK_ACTION: action_name,

                handlers.TASK_REGIONS: self.validate_regions(regions=item.get(configuration.CONFIG_REGIONS, []),
                                                             action_name=action_name),
                handlers.TASK_THIS_ACCOUNT: process_this_account,
                handlers.TASK_INTERVAL: TaskConfiguration.verify_interval(interval=item.get(configuration.CONFIG_INTERVAL, None)),

                handlers.TASK_EVENTS: TaskConfiguration.validate_events(events=item.get(configuration.CONFIG_EVENTS, {}),
                                                                        action_name=action_name),

                handlers.TASK_TIMEZONE: self.verified_timezone(tz_name=item.get(configuration.CONFIG_TIMEZONE, DEFAULT_TIMEZONE)),

                handlers.TASK_TIMOUT: self.verify_timeout(action_name=action_name,
                                                          timeout=item.get(configuration.CONFIG_TASK_TIMEOUT)),

                handlers.TASK_TAG_FILTER: TaskConfiguration.validate_tagfilter(tag_filter=item.get(configuration.CONFIG_TAG_FILTER),
                                                                               action_name=action_name),

                handlers.TASK_DRYRUN: TaskConfiguration.as_boolean(val=item.get(configuration.CONFIG_DRYRUN, False)),
                handlers.TASK_DEBUG: TaskConfiguration.as_boolean(val=item.get(configuration.CONFIG_DEBUG, False)),
                handlers.TASK_ENABLED: TaskConfiguration.as_boolean(val=item.get(configuration.CONFIG_ENABLED, True)),
                handlers.TASK_INTERNAL: TaskConfiguration.as_boolean(val=item.get(configuration.CONFIG_INTERNAL, False)),

                handlers.TASK_CROSS_ACCOUNT_ROLES: self.verify_cross_account_roles(account, cross_account_roles, action_name),

                handlers.TASK_DESRIPTION: item.get(configuration.CONFIG_DESCRIPTION),

                handlers.TASK_PARAMETERS: TaskConfiguration.verify_task_parameters(
                    task_parameters=item.get(configuration.CONFIG_PARAMETERS, {}), action_name=action_name)
            }
            return result

        except ValueError as ex:
            raise ValueError(ERR_ERR_IN_CONFIG_ITEM.format(item, str(ex)))

    def _verify_configuration_item(self, **task_attributes):
        """
        Verifies the parameters for creating or updating a task configuration item
        :param task_attributes: The configuration parameters
    
        Constants can be found in configuration/__init__.py
        -CONFIG_ACTION_NAME: Name of the action executed by the task, exception is raised if not specified or action does not
        exist (mandatory, string)
    
        -CONFIG_DEBUG: Set to True to log additional debug information for this task (optional, default False, boolean)
    
        -CONFIG_DESCRIPTION: Task description(optional, default None, string)
    
        -CONFIG_CROSS_ACCOUNT_ROLES: List of cross accounts for cross account processing. Note that roles if the account of a role
        has already been found in another role, or if the account of a role is the processed account of the scheduler a warning
        is generated when executing the task and the role is skipped (optional, default [], List<string>)
    
        -CONFIG_ENABLED: Set to True to enable execution of task, False to suspend executions (optional, default True, boolean)
    
        -CONFIG_INTERNAL: Flag to indicate task is used for internal  tasks of the scheduler (optional, default False, boolean)
        
        -CONFIG_TASK_TIMEOUT: Timeout in minutes for task to complete (optional,default is action's value or global timeout, number)
    
        -CONFIG_INTERVAL: Cron expression to schedule time/date based execution of task (optional, default "", string)
    
        -CONFIG_TASK_NAME: Name of the task, exception is raised if not specified or name does already exist (mandatory, string)
    
        -CONFIG_PARAMETERS: dictionary with names and values passed to the executed action of this task (optional, default {},
        dictionary)
    
        -CONFIG_THIS_ACCOUNT: Set to True to run tasks for resources in the account of the (optional, default True, boolean)
    
        -CONFIG_TIMEZONE: Timezone for time/date based tasks for this task (optional, default UTC, string)
    
        -CONFIG_TAG_FILTER: Tag filter used to select resources for the task instead of name of task in the list of values for the
        automation tag. Only allowed if selected resources support tags (optional, default "", string)
    
        -CONFIG_REGIONSs: Regions in which to run the task. Use "*" for all regions in which the service for this tasks action is
        available.
        If no regions are specified the region in which the scheduler is installed is used as default. Specifying one or more
        regions for services tha are not region specific will generate a warning when processing the task. (optional,
        default current region, List<string>)
    
        -CONFIG_STACK_ID: Id of the stack if the task is created as part of a cloudformation template (optional, default None,
        string)
    
        -CONFIG_DRYRUN: Dryrun parameter passed to the executed action (optional, default False, boolean)
    
        -CONFIG_EVENTS: List of resource events that trigger the task to be executed  (optional, default, List<string>)
    
        -CONFIG_DRYRUN: Dryrun parameter passed to the executed action (optional, default False, boolean)
    
        -CONFIG_EVENTS: List of resource events that trigger the task to be executed  (optional, default, List<string>)
    
        :return: Verified configuration item
        """

        result = {}

        valid_attributes = [configuration.CONFIG_ACTION_NAME,
                            configuration.CONFIG_DEBUG,
                            configuration.CONFIG_DESCRIPTION,
                            configuration.CONFIG_CROSS_ACCOUNT_ROLES,
                            configuration.CONFIG_ENABLED,
                            configuration.CONFIG_TASK_TIMEOUT,
                            configuration.CONFIG_INTERVAL,
                            configuration.CONFIG_INTERNAL,
                            configuration.CONFIG_TASK_NAME,
                            configuration.CONFIG_PARAMETERS,
                            configuration.CONFIG_THIS_ACCOUNT,
                            configuration.CONFIG_TIMEZONE,
                            configuration.CONFIG_TAG_FILTER,
                            configuration.CONFIG_REGIONS,
                            configuration.CONFIG_DRYRUN,
                            configuration.CONFIG_EVENTS,
                            configuration.CONFIG_STACK_ID]

        def remove_empty_attributes(o):

            def clean_dict(d):
                result_dict = {}
                for k, v in d.iteritems():
                    vv = remove_empty_attributes(v)
                    if vv is not None:
                        result_dict[k] = vv
                return result_dict if len(result_dict) > 0 else None

            def clean_val(l):
                result_list = []
                for i in l:
                    ii = remove_empty_attributes(i)
                    if ii is not None:
                        result_list.append(ii)
                return result_list if len(result_list) > 0 else None

            if isinstance(o, dict):
                return clean_dict(o)
            elif isinstance(o, type([])):
                return clean_val(o)
            else:
                return o if o is not None and len(str(o)) > 0 else None

        attributes = remove_empty_attributes(task_attributes)

        # test for missing required parameters
        for attr in [configuration.CONFIG_TASK_NAME, configuration.CONFIG_ACTION_NAME]:
            if attr not in attributes or len(attributes[attr]) == 0:
                raise ValueError(ERR_MISSING_REQUIRED_PARAMETER.format(attr))

        # test for unknown parameters
        for attr in attributes:
            if attr not in valid_attributes:
                raise ValueError(ERR_UNKNOWN_PARAMETER.format(attr, ",".join(valid_attributes)))

        result[configuration.CONFIG_TASK_NAME] = attributes[configuration.CONFIG_TASK_NAME]
        action_name = self.validate_action(attributes[configuration.CONFIG_ACTION_NAME])
        result[configuration.CONFIG_ACTION_NAME] = action_name

        result[configuration.CONFIG_PARAMETERS] = TaskConfiguration.verify_task_parameters(
            attributes.get(configuration.CONFIG_PARAMETERS, {}), action_name)

        process_this_account = TaskConfiguration.as_boolean(attributes.get(configuration.CONFIG_THIS_ACCOUNT, True))
        result[configuration.CONFIG_THIS_ACCOUNT] = process_this_account
        account = self.this_account if process_this_account else None

        for attr in attributes:

            if attr in [configuration.CONFIG_TASK_NAME, configuration.CONFIG_ACTION_NAME, configuration.CONFIG_PARAMETERS]:
                continue

            try:
                # verify cross-account roles
                if attr == configuration.CONFIG_CROSS_ACCOUNT_ROLES:
                    if len(attributes[attr]) > 0:
                        result[attr] = self.verify_cross_account_roles(account, attributes[attr], action_name)
                    continue

                # verify boolean enabled, dryrun and debug parameters
                if attr in [configuration.CONFIG_ENABLED, configuration.CONFIG_DRYRUN, configuration.CONFIG_DEBUG]:
                    result[attr] = TaskConfiguration.as_boolean(attributes[attr])
                    continue

                # verify interval (cron) expression
                if attr == configuration.CONFIG_INTERVAL:
                    result[attr] = TaskConfiguration.verify_interval(attributes[attr])
                    continue

                # verify timeout for task
                if attr == configuration.CONFIG_TASK_TIMEOUT:
                    timeout = TaskConfiguration.verify_timeout(action_name, attributes[attr])
                    if timeout is not None:
                        result[attr] = timeout
                    continue

                # verify timezone
                if attr == configuration.CONFIG_TIMEZONE:
                    result[attr] = self.verified_timezone(attributes[attr]) or DEFAULT_TIMEZONE
                    continue

                # verify tag filter
                if attr == configuration.CONFIG_TAG_FILTER:
                    tag_filter = TaskConfiguration.validate_tagfilter(attributes[attr], action_name)
                    if tag_filter is not None:
                        result[attr] = tag_filter
                    continue

                # verify events
                if attr == configuration.CONFIG_EVENTS:
                    result[attr] = TaskConfiguration.validate_events(attributes[attr], action_name)
                    continue

                # verify regions
                if attr == configuration.CONFIG_REGIONS:
                    result[attr] = "*" if attributes[attr] in ["*", ["*"]] else self.validate_regions(attributes[attr], action_name)

                # verify internal
                if attr == configuration.CONFIG_INTERNAL:
                    result[attr] = TaskConfiguration.verify_internal(attributes[attr], action_name)
                    continue

                # copy description and stack
                if attr in [configuration.CONFIG_DESCRIPTION, configuration.CONFIG_STACK_ID]:
                    result[attr] = attributes[attr]

            except ValueError as ex:
                raise ValueError("Parameter : {}, ({})".format(attr, str(ex)))

        # default for enabled parameter
        if configuration.CONFIG_ENABLED not in result:
            result[configuration.CONFIG_ENABLED] = True

        # default for region
        if configuration.CONFIG_REGIONS not in result:
            regions = self.validate_regions(None, action_name)
            if len(regions) > 0:
                result[configuration.CONFIG_REGIONS] = self.validate_regions(None, action_name)

        # set internal flag if the task action i internal
        if configuration.CONFIG_INTERNAL not in result:
            if actions.get_action_properties(action_name).get(actions.ACTION_INTERNAL, False):
                result[configuration.CONFIG_INTERNAL] = True

        return result

    def get_external_task_configuration_stacks(self):
        """
        Returns list of external stacks that have task configuration items in the configuration table
        :return: list of task configuration stacks
        """
        stacks = []
        this_stack = os.getenv(handlers.ENV_STACK_ID)
        for item in self.config_items(include_internal=True):
            stack = item.get(configuration.CONFIG_STACK_ID)
            if stack is not None and stack != this_stack:
                stacks.append(stack)
        return stacks

    def get_tasks(self, include_internal=True):
        """
        Gets a list of all configured tasks, processed to be used internally by the scheduler
        :param include_internal: include internal tasks
        :return:
        """
        for config_item in self.config_items(include_internal=include_internal):
            try:
                yield self.configuration_item_to_task(config_item)
            except Exception as ex:
                self._warn(str(ex))
