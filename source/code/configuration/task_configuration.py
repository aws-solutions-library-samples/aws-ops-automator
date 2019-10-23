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
import copy
import datetime
import decimal
import json
import os
import re
import types

import boto3

import actions
import boto_retry
import configuration
import handlers
import metrics
import metrics.task_metrics
import pytz
import services
from boto_retry import get_client_with_retries
from helpers import safe_json
from outputs import raise_exception, raise_value_error
from scheduling.cron_expression import CronExpression

VALID_EVENT_SCOPES = [handlers.EVENT_SCOPE_RESOURCE, handlers.EVENT_SCOPE_REGION]

VALID_TASK_ATTRIBUTES = [
    configuration.CONFIG_ACCOUNTS,
    configuration.CONFIG_ACTION_NAME,
    configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME,
    configuration.CONFIG_DEBUG,
    configuration.CONFIG_DESCRIPTION,
    configuration.CONFIG_DRYRUN,
    configuration.CONFIG_ECS_COMPLETION_MEMORY,
    configuration.CONFIG_ECS_EXECUTE_MEMORY,
    configuration.CONFIG_ECS_SELECT_MEMORY,
    configuration.CONFIG_EVENT_SOURCE_TAG_FILTER,
    configuration.CONFIG_ENABLED,
    configuration.CONFIG_EVENT_SCOPES,
    configuration.CONFIG_EVENTS,
    configuration.CONFIG_INTERNAL,
    configuration.CONFIG_INTERVAL,
    configuration.CONFIG_PARAMETERS,
    configuration.CONFIG_REGIONS,
    configuration.CONFIG_STACK_ID,
    configuration.CONFIG_TAG_FILTER,
    configuration.CONFIG_TASK_COMPLETION_SIZE,
    configuration.CONFIG_TASK_EXECUTE_SIZE,
    configuration.CONFIG_TASK_METRICS,
    configuration.CONFIG_TASK_NAME,
    configuration.CONFIG_TASK_NOTIFICATIONS,
    configuration.CONFIG_TASK_SELECT_SIZE,
    configuration.CONFIG_TASK_TIMEOUT,
    configuration.CONFIG_THIS_ACCOUNT,
    configuration.CONFIG_TIMEZONE
]

SSM_PARAM_REGEX = r"^{ssm:(.+)\}$"

VALID_ACCOUNT_REGEX = r"^\d{12}$"
VALID_ROLE_NAME_REGEX = "^[\w+=,\.@-]{1,64}$"

DEFAULT_TIMEZONE = "UTC"

INF_READ_ARN_RESULT = "Read {} cross account arns for task with name {}{}"
INF_READING_OBJECT = "Reading task cross account roles file {}"
INF_REMOVE_TOPIC_PERMISSION = "Remove permission for account {} to public on ops automator topic, label = {}"
INF_ADD_ACCOUNT_TOPIC_PERMISSION = "Add permission for account {} to publish on ops automator topic, label is {}"

WARN_NOT_REGIONAL_SERVICE = "One or more regions ({}) are specified but service \"{}\" or action {} is not a regional service"
WARN_READING_TASK_ROLES = "Error reading roles from {} in bucket {}, ({})"
WARN_IGNORED_YEAR = "Year field not supported for cron expression {}, value in years field \"{}\" is ignored as it's a wildcard"
WARN_INVALID_PARAMETER = "Parameter \"{}\" is not a valid parameter for action \"{}\", valid parameters are {}"
WARN_NO_PARAMETERS = "Parameter \"{}\" is not a valid parameter, action \"{}\" has no parameters"
WARN_DOUBLE_ACCOUNTS = "Account {} in account list is duplicated or same as own account number for task {}"

ERR_ACTION_ONLY_INTERNAL = "Action {} is marked as an internal action and can only be used in internal tasks"
ERR_BAD_REGION = "Region \"{}\" is not a valid region for service \"{}\", available regions are {}"
ERR_CREATING_TASK_OBJECT = "Error creating task config objects for task {} in {}/{}, {}"
ERR_DETAIL_TYPE_NOT_HANDLED = "Event detail type {} not handled by action, types supported for source {} are {}"
ERR_ERR_IN_CONFIG_ITEM = "Error in configuration item : {} ({})"
ERR_EVENT_NOT_HANDLED = "Event {} for event source {}, detail type {} is not handled by this action, valid events are {}"
ERR_EVENT_SCOPE_DETAIL_TYPE_NOT_HANDLED = "Event detail type {} not handled by action or detail type does not allow scope " \
                                          "definition"
ERR_EVENT_SCOPE_EVENT_NOT_HANDLED = "Event {} for event source {}, detail type {} is not handled by this action or event does " \
                                    "not allow setting the event scope"
ERR_EVENT_SCOPE_SOURCE_NOT_HANDLED = "Events of source {} are not supported by this action or source has no events that allow " \
                                     "setting the event scope"
ERR_EVENT_SOURCE_NOT_HANDLED = "Events of source {} are not supported by this action, supported sources are \"{}\""
ERR_INVALID_ACTION_NAME = "Action with name \"{}\" does not exist, available actions are {}"
ERR_INVALID_BOOL = "\"{}\" is not a valid boolean value"
ERR_INVALID_CRON_EXPRESSION = "{} is not a valid cron expression, ({})"
ERR_INVALID_ECS_MEMORY_SIZE = "{} is not a valid value for Ecs memory size {} parameter."
ERR_INVALID_LAMBDA_SIZE = "{} is not a valid size, possible values are {}"
ERR_INVALID_NUMERIC_TIMEOUT = "{} is not a valid numeric value for timeout parameter ({})"

ERR_INVALID_TASK_INTERVAL = "Interval cron expression \"{}\" for task {} must have 5 fields"
ERR_INVALID_TIMEZONE = "\"{}\" is not a valid timezone"
ERR_MAX_LEN = "Value \"{}\" is longer than maximum length {} for parameter {}"
ERR_MAX_VALUE = "Value {} is higher than maximum value {} for parameter {}"
ERR_MIN_LEN = "Value \"{}\" is shorter than minimum length {} for parameter {}"
ERR_MIN_VALUE = "Value {} is less than minimum value {} for parameter {}"
ERR_MISSING_REQUIRED_PARAMETER = "error: parameter \"{}\" must exists and can not be empty"
ERR_NO_CROSS_ACCOUNT_OPERATIONS = "Action {} does not support cross account operations"
ERR_NO_TAG_FILTER = "Resource type \"{}\" for task does not support tags, tag-filer \"{}\" not allowed for action \"{}\""
ERR_NO_TASK_ACTION_NAME = "Action name not specified"
ERR_NO_WILDCARDS_TAG_FILTER_ALLOWED = "Tag wildcard filter \"{}\" is not allowed for name of tag in tagfilter for action \"{}\""
ERR_NOT_ALLOWED_VALUE = "Value \"{}\" is not in list of allowed values ({}) for parameter {}"
ERR_PATTERN_VALUE = "Value \"{}\" does not match allowed pattern \"{}\" for parameter \"{}\""
ERR_REQUIRED_PARAM_MISSING = "Required parameter \"{}\" is missing"
ERR_SSM_PARAM_NOT_FOUND = "SSM Parameter {} not found"
ERR_TASK_INTERVAL_TOO_SHORT = "Interval between executions must be at least {} minutes, interval \"{}\" can not be used as its " \
                              "time interval is {} minutes"
ERR_THIS_ACCOUNT_MUST_BE_TRUE = "Action {} only supports operations in this account, parameter \"{}\"must be set to value true"
ERR_TIMEOUT_MUST_BE_GREATER_0 = "Timeout parameter value must be > 0, current value is {}"
ERR_TIMEOUT_NOT_ALLOWED = "Action {} has no completion handling, timeout parameter not allowed"
ERR_UNKNOWN_PARAMETER = "error: parameter \"{}\" is not a valid parameter, valid parameters are {}"
ERR_VALIDATING_TASK_PARAMETERS = "Error validating parameters for task {}, {}"
ERR_WRONG_PARAM_TYPE = "Type of parameter \"{}\" must be \"{}\", current type for value {} is {}"
ERR_INVALID_EVENT_SCOPE = "Event scope {} is not valid, valid values are {}"
ERR_INVALID_ACCOUNT_NUMBER_FORMAT = "{} is not a valid account number"
ERR_INVALID_ROLE_NAME = "\"{}\" in task {} is not a valid role name"
ERR_FETCHING_TASKS_FROM_CONFIG = "Error getting tasks {}"

_checked_timezones = dict()
_invalid_timezones = set()

_service_regions = {}
_service_is_regional = {}


class TaskConfiguration(object):
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
        self._ssm_client = None

    @property
    def config_table(self):
        """
        Returns the configuration table
        :return: the configuration table
        """
        table_name = os.getenv(configuration.ENV_CONFIG_TABLE)

        table = boto3.resource("dynamodb").Table(table_name)
        boto_retry.add_retry_methods_to_resource(table, ["scan", "get_item", "delete_item", "put_item"], context=self._context)

        return table

    @property
    def s3_client(self):
        """
        Returns S3 client for handling configuration files
        :return: S3 client
        """
        if self._s3_client is None:
            # IMPORTANT, list_objects and list_objects_v2 require s3:ListBucket permission !!
            self._s3_client = boto_retry.get_client_with_retries("s3", ["list_objects_v2", "get_object"])
        return self._s3_client

    @property
    def ssm_client(self):
        if self._ssm_client is None:
            self._ssm_client = get_client_with_retries("ssm", ["get_parameters"])
        return self._ssm_client

    @staticmethod
    def config_table_exists():
        tablename = os.environ[configuration.ENV_CONFIG_TABLE]
        for t in boto3.resource("dynamodb").tables.all():
            if t.table_name == tablename:
                return True
        return False

    def _info(self, msg, *args):
        if self._logger:
            self._logger.info(msg, *args)
        else:
            print((msg.format(*args)))

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

        # get the regions for which item may have event permissions
        self.config_table.delete_item_with_retries(Key={configuration.CONFIG_TASK_NAME: name})

        # update event topic permissions
        self._update_ops_automator_topic_permissions()

        return {"name": name}

    def put_config_item(self, **kwargs):
        """
        Writes a task item to the table after validating the input arguments. If the item with the name as specified in the
        task argument already exists it is overwritten
        :param kwargs:
        :return:
        """

        def remove_empty_strings(item):

            if item is None:
                return None

            if isinstance(item, list):
                return [remove_empty_strings(j) for j in item]

            if isinstance(item, dict):
                item = {i: remove_empty_strings(item[i]) for i in item}

            if isinstance(item, str):
                if len(item.strip()) == 0:
                    return None

            return item

        config_item = self._verify_configuration_item(**kwargs)
        config_item = remove_empty_strings(remove_empty_strings(config_item))

        # test if update has action that has event settings
        event_regions = set()
        if len(actions.get_action_properties(config_item[configuration.CONFIG_ACTION_NAME]).get(actions.ACTION_EVENTS, {})) > 0:
            # get the regions with event bus permissions
            event_regions.update(self._regions_for_tasks_with_events())
            event_regions.update(config_item.get(configuration.CONFIG_REGIONS, []))

        self.config_table.put_item_with_retries(Item=config_item)

        # update topic permissions
        self._update_ops_automator_topic_permissions()

        if config_item[configuration.CONFIG_TASK_METRICS]:
            metrics.setup_tasks_metrics(task=config_item[configuration.CONFIG_TASK_NAME],
                                        action_name=config_item[configuration.CONFIG_ACTION_NAME],
                                        task_level_metrics=config_item[configuration.CONFIG_TASK_METRICS],
                                        context=self._context,
                                        logger=self._logger)

        self.create_task_config_objects(config_item)

        return config_item

    def create_task_config_objects(self, config_item):
        # get the class that implements the action and test if there is a static method for creating templates
        action_class = actions.get_action_class(config_item[configuration.CONFIG_ACTION_NAME])
        create_task_objects_method = getattr(action_class, "create_task_config_objects", None)
        # if the method exists then validate the parameters using the business logic for that class
        bucket = os.getenv(configuration.ENV_CONFIG_BUCKET)
        prefix = "{}/{}/{}/".format(configuration.TASKS_OBJECTS, config_item[configuration.CONFIG_ACTION_NAME],
                                    config_item[configuration.CONFIG_TASK_NAME])
        task_name = config_item[configuration.CONFIG_TASK_NAME]
        try:
            if create_task_objects_method is not None:
                cfg = self.get_parameters(config_item)
                objects = create_task_objects_method(cfg)
                if objects is not None:
                    s3 = boto3.client("s3")
                    for t in objects:
                        s3.put_object(Bucket=bucket, Key=prefix + t, Body=objects[t])
                        self._logger.info("Created config object {}/{} in bucket {} for task {}", prefix, t, bucket, task_name)
        except Exception as ex:
            self._logger.error(ERR_CREATING_TASK_OBJECT, task_name, bucket, prefix, ex)

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
    def is_valid_account_number(arn):
        return re.match(VALID_ACCOUNT_REGEX, arn) is not None

    def validate_action(self, action_name):
        """
        Tests if an action name is a known action name
        :param action_name: The name of the action
        :return: The name of the action if it is valid, if not an exception is raised
        """
        if action_name is None or action_name.strip() == "":
            raise_value_error(ERR_NO_TASK_ACTION_NAME)
        result = action_name.strip()
        if result not in self._all_actions:
            raise_value_error(ERR_INVALID_ACTION_NAME, result, ",".join(sorted(self._all_actions)))
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

        if tag_filter is not None:
            tag_filter = tag_filter.strip()

        if tag_filter in ["None", None, ""]:
            return None

        action_properties = actions.get_action_properties(action_name)
        resources = action_properties.get(actions.ACTION_RESOURCES)
        resources_with_tags = services.create_service(action_properties[actions.ACTION_SERVICE]).resources_with_tags

        resource_supports_tags = (resources == "" and len(resources_with_tags) > 0) or resources in resources_with_tags

        # resource does not allow tags, so tag filters can not be used
        if not resource_supports_tags:
            raise_value_error(ERR_NO_TAG_FILTER, action_properties[actions.ACTION_RESOURCES], tag_filter, action_name)

        # destructive actions can deny use of wildcards for tag name
        if not action_properties.get(actions.ACTION_ALLOW_TAGFILTER_WILDCARD, True):
            if "".join([s.strip() for s in tag_filter.split("=")[0:1]]) in ["*", "**", "*="]:
                raise_value_error(ERR_NO_WILDCARDS_TAG_FILTER_ALLOWED, tag_filter, action_name)

        return tag_filter

    def verify_task_parameters(self, task_parameters, task_settings, action_name):
        """
        Validates parameter set and values for the specified action.
        A ValueException is raised in the following cases:
        -Required parameter is not available and there is no default specified for the action
        -Unknown parameter found
        -Type of parameter is wrong and value can task_parameters not be converted to the required type
        -Value is out of range specified by min and max value for numeric parameters
        -Value is too long, too short or does not mats the allowed pattern for string parameters
        :param task_parameters: Dictionary of parameters, keys are name of the parameters, value is parameter value
        :param task_settings: Task settings without parameters included yet
        :param action_name: Name of the action
        :return: Dictionary of validated parameters, missing non required parameters are set to default if specified in action
        implementation
        """
        validated_parameters = {}

        def verify_numeric_parameter(value, action_param):
            if type(value) in [int, float, int, complex, decimal]:
                if actions.PARAM_MIN_VALUE in action_param and value < action_param[actions.PARAM_MIN_VALUE]:
                    raise_value_error(ERR_MIN_VALUE, value, action_param[actions.PARAM_MIN_VALUE], param_name)
                if actions.PARAM_MAX_VALUE in action_param and value > action_param[actions.PARAM_MAX_VALUE]:
                    raise_value_error(ERR_MAX_VALUE, value, action_param[actions.PARAM_MAX_VALUE], param_name)

        def verify_string_parameter(value, action_param):
            if type(value) in [str, str]:
                if actions.PARAM_MIN_LEN in action_param and len(value) < action_param[actions.PARAM_MIN_LEN]:
                    raise_value_error(ERR_MIN_LEN, value, action_param[actions.PARAM_MIN_LEN], param_name)
                if actions.PARAM_MAX_LEN in action_param and len(value) > action_param[actions.PARAM_MAX_LEN]:
                    raise_value_error(ERR_MAX_LEN, value, action_param[actions.PARAM_MAX_LEN], param_name)
                if actions.PARAM_PATTERN in action_param and not re.match(action_param[actions.PARAM_PATTERN], value):
                    raise_value_error(ERR_PATTERN_VALUE, value, action_param[actions.PARAM_PATTERN], param_name)

        def verify_known_parameter(parameters, action_params):
            # test for unknown parameters in the task definition
            for tp in parameters:
                if tp not in action_params:
                    if len(action_params) > 0:
                        self._logger.warning(WARN_INVALID_PARAMETER, tp, action_name, ", ".join(action_params))
                    else:
                        self._logger.warning(WARN_NO_PARAMETERS, tp, action_name)

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
                            ERR_WRONG_PARAM_TYPE.format(param_name, str(parameter_type), parameter_value,
                                                        type(parameter_value)))
            return value

        def verify_allowed_values(value, action_param):
            if actions.PARAM_ALLOWED_VALUES in action_param and value not in action_param[actions.PARAM_ALLOWED_VALUES]:
                raise ValueError(
                    ERR_NOT_ALLOWED_VALUE.format(str(parameter_value), ",".join(action_param[actions.PARAM_ALLOWED_VALUES]),
                                                 param_name))

        def verify_required_parameter_available(parameter_name, action_params, parameters):
            if action_params[parameter_name].get(actions.PARAM_REQUIRED, False) and parameter_name not in parameters:
                raise_value_error(ERR_REQUIRED_PARAM_MISSING, parameter_name)

        def get_param_value(name, action_param, parameters):
            value = parameters.get(name)
            if value is None:
                value = action_param.get(actions.PARAM_DEFAULT)
            return value

        def action_class_parameter_check(parameters, tsk_settings, name):
            # get the class that implements the action and test if there is a static method for additional checks of the parameters
            action_class = actions.get_action_class(name)
            validate_params_method = getattr(action_class, handlers.ACTION_VALIDATE_PARAMETERS_METHOD, None)
            # if the method exists then validate the parameters using the business logic for that class
            try:
                if validate_params_method is not None:
                    return validate_params_method(parameters, tsk_settings, self._logger)
            except Exception as ex:
                self._logger.error(ERR_VALIDATING_TASK_PARAMETERS, name, ex)
                raise_value_error(ERR_VALIDATING_TASK_PARAMETERS, name, ex)

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

        validated_parameters = action_class_parameter_check(parameters=validated_parameters, tsk_settings=task_settings,
                                                            name=action_name)

        return validated_parameters

    @staticmethod
    def validate_events(events, action_name):

        validated = {}
        # get properties for action for the task and the actions parameter definitions
        action_properties = actions.get_action_properties(action_name)

        action_events = action_properties.get(configuration.CONFIG_EVENTS, {})
        for source in events:
            if source not in action_events:
                raise_value_error(ERR_EVENT_SOURCE_NOT_HANDLED, source, ",".join(action_events))

            action_detail_types = action_events.get(source, {})
            for detail_type in events[source]:
                if detail_type not in action_detail_types:
                    raise_value_error(ERR_DETAIL_TYPE_NOT_HANDLED, detail_type, source, ",".join(action_detail_types))

                action_event_names = action_detail_types.get(detail_type, [])

                for event in events[source][detail_type]:
                    if event not in action_event_names:
                        raise_value_error(ERR_EVENT_NOT_HANDLED, event, source, detail_type, ",".join(action_event_names))

                # if the events are validated from a dynamodb item it is a list of events
                if isinstance(events[source][detail_type], list):
                    events_for_detail_type = events[source][detail_type]
                else:
                    # coming from update in cloudformation, it is a dictionary where the value for every key holds
                    # the value of the event is used or not
                    events_for_detail_type = [e for e in events[source][detail_type] if
                                              TaskConfiguration.as_boolean(events[source][detail_type][e])]

                if len(events_for_detail_type) > 0:
                    if source not in validated:
                        validated[source] = {}
                    validated[source][detail_type] = events_for_detail_type

        return validated

    @staticmethod
    def validate_event_scopes(scopes, action_name):

        validated = {}
        # get properties for action for the task and the actions parameter definitions
        action_properties = actions.get_action_properties(action_name)

        action_scopes = action_properties.get(configuration.CONFIG_EVENT_SCOPES, {})
        action_events = action_properties.get(configuration.CONFIG_EVENTS, {})
        for source in scopes:
            if source not in action_scopes or source not in action_events:
                raise_value_error(ERR_EVENT_SCOPE_SOURCE_NOT_HANDLED, source)

            action_detail_event_scopes = action_scopes.get(source, {})
            action_detail_types = action_events.get(source, {})
            for detail_scopes_type in scopes[source]:
                if detail_scopes_type not in action_detail_event_scopes or detail_scopes_type not in action_detail_types:
                    raise_value_error(ERR_EVENT_SCOPE_DETAIL_TYPE_NOT_HANDLED, detail_scopes_type, source)

                action_scope_events = action_detail_event_scopes.get(detail_scopes_type, [])
                action_supported_events = action_detail_types.get(detail_scopes_type, [])
                for event in scopes[source][detail_scopes_type]:
                    if event not in action_scope_events or event not in action_supported_events:
                        raise_value_error(ERR_EVENT_SCOPE_EVENT_NOT_HANDLED, event, source, detail_scopes_type)
                    if action_scope_events[event] not in VALID_EVENT_SCOPES:
                        raise_value_error(ERR_INVALID_EVENT_SCOPE, action_scope_events[event], ",".join(VALID_EVENT_SCOPES))

                # only use values other than default resource value
                scopes_for_detail_type = {s: scopes[source][detail_scopes_type][s] for s in scopes[source][detail_scopes_type] if
                                          scopes[source][detail_scopes_type][s] != handlers.EVENT_SCOPE_RESOURCE}

                if len(scopes_for_detail_type) > 0:
                    if source not in validated:
                        validated[source] = {}
                    validated[source][detail_scopes_type] = scopes_for_detail_type

        return validated

    def validate_regions(self, regions, action_name, ):
        action_properties = actions.get_action_properties(action_name)
        service_name = action_properties[actions.ACTION_SERVICE]
        is_multi_region_action = action_properties.get(actions.ACTION_MULTI_REGION, True)

        if self.service_is_regional(service_name):
            if regions is None or len(regions) == 0:
                return [services.get_session().region_name]
            else:
                available_regions = self.service_regions(service_name)
                if len(regions) == 1 and list(regions)[0] == "*":
                    return available_regions if is_multi_region_action else [services.get_session().region_name]

                for region in regions:
                    if region not in available_regions:
                        raise_value_error(ERR_BAD_REGION, region, service_name, ",".join(available_regions))

                return list(regions)
        else:
            if regions is not None and len(regions) != 0:
                if self._logger is not None:
                    self._logger.warning(WARN_NOT_REGIONAL_SERVICE, ",".join(regions), service_name, action_name)

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
            raise_value_error(ERR_INVALID_TIMEZONE, tz_name)

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
        raise_value_error(ERR_INVALID_BOOL, str(val))

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
            raise_value_error(ERR_ACTION_ONLY_INTERNAL, action_name)
        return internal

    def verify_accounts(self, this_account, accounts, action_name, task_name):
        results = []

        action_properties = actions.get_action_properties(action_name)
        if not action_properties.get(actions.ACTION_CROSS_ACCOUNT, True):
            if len(accounts) > 0:
                raise_value_error(ERR_NO_CROSS_ACCOUNT_OPERATIONS, action_name)
            if this_account is None:
                raise_value_error(ERR_THIS_ACCOUNT_MUST_BE_TRUE, action_name, configuration.CONFIG_THIS_ACCOUNT)

        for account in set(accounts):
            if not TaskConfiguration.is_valid_account_number(account):
                raise_value_error(ERR_INVALID_ACCOUNT_NUMBER_FORMAT, account)

            if account in results or account == this_account:
                self._logger.warning(WARN_DOUBLE_ACCOUNTS, account, task_name)
            else:
                results.append(account)

        return results

    @staticmethod
    def verify_task_role_name(role_name, action_name):
        if role_name in ["", None]:
            return None

        role_name = role_name.strip()

        if re.match(VALID_ROLE_NAME_REGEX, role_name) is None:
            raise_value_error(ERR_INVALID_ROLE_NAME, role_name, action_name)

        return role_name

    def verify_interval(self, interval, item, action_name, task_name):

        action_properties = actions.get_action_properties(action_name)
        use_intervals = actions.ACTION_TRIGGER_INTERVAL[0] in action_properties.get(actions.ACTION_TRIGGERS,
                                                                                    actions.ACTION_TRIGGER_BOTH)

        if not use_intervals and interval is not None:
            raise ValueError("Interval is not used for action {}".format(action_name))

        if interval is not None:
            try:

                cron_elements = interval.split(" ")
                if len(cron_elements) != 5:
                    if len(cron_elements) == 6 and cron_elements[5] in ["?", "*"]:
                        self._logger.warning(WARN_IGNORED_YEAR, interval, cron_elements[5])
                    else:
                        raise_exception(ERR_INVALID_TASK_INTERVAL, interval, task_name)

                expression = CronExpression(interval)
                expression.validate()

                # test if there are concurrency restrictions
                min_interval = action_properties.get(actions.ACTION_MIN_INTERVAL_MIN)

                # no maximum
                if min_interval is not None:

                    # property may be a lambda function, call the function with parameters of task as lambda parameters
                    if types.FunctionType == type(min_interval):
                        parameters = item
                        min_interval = min_interval(parameters)

                    if min_interval is not None:
                        min_interval = max(1, min_interval)
                        e = CronExpression(" ".join(interval.split(" ")[0:2]) + " * * ?")
                        last = None
                        for i in e.within_next(timespan=datetime.timedelta(hours=25),
                                               start_dt=datetime.datetime.now().replace(hour=0, minute=0, second=0,
                                                                                        microsecond=0) - datetime.timedelta(
                                                   minutes=1)):
                            if last is not None:
                                between = i - last
                                if between < datetime.timedelta(minutes=min_interval):
                                    raise_value_error(ERR_TASK_INTERVAL_TOO_SHORT.format(min_interval, interval,
                                                                                         int(between.total_seconds() / 60)))
                            last = i

                return interval
            except Exception as ex:
                raise_value_error(ERR_INVALID_CRON_EXPRESSION, interval, str(ex))
        return interval

    @staticmethod
    def verify_timeout(action_name, timeout):
        completion_method = getattr(actions.get_action_class(action_name), handlers.COMPLETION_METHOD, None)
        if completion_method is None and timeout is not None:
            raise_value_error(ERR_TIMEOUT_NOT_ALLOWED, action_name)

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
                raise_value_error(ERR_TIMEOUT_MUST_BE_GREATER_0, result)
        except ValueError as ex:
            raise_value_error(ERR_INVALID_NUMERIC_TIMEOUT, timeout, ex)

    @staticmethod
    def validate_lambda_size(size):
        valid_sizes = {a.lower(): a for a in actions.ACTION_SIZE_ALL_WITH_ECS}
        if size.lower() not in valid_sizes:
            raise_value_error(ERR_INVALID_LAMBDA_SIZE, ", ".join(actions.ACTION_SIZE_ALL_WITH_ECS))
        return valid_sizes[size.lower()]

    def get_parameters(self, itm):

        def get_param(value):
            if isinstance(value, str) or isinstance(value, str):
                m = re.match(SSM_PARAM_REGEX, value)
                if m is not None:
                    name = m.groups()[0]
                    resp = self.ssm_client.get_parameters_with_retries(Names=[name])
                    if len(resp.get("Parameters", [])) > 0:
                        ssm_value = resp["Parameters"][0].get("Value", "")
                        ssm_type = resp["Parameters"][0].get("Type", "")
                        if ssm_type == "StringList":
                            return ssm_value.split(",")
                        return ssm_value
                    else:
                        self._logger.error(ERR_SSM_PARAM_NOT_FOUND, name)
            return value

        result_item = copy.deepcopy(itm)
        for i in result_item:

            if isinstance(result_item[i], dict):
                result_item[i] = self.get_parameters(result_item[i])
                continue

            if isinstance(result_item[i], list):
                temp = []
                for l in result_item[i]:
                    v = get_param(l)
                    if isinstance(v, list):
                        temp += v
                    else:
                        temp.append(v)
                result_item[i] = temp
                continue

            if isinstance(result_item[i], str) or isinstance(result_item[i], str):
                result_item[i] = get_param(result_item[i])

        return result_item

    def configuration_item_to_task(self, item):
        """
        Processes a configuration item into an internally used task specification. The method verifies the attributes from the
        configuration and sets defaults for missing items.
        :param item: Task configuration item
        :return: Task item

        """

        action_name = self.validate_action(item.get(configuration.CONFIG_ACTION_NAME))

        conf_item = self.get_parameters(item)

        process_this_account = TaskConfiguration.as_boolean(conf_item.get(configuration.CONFIG_THIS_ACCOUNT, True))
        account = self.this_account if process_this_account else None
        task_name = conf_item[configuration.CONFIG_TASK_NAME]

        try:
            result = {
                handlers.TASK_NAME: task_name, handlers.TASK_ACTION: action_name,

                handlers.TASK_REGIONS: self.validate_regions(regions=conf_item.get(configuration.CONFIG_REGIONS, []),
                                                             action_name=action_name),

                handlers.TASK_THIS_ACCOUNT: process_this_account,

                handlers.TASK_INTERVAL: self.verify_interval(
                    interval=conf_item.get(configuration.CONFIG_INTERVAL, None),
                    item=item,
                    action_name=action_name,
                    task_name=task_name),

                handlers.TASK_EVENTS: TaskConfiguration.validate_events(
                    conf_item.get(configuration.CONFIG_EVENTS, {}), action_name),

                handlers.TASK_EVENT_SCOPES: TaskConfiguration.validate_event_scopes(
                    conf_item.get(configuration.CONFIG_EVENT_SCOPES, {}), action_name),

                handlers.TASK_TIMEZONE: self.verified_timezone(
                    tz_name=conf_item.get(configuration.CONFIG_TIMEZONE, DEFAULT_TIMEZONE)),

                handlers.TASK_SELECT_SIZE: TaskConfiguration.validate_lambda_size(
                    conf_item.get(configuration.CONFIG_TASK_SELECT_SIZE,
                                  actions.ACTION_SIZE_STANDARD)),

                handlers.TASK_SELECT_ECS_MEMORY: conf_item.get(configuration.CONFIG_ECS_SELECT_MEMORY, None),

                handlers.TASK_EXECUTE_SIZE: TaskConfiguration.validate_lambda_size(
                    conf_item.get(configuration.CONFIG_TASK_EXECUTE_SIZE,
                                  actions.ACTION_SIZE_STANDARD)),

                handlers.TASK_EXECUTE_ECS_MEMORY: conf_item.get(
                    configuration.CONFIG_ECS_EXECUTE_MEMORY, None),

                handlers.TASK_COMPLETION_SIZE: TaskConfiguration.validate_lambda_size(
                    conf_item.get(configuration.CONFIG_TASK_COMPLETION_SIZE, actions.ACTION_SIZE_STANDARD)),

                handlers.TASK_COMPLETION_ECS_MEMORY: conf_item.get(
                    configuration.CONFIG_ECS_SELECT_MEMORY, None),

                handlers.TASK_TIMEOUT: self.verify_timeout(
                    action_name=action_name,
                    timeout=conf_item.get(configuration.CONFIG_TASK_TIMEOUT)),

                handlers.TASK_TAG_FILTER: TaskConfiguration.validate_tagfilter(
                    tag_filter=conf_item.get(configuration.CONFIG_TAG_FILTER),
                    action_name=action_name),

                handlers.TASK_EVENT_SOURCE_TAG_FILTER: TaskConfiguration.validate_tagfilter(
                    tag_filter=conf_item.get(configuration.CONFIG_EVENT_SOURCE_TAG_FILTER),
                    action_name=action_name),

                handlers.TASK_DRYRUN: TaskConfiguration.as_boolean(
                    val=conf_item.get(configuration.CONFIG_DRYRUN, False)),

                handlers.TASK_DEBUG: TaskConfiguration.as_boolean(
                    val=conf_item.get(configuration.CONFIG_DEBUG, False)),

                handlers.TASK_NOTIFICATIONS: TaskConfiguration.as_boolean(
                    val=conf_item.get(configuration.CONFIG_TASK_NOTIFICATIONS, False)),

                handlers.TASK_ENABLED: TaskConfiguration.as_boolean(
                    val=conf_item.get(configuration.CONFIG_ENABLED, True)),

                handlers.TASK_INTERNAL: TaskConfiguration.as_boolean(
                    val=conf_item.get(configuration.CONFIG_INTERNAL, False)),

                handlers.TASK_METRICS: TaskConfiguration.as_boolean(
                    val=conf_item.get(configuration.CONFIG_TASK_METRICS, False)),

                handlers.TASK_ACCOUNTS: self.verify_accounts(
                    account, conf_item.get(configuration.CONFIG_ACCOUNTS, []), action_name, task_name),

                handlers.TASK_ROLE: self.verify_task_role_name(
                    role_name=conf_item.get(configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME, ""),
                    action_name=action_name),

                handlers.TASK_DESCRIPTION: conf_item.get(configuration.CONFIG_DESCRIPTION),

                handlers.TASK_PARAMETERS: self.verify_task_parameters(
                    task_parameters=conf_item.get(configuration.CONFIG_PARAMETERS, {}),
                    task_settings=conf_item,
                    action_name=action_name),

                handlers.TASK_SERVICE: actions.get_action_properties(action_name).get(actions.ACTION_SERVICE, ""),

                handlers.TASK_RESOURCE_TYPE: actions.get_action_properties(action_name).get(actions.ACTION_RESOURCES, "")

            }

            return result

        except ValueError as ex:
            raise_value_error(ERR_ERR_IN_CONFIG_ITEM, safe_json(conf_item, indent=3), str(ex))

    def _verify_configuration_item(self, **task_attributes):
        """
        Verifies the parameters for creating or updating a task configuration item
        :param task_attributes: The configuration parameters

        Constants for dictionary keys can be found in configuration/__init__.py

        :return: Verified configuration item
        """

        result = {}

        valid_attributes = VALID_TASK_ATTRIBUTES

        def remove_empty_attributes(o):

            def clean_dict(d):
                result_dict = {}
                for k, v in d.items():
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
                raise_value_error(ERR_MISSING_REQUIRED_PARAMETER, attr)

        # test for unknown parameters
        for attr in attributes:
            if attr not in valid_attributes:
                raise_value_error(ERR_UNKNOWN_PARAMETER, attr, ",".join(valid_attributes))

        result[configuration.CONFIG_TASK_NAME] = attributes[configuration.CONFIG_TASK_NAME]
        action_name = self.validate_action(attributes[configuration.CONFIG_ACTION_NAME])
        result[configuration.CONFIG_ACTION_NAME] = action_name

        process_this_account = TaskConfiguration.as_boolean(attributes.get(configuration.CONFIG_THIS_ACCOUNT, True))
        result[configuration.CONFIG_THIS_ACCOUNT] = process_this_account
        account = self.this_account if process_this_account else None

        for attr in attributes:

            if attr in [configuration.CONFIG_TASK_NAME, configuration.CONFIG_ACTION_NAME, configuration.CONFIG_PARAMETERS]:
                continue

            try:
                # verify cross-account roles
                if attr == configuration.CONFIG_ACCOUNTS:
                    if len(attributes[attr]) > 0:
                        result[attr] = self.verify_accounts(account, attributes[attr], action_name,
                                                            attributes[configuration.CONFIG_TASK_NAME])
                    continue

                if attr == configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME:
                    result[attr] = TaskConfiguration.verify_task_role_name(attributes[attr], action_name)

                # verify boolean enabled, dryrun and debug parameters
                if attr in [
                    configuration.CONFIG_ENABLED,
                    configuration.CONFIG_DRYRUN,
                    configuration.CONFIG_DEBUG,
                    configuration.CONFIG_TASK_NOTIFICATIONS,
                    configuration.CONFIG_TASK_METRICS
                ]:
                    result[attr] = TaskConfiguration.as_boolean(attributes[attr])
                    continue

                # verify interval (cron) expression
                if attr == configuration.CONFIG_INTERVAL:
                    result[attr] = self.verify_interval(attributes[attr], attributes, action_name=action_name,
                                                        task_name=attributes[configuration.CONFIG_TASK_NAME])
                    continue

                # verify timeout for task
                if attr == configuration.CONFIG_TASK_TIMEOUT:
                    timeout = TaskConfiguration.verify_timeout(action_name, attributes[attr])
                    if timeout is not None:
                        result[attr] = timeout
                    continue

                # memory settings for task
                if attr in [
                    configuration.CONFIG_TASK_SELECT_SIZE,
                    configuration.CONFIG_TASK_EXECUTE_SIZE,
                    configuration.CONFIG_TASK_COMPLETION_SIZE
                ]:
                    result[attr] = TaskConfiguration.validate_lambda_size(attributes[attr])
                    continue

                # Ecs memory settings for task
                if attr in [
                    configuration.CONFIG_ECS_SELECT_MEMORY,
                    configuration.CONFIG_ECS_EXECUTE_MEMORY,
                    configuration.CONFIG_ECS_COMPLETION_MEMORY
                ]:
                    try:
                        if attributes[attr] is not None:
                            result[attr] = int(attributes[attr])
                    except ValueError:
                        raise_exception(ERR_INVALID_ECS_MEMORY_SIZE, attributes[attr], attr)
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

                # verify event scopes
                if attr == configuration.CONFIG_EVENT_SCOPES:
                    result[attr] = TaskConfiguration.validate_event_scopes(attributes[attr], action_name)
                    continue

                # verify regions
                if attr == configuration.CONFIG_REGIONS:
                    result[attr] = "*" if attributes[attr] in ["*", ["*"]] else self.validate_regions(attributes[attr],
                                                                                                      action_name)

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

        # default for metrics parameter
        if configuration.CONFIG_TASK_METRICS not in result:
            result[configuration.CONFIG_TASK_METRICS] = False

        # default for region
        if configuration.CONFIG_REGIONS not in result:
            regions = self.validate_regions(None, action_name)
            if len(regions) > 0:
                result[configuration.CONFIG_REGIONS] = self.validate_regions(None, action_name)

        # set internal flag if the task action is internal
        if configuration.CONFIG_INTERNAL not in result:
            if actions.get_action_properties(action_name).get(actions.ACTION_INTERNAL, False):
                result[configuration.CONFIG_INTERNAL] = True

        result[configuration.CONFIG_PARAMETERS] = self.verify_task_parameters(
            attributes.get(configuration.CONFIG_PARAMETERS, {}), task_settings=result, action_name=action_name)

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
        :param: include_internal: include internal tasks
        :return:
        """
        for config_item in self.config_items(include_internal=include_internal):
            try:
                yield self.configuration_item_to_task(config_item)
            except Exception as ex:
                if self._logger is not None:
                    self._logger.error(ERR_FETCHING_TASKS_FROM_CONFIG, ex)

    def get_task(self, name):
        """
        Gets a configured task by name
        :param:  name of the task
        :return: The task, or None of the task does not exist
        """
        item = self.get_config_item(name=name)
        if item is not None:
            return self.configuration_item_to_task(item)
        return None

    def _regions_for_tasks_with_events(self, task_name=None):
        regions = set()
        if task_name is None:
            tasks = self.get_tasks(include_internal=False)
        else:
            task = self.get_task(task_name)
            tasks = [task] if task is not None else []
        for task in tasks:
            task_events = task.get(handlers.TASK_EVENTS, {})
            if len(task_events) == 0:
                continue
            regions.update(task.get(handlers.TASK_REGIONS, []))
        return regions

    @staticmethod
    def _event_bus_permissions_sid_prefix():
        return "ops-automator-{}-{}-".format(os.getenv(handlers.ENV_STACK_NAME).lower(), services.get_session().region_name)

    def _update_ops_automator_topic_permissions(self):

        def get_accounts_with_events():

            accounts_with_events = set()
            for task in self.get_tasks(include_internal=False):
                task_events = task.get(handlers.TASK_EVENTS, {})
                if len(task_events) == 0:
                    continue

                task_accounts = task.get(handlers.TASK_ACCOUNTS, [])
                accounts_with_events.update(task_accounts)

            return accounts_with_events

        external_account_with_events = get_accounts_with_events()

        methods = ["add_permission",
                   "remove_permission",
                   "get_topic_attributes"]

        sns_client = boto_retry.get_client_with_retries("sns", methods=methods, context=self._context)
        topic_arn = os.getenv(handlers.ENV_EVENTS_TOPIC_ARN)
        # get policy document for topics
        statement = json.loads(sns_client.get_topic_attributes_with_retries(
            TopicArn=topic_arn).get("Attributes", {}).get("Policy", "{}")).get("Statement", [])

        # get all sid for all accounts that have permission where the sid starts with prefix used for this stack
        accounts_with_topic_permissions = {s["Principal"]["AWS"].split(":")[-2]: s["Sid"] for s in statement
                                           if len(s.get("Principal", {}).get("AWS", "").split(":")) == 6 and
                                           s["Sid"].startswith(TaskConfiguration._event_bus_permissions_sid_prefix())}

        # add permission for other accounts that have tasks don't have permission yet
        for account in external_account_with_events:
            if account not in accounts_with_topic_permissions:
                label = self._event_bus_permissions_sid_prefix() + account
                if self._logger is not None:
                    self._logger.info(INF_ADD_ACCOUNT_TOPIC_PERMISSION, account, label)
                sns_client.add_permission_with_retries(TopicArn=topic_arn,
                                                       AWSAccountId=[account],
                                                       ActionName=["Publish"],
                                                       Label=label)

        # remove permissions for accounts that don't have tasks that use events
        for account in accounts_with_topic_permissions:
            if account not in external_account_with_events:
                if self._logger is not None:
                    self._logger.info(INF_REMOVE_TOPIC_PERMISSION, account, accounts_with_topic_permissions[account])
                sns_client.remove_permission_with_retries(TopicArn=topic_arn, Label=accounts_with_topic_permissions[account])

    def remove_stack_event_topic_permissions(self):
        """
        Removes all permissions for accounts for putting events on the event bus of the Ops Automator account.
        Only permissions created by this stack are removed.
        :return:
        """

        topic_arn = os.getenv(handlers.ENV_EVENTS_TOPIC_ARN)
        sns_client = boto_retry.get_client_with_retries("sns",
                                                        methods=["remove_permission", "get_topic_attributes"],
                                                        context=self._context)

        statement = json.loads(sns_client.get_topic_attributes_with_retries(
            TopicArn=topic_arn).get("Attributes", {}).get("Policy", "{}")).get("Statement", [])

        permission_sids_for_stack = [s["Sid"] for s in statement
                                     if s["Sid"].startswith(TaskConfiguration._event_bus_permissions_sid_prefix())]

        for label in permission_sids_for_stack:
            sns_client.remove_permission_with_retries(Label=label, TopicArn=topic_arn)
