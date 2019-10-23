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
import json
import os
import re
import time
import types
from datetime import datetime

import boto3

import actions
import configuration.task_configuration
import handlers
import services
import testing
from handlers.select_resources_handler import SelectResourcesHandler
from helpers import safe_json
from helpers.timer import Timer
from testing.console_logger import ConsoleLogger
from testing.context import Context
from testing.datetime_provider import DatetimeProvider, set_datetime_delta
from testing.report_output_writer import create_output_writer
from testing.stack import Stack
from testing.task_tracker import TaskTracker


class TaskTestRunner(object):

    def __init__(self, action_name, tested_region=None, test_region=None, action_stack_name=None, logger=None,
                 task_list_tag_name=None, debug=False):

        self.debug = debug
        self.logger = logger if logger is not None else ConsoleLogger(debug=self.debug)
        if getattr(self.logger, "test") is None:
            setattr(self.logger, "test", getattr(self.logger, "info"))

        self.context = None

        self.action_name = action_name
        self.action_class = actions.get_action_class(self.action_name)
        self.action_properties = actions.get_action_properties(self.action_name)

        self.tested_region = tested_region if tested_region is not None else boto3.Session().region_name
        self.test_region = test_region if test_region is not None else boto3.Session().region_name

        if task_list_tag_name is not None:
            os.environ[handlers.ENV_AUTOMATOR_TAG_NAME] = task_list_tag_name

        self.interval = None

        os.environ[handlers.ENV_OPS_AUTOMATOR_ACCOUNT] = services.get_aws_account()

        if action_stack_name is None:
            self.action_stack_name = testing.action_stack_name(self.action_name)
        else:
            self.action_stack_name = action_stack_name

        self._action_stack = None

        os.environ[handlers.ENV_STACK_NAME] = self.action_stack_name

        actions.set_report_output_provider(create_output_writer)

        self._action_stack_resources = None

        self._assumed_role = None
        self._tested_account = None
        self._session = None

        self._cloudformation_client = None
        self._action_stack_template = None
        self._events = None
        self._tag_filter = None

        self.results = None
        self.parameters = None
        self.task_name = None
        self.action_select_parameters = None
        self.run_after_select = None

        self.max_concurrency = None
        self.concurrency_key = None
        self.log_subject = None
        self.run_in_regions = None

        self.run_after_select = None

        self.executed_tasks = []

    @property
    def action_stack_template(self):
        if self._action_stack_template is None:

            self._action_stack_template = copy.deepcopy(testing.TEMPLATE)
            self._action_stack_template["Resources"][testing.OPS_AUTOMATOR_ROLE_NAME]["Properties"][
                "RoleName"] = testing.assumed_test_role_name(self.action_name)
            if len(self.action_properties.get(actions.ACTION_STACK_RESOURCES, {})) > 0:
                self._add_action_stack_resources_to_action_stack_template()

            # noinspection PyTypeChecker,PyPep8
            self._action_stack_template["Resources"][testing.OPS_AUTOMATOR_ROLE_NAME]["Properties"]["AssumeRolePolicyDocument"
            ]["Statement"][0]["Principal"]["AWS"] = boto3.client("sts").get_caller_identity()["Arn"]
            self._add_actions_permissions_to_action_stack_template()

            self._action_stack_template[
                "Description"] = "Stack containing resources for test of Ops Automator action {}".format(self.action_name)

        return self._action_stack_template

    @property
    def tested_account(self):
        if self._tested_account is None:
            if self._assumed_role not in [None, ""]:
                self._tested_account = self._assumed_role = services.account_from_role_arn(self._assumed_role)
            else:
                self._tested_account = boto3.client("sts").get_caller_identity()['Account']
        return self._tested_account

    @property
    def action_stack(self):
        if self._action_stack is None:
            self._ensure_action_stack()
        return self._action_stack

    @property
    def action_stack_resources(self):
        if self._action_stack_resources is None:

            self._action_stack_resources = {}
            if self._action_stack is not None:
                # test if this action has additional stack resources
                resources = actions.get_action_properties(self.action_name).get(actions.ACTION_STACK_RESOURCES, {})
                if resources:
                    # name of the class
                    class_name = self.action_properties[actions.ACTION_CLASS_NAME][0:-len("Action")]
                    # actual resource names is name of class + name from class properties
                    logical_resource_names = [class_name + resource_name for resource_name in resources]

                    for res in list(self.action_stack.stack_resources.values()):
                        # actual name
                        logical_resource_id = res["LogicalResourceId"]
                        # test if this resource is an resource from the action properties
                        if logical_resource_id in logical_resource_names:
                            self._action_stack_resources[logical_resource_id[len(class_name):]] = {
                                i: res[i] for i in ["LogicalResourceId",
                                                    "PhysicalResourceId",
                                                    "ResourceType"]
                            }

                            if len(list(self._action_stack_resources.keys())) == len(resources):
                                return self._action_stack_resources

        return self._action_stack_resources

    def _build_config_item(self):

        item = {
            configuration.CONFIG_ACTION_NAME: self.action_name,
            configuration.CONFIG_PARAMETERS: self.parameters,
            configuration.CONFIG_ENABLED: True,
            configuration.CONFIG_DEBUG: self.debug,
            configuration.CONFIG_THIS_ACCOUNT: False,
            configuration.CONFIG_ACCOUNTS: [self.tested_account],
            configuration.CONFIG_TASK_CROSS_ACCOUNT_ROLE_NAME: testing.assumed_test_role_name(self.action_name),
            configuration.CONFIG_DRYRUN: False,
            configuration.CONFIG_TASK_NOTIFICATIONS: False,
            configuration.CONFIG_INTERNAL: self.action_properties.get(actions.ACTION_INTERNAL, False),
            configuration.CONFIG_TASK_NAME: self.task_name,
            configuration.CONFIG_TAG_FILTER: self._tag_filter,
            configuration.CONFIG_TIMEZONE: "UTC",
            configuration.CONFIG_REGIONS: self.run_in_regions,
            configuration.CONFIG_TASK_METRICS: False,
            configuration.CONFIG_EVENTS: self._events
        }

        if getattr(self.action_class, handlers.COMPLETION_METHOD, None) is not None:
            timeout_value = self.parameters.get(actions.ACTION_PARAM_TIMEOUT)
            if timeout_value is None:
                timeout_value = self.action_properties.get(actions.ACTION_COMPLETION_TIMEOUT_MINUTES,
                                                           actions.DEFAULT_COMPLETION_TIMEOUT_MINUTES_DEFAULT)
            item[configuration.CONFIG_TASK_TIMEOUT] = timeout_value

        use_intervals = actions.ACTION_TRIGGER_INTERVAL[0] in self.action_properties.get(actions.ACTION_TRIGGERS,
                                                                                         actions.ACTION_TRIGGER_BOTH)
        if use_intervals:
            item[configuration.CONFIG_INTERVAL] = self.interval if self.interval is not None else "0 0 * * ?"

        return item

    def _build_tasks_for_selected_resources(self):
        try:

            configured_task = configuration.task_configuration.TaskConfiguration(self.context, self.logger) \
                .configuration_item_to_task(self._build_config_item())

            select_event = {
                handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                handlers.HANDLER_EVENT_CUSTOM_SELECT: True,
                handlers.HANDLER_EVENT_SOURCE: "{}-test".format(configured_task[handlers.TASK_ACTION]).lower(),
                handlers.HANDLER_EVENT_TASK: configured_task,
                handlers.HANDLER_EVENT_TASK_DT: datetime.now().isoformat(),
                handlers.HANDLER_SELECT_ARGUMENTS: self.action_select_parameters,
                handlers.HANDLER_EVENT_TASK_GROUP: self.task_name
            }

            self.task_tracking = TaskTracker()
            select_handler = SelectResourcesHandler(select_event, self.context, self.logger, self.task_tracking)
            select_handler.handle_request()
            if self.run_after_select is not None:
                self.run_after_select(self.task_tracking)
            return self.task_tracking.task_items
        except Exception as ex:
            self.logger.test("Error building tasks for resources, {}", ex)
            raise ex

    def _get_tasks_to_execute(self):

        task_items = self._build_tasks_for_selected_resources()

        for item in task_items:
            event = {i: item.get(i) for i in item}
            event[handlers.HANDLER_EVENT_ACTION] = handlers.HANDLER_ACTION_EXECUTE
            event[actions.ACTION_SERVICE] = actions.get_action_properties(item[handlers.TASK_TR_ACTION]).get(actions.ACTION_SERVICE)
            action_argument = {
                actions.ACTION_PARAM_CONTEXT: self.context,
                actions.ACTION_PARAM_EVENT: event,
                actions.ACTION_PARAM_SESSION: services.get_session(role_arn=item.get(handlers.TASK_TR_ASSUMED_ROLE, None)),
                actions.ACTION_PARAM_RESOURCES: handlers.get_item_resource_data(item, context=self.context),
                actions.ACTION_PARAM_DEBUG: item[handlers.TASK_TR_DEBUG],
                actions.ACTION_PARAM_DRYRUN: item[handlers.TASK_TR_DRYRUN],
                actions.ACTION_PARAM_TASK_ID: item[handlers.TASK_TR_ID],
                actions.ACTION_PARAM_TASK: item[handlers.TASK_TR_NAME],
                actions.ACTION_PARAM_TASK_TIMEZONE: item[handlers.TASK_TR_TIMEZONE],
                actions.ACTION_PARAM_STACK: self.action_stack.stack_name if self.action_stack is not None else None,
                actions.ACTION_PARAM_STACK_ID: self.action_stack.stack_id if self.action_stack is not None else None,
                actions.ACTION_PARAM_STACK_RESOURCES: self.action_stack_resources,
                actions.ACTION_PARAM_ASSUMED_ROLE: item.get(handlers.TASK_TR_ASSUMED_ROLE),
                actions.ACTION_PARAM_STARTED_AT: item[handlers.TASK_TR_STARTED_TS],
                actions.ACTION_PARAM_TAGFILTER: item[handlers.TASK_TR_TAGFILTER],
                actions.ACTION_PARAM_TIMEOUT: item[handlers.TASK_TR_TIMEOUT],
                actions.ACTION_PARAM_LOGGER: self.logger,
                actions.ACTION_PARAM_EVENTS: self._events,
                actions.ACTION_PARAM_HAS_COMPLETION: item[handlers.TASK_TR_HAS_COMPLETION],
                actions.ACTION_PARAM_INTERVAL: item[handlers.TASK_INTERVAL]
            }

            if self._assumed_role is None:
                self._assumed_role = action_argument[actions.ACTION_PARAM_ASSUMED_ROLE]

            action_instance = self.action_class(action_argument, item.get(handlers.TASK_TR_PARAMETERS, {}))

            self.verify_log_subject(action_argument)
            self.verify_concurrency(action_argument, item)

            yield action_instance

    def verify_concurrency(self, action_argument, item):
        concurrency_key_method = getattr(self.action_class, handlers.ACTION_CONCURRENCY_KEY_METHOD, None)
        # prepare parameters for calling static function that returns the concurrency key
        if concurrency_key_method is not None:
            get_key_params = {
                actions.ACTION_PARAM_RESOURCES: action_argument[actions.ACTION_PARAM_RESOURCES],
                actions.ACTION_PARAM_ACCOUNT: item[handlers.TASK_TR_ACCOUNT],
                actions.ACTION_PARAM_STACK: action_argument[actions.ACTION_PARAM_STACK],
                actions.ACTION_PARAM_STACK_ID: action_argument[actions.ACTION_PARAM_STACK_ID],
                actions.ACTION_PARAM_TASK_ID: action_argument[actions.ACTION_PARAM_STACK_ID],
                actions.ACTION_PARAM_TASK: item[handlers.TASK_TR_NAME]

            }
            get_key_params.update(item.get(handlers.TASK_TR_PARAMETERS))
            key = concurrency_key_method(get_key_params)
            assert (key is not None)
            self.logger.test("Action concurrency key is ", key)
            self.concurrency_key = key

        # test if there are concurrency restrictions
        max_action_concurrency = self.action_properties.get(actions.ACTION_MAX_CONCURRENCY)
        # has maximum method
        if max_action_concurrency is not None and types.FunctionType == type(max_action_concurrency):
            # noinspection PyBroadException,PyPep8
            try:
                parameters = item[handlers.TASK_TR_PARAMETERS]
                max_action_concurrency = max_action_concurrency(parameters)
            except:
                max_action_concurrency = None
            assert (max_action_concurrency is not None)
            self.logger.test("Concurrency method returns {}", max_action_concurrency)
        self.max_concurrency = max_action_concurrency

    def verify_log_subject(self, action_argument):
        if callable(getattr(self.action_class, "action_logging_subject", None)):
            # noinspection PyBroadException,PyPep8
            try:
                action_log_subject = self.action_class.action_logging_subject(action_argument, self.parameters)
            except:
                action_log_subject = None
            assert (action_log_subject is not None)
            self.logger.test("Action log subject is {}", action_log_subject)
            self.log_subject = action_log_subject

    def _add_action_stack_resources_to_action_stack_template(self):

        def fix_resource_references(resources, old, new):

            def update_list(l, old_name, new_name):
                for item in l:
                    if isinstance(item, dict):
                        fix_resource_references(item, old_name, new_name)
                    elif isinstance(item, list):
                        update_list(item, old_name, new_name)

            for key in resources:
                val = resources[key]
                if key == "Ref" and val == old:
                    resources[key] = new
                if isinstance(val, dict):
                    fix_resource_references(val, old, new)

                elif isinstance(val, list):
                    update_list(val, old, new)

        template_resources = self.action_stack_template["Resources"]

        action_statement = template_resources[testing.OPS_AUTOMATOR_ROLE_NAME]["Properties"]["Policies"][0]["PolicyDocument"][
            "Statement"]

        stack_resources = self.action_properties.get(actions.ACTION_STACK_RESOURCES)
        stack_resource_permissions = self.action_properties.get(actions.ACTION_STACK_RESOURCES_PERMISSIONS, {})

        if stack_resources:

            resource_names = []

            action_resources_to_add = {}
            # get additional resources and build new dict with prefixed names
            for resource_name in stack_resources:
                prefixed_resource_name = self.action_name + resource_name
                resource_names.append((resource_name, prefixed_resource_name))
                action_resources_to_add[prefixed_resource_name] = stack_resources[resource_name]

            # fix names of prefixed resource names in references
            for resource_name in resource_names:
                # references in stack resources
                fix_resource_references(action_resources_to_add, resource_name[0], resource_name[1])
                # references in list of permissions for stack resources
                for i in stack_resource_permissions["Resource"]:
                    if isinstance(i, dict):
                        fix_resource_references(i, resource_name[0], resource_name[1])

            # add the resources for this action to the template
            for resource_name in action_resources_to_add:
                template_resources[resource_name] = action_resources_to_add[resource_name]

            if len(stack_resource_permissions) != 0:
                # statements = build_action_policy_statement(action_name, stack_resource_permissions)
                stack_resource_permissions["Sid"] = re.sub("[^0-9A-Za-z]", "", self.action_name + "Resources")
                action_statement.append(stack_resource_permissions)

    def _add_actions_permissions_to_action_stack_template(self):

        def build_action_policy_statement(action_name, added_action_permissions):
            statements = []

            if len(added_action_permissions) > 0:
                statements.append({
                    "Sid": re.sub("[^0-9A-Za-z]", "", action_name),
                    "Effect": "Allow",
                    "Resource": "*",
                    "Action": sorted(list(set(added_action_permissions)))
                })
            return statements

        def action_select_resources_permissions(action_prop):
            return services.get_resource_describe_permissions(action_prop[actions.ACTION_SERVICE],
                                                              [action_prop[actions.ACTION_RESOURCES]])

        # noinspection PyTypeChecker
        ops_automator_role = self.action_stack_template["Resources"]["OpsAutomatorRole"]
        action_statement = ops_automator_role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]

        required_actions = set()

        action_permissions = self.action_properties.get(actions.ACTION_PERMISSIONS, [])
        action_permissions += list(action_select_resources_permissions(self.action_properties))

        if len(action_permissions) != 0:
            required_actions.update(action_permissions)

        action_statement += build_action_policy_statement("ActionPermissions", required_actions)

    def _ensure_action_stack(self):

        if self.action_stack_template == {}:
            return

        if self._action_stack is None:
            self._action_stack = Stack(self.action_stack_name, region=self.test_region, owned=False)
            if not self._action_stack.is_stack_present():
                self._action_stack = Stack(self.action_stack_name, region=self.test_region, owned=True)
                self.logger.test("Creating action resources stack {} in region {}", self.action_stack_name, self.test_region)
                if len(self.action_stack_template) > 0:
                    self._action_stack.create_stack(json.dumps(self.action_stack_template), iam_capability=True)
            else:
                self.logger.test("Using existing action stack {}", self.action_stack_name)

    def run(self, parameters,
            complete_check_polling_interval=60,
            task_timeout=None,
            task_name=None,
            datetime_delta=None,
            events=None,
            tag_filter=None,
            run_in_regions=None,
            action_select_params=None,
            debug=False,
            run_after_select=None):

        self.results = []
        self.executed_tasks = []
        self.parameters = parameters
        self.action_select_parameters = action_select_params if action_select_params is not None else {}
        self.task_name = task_name if task_name is not None else "{}-test".format(self.action_name).lower()
        self._ensure_action_stack()

        self.run_after_select = run_after_select

        self.context = Context()

        self._events = events if events is not None else {}
        self._tag_filter = tag_filter

        save_debug = self.debug
        self.debug = debug
        self.logger._debug = self.debug

        self.interval = parameters.get(actions.ACTION_PARAM_INTERVAL, None)

        self.run_in_regions = run_in_regions if run_in_regions is not None else [self.test_region]

        try:
            if datetime_delta is not None:
                set_datetime_delta(datetime_delta)
                actions.set_date_time_provider(DatetimeProvider)
                self.logger.test("Setting simulated test execution date and time to {}", actions.date_time_provider().now())

            for executed_task in self._get_tasks_to_execute():
                try:
                    self.executed_tasks.append(executed_task)
                    self.logger.test("Start execution of action {} using assumed role {} in region {}", self.action_name,
                                     self._assumed_role, self.tested_region)
                    start_result = executed_task.execute()
                    if not executed_task.get(actions.ACTION_PARAM_HAS_COMPLETION, False):
                        self.logger.test("Action completed with result {}", safe_json(start_result, indent=3))
                        setattr(executed_task, handlers.TASK_TR_RESULT, start_result)
                        setattr(executed_task, handlers.TASK_TR_STATUS, handlers.STATUS_COMPLETED)
                    else:
                        self.logger.test("Waiting for task to complete")
                        setattr(executed_task, handlers.TASK_TR_START_RESULT, start_result)

                        # noinspection PyProtectedMember
                        timeout = executed_task._timeout_
                        if timeout is None:
                            timeout = task_timeout * 60 if task_timeout is not None else 60
                        timeout *= 60
                        with Timer(timeout_seconds=timeout) as timer:
                            while True:
                                is_completed = getattr(executed_task, handlers.COMPLETION_METHOD, None)
                                if is_completed is None:
                                    raise Exception("Tested action needs completion but does not implement the required {} method",
                                                    handlers.COMPLETION_METHOD)
                                complete_result = executed_task.is_completed(start_result)
                                if complete_result is not None:
                                    self.logger.test("Action completed with result {}", safe_json(complete_result, indent=3))
                                    setattr(executed_task, handlers.TASK_TR_STATUS, handlers.STATUS_COMPLETED)
                                    setattr(executed_task, handlers.TASK_TR_RESULT, complete_result)
                                    break
                                if timer.timeout:
                                    self.logger.test("Action timed out")
                                    setattr(executed_task, handlers.TASK_TR_STATUS, handlers.STATUS_TIMED_OUT)
                                    setattr(executed_task, handlers.TASK_TR_ERROR, "Timeout")
                                    break
                                self.logger.test("Action not completed yet, waiting {} seconds", complete_check_polling_interval)
                                time.sleep(complete_check_polling_interval)
                    self.results.append(executed_task)

                except Exception as ex:
                    self.logger.test("Action failed {}", str(ex))
                    setattr(executed_task, handlers.TASK_TR_STATUS, handlers.STATUS_FAILED)
                    setattr(executed_task, handlers.TASK_TR_ERROR, str(ex))
                    self.results.append(executed_task)

        finally:
            if datetime_delta is not None:
                actions.reset_date_provider()
            self.debug = save_debug
            self.logger._debug = self.debug

        return self.results

    def create_stack(self):
        self._ensure_action_stack()

    def cleanup(self, keep_action_stack=False):
        # noinspection PyBroadException,PyPep8
        try:
            if self._action_stack is not None:
                if self.action_stack.owned and (not keep_action_stack):
                    self.logger.test("Deleting stack {}", self.action_stack_name)
                    self.action_stack.delete_stack(empty_bucket_resources=True)
        except:
            pass

    def success(self, expected_executed_tasks=None, results=None):
        checked_results = results if results is not None else self.results
        if expected_executed_tasks is not None and expected_executed_tasks != len(checked_results):
            return False
        return all([getattr(a, handlers.TASK_TR_STATUS) == handlers.STATUS_COMPLETED for a in self.results])
