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


import json
import os
from copy import copy
from datetime import datetime
from hashlib import sha256

import boto3

import actions
import builders
import configuration
import handlers
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from builders import build_events_forward_template
from builders.action_template_builder import ActionTemplateBuilder
from builders.cross_account_role_builder import CrossAccountRoleBuilder
from configuration.task_configuration import TaskConfiguration
from handlers.custom_resource import CustomResource
from helpers import full_stack, safe_dict, safe_json
from metrics.anonymous_metrics import allow_send_metrics, send_metrics_data
from outputs.queued_logger import QueuedLogger

ERR_HANDLING_SETUP_REQUEST = "{} {}"
ERR_BUILDING_TEMPLATES = "Error building templates ({})\n{}"
ERR_DELETE_CONFIG_ITEM = "Unable to delete configuration object {} from bucket {}, ({})"

ERR_DELETING_STACK = "Error deleting  {}, ({})"

INF_CREATE_ACTION_TEMPLATE = "Creating template for action \"{}\", name is \"{}\""
INF_CREATE_ALL_ACTIONS_CROSS_ROLES_TEMPLATE = "Creating cross role template for all actions, name is \"{}\""
INF_CREATE_EVENT_FORWARD_TEMPLATE = "Creating events forward template \"{}\""
INF_DELETE_ALL_ACTIONS_TEMPLATE = "Configuration objects"
INF_DELETE_LOG_RETENTION_POLICY = "Deleting log retention policy for Lambda CloudWatch loggroup {}"
INF_DELETED_STACKS = "Stacks to delete: {}"
INF_DELETING_STACKS = "Deleting external task configuration stacks"
INF_GENERATING_TEMPLATES = "Generating templates in bucket {}"
INF_NO_STACKS = "No stacks to delete"
INF_SET_LOG_RETENTION_POLICY = "Setting log retention policy for Lambda CloudWatch loggroup {} to {} days"
INF_STACK = "Deleting stack"
INF_DELETING_ACTION_TEMPLATE = "Deleting templates for action \"{}\""
INF_SCENARIO_TEMPLATE = "Creating scenario template template {} in bucket {}"

TEMPLATE_DESC_ALL_ACTIONS = "Cross account role for all available automation actions for Ops Automator stack \"{}\" in account {}"
TEMPLATE_DESC_ALL_ACTIONS_PARAMETERS = "Cross account role for selected automation actions for Ops Automator " \
                                       "stack \"{}\" in account {}"

S3_KEY_TASK_CONFIG = "TaskConfiguration/"
S3_KEY_ACTION_CONFIGURATION_TEMPLATE = S3_KEY_TASK_CONFIG + "{}.template"
S3_KEY_ACTIONS_HTML_PAGE = S3_KEY_TASK_CONFIG + "ActionsConfiguration.html"
S3_KEY_ACCOUNT_CONFIG = "AccountsConfiguration/"
S3_KEY_ACCOUNT_CONFIG_WITH_PARAMS = S3_KEY_ACCOUNT_CONFIG + "AccountRoleConfiguration.template"
S3_KEY_ACCOUNT_CONFIG_CREATE_ALL = S3_KEY_ACCOUNT_CONFIG + "AccountRoleCreateAllActions.template"

FORWARD_EVENTS_TEMPLATE = "AccountForwardEvents.template"
S3_KEY_ACCOUNT_EVENTS_FORWARD_TEMPLATE = S3_KEY_ACCOUNT_CONFIG + FORWARD_EVENTS_TEMPLATE

S3_KEY_SCENARIO_TEMPLATE_BUCKET = S3_KEY_TASK_CONFIG + "ScenarioTemplates"
S3_KEY_SCENARIO_TEMPLATE_KEY = S3_KEY_SCENARIO_TEMPLATE_BUCKET + "/{}"

S3_KEY_CUSTOM_RESOURCE_BUILDER = S3_KEY_TASK_CONFIG + "Scripts/BuildTaskCustomResource.py"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class SetupHelperHandler(CustomResource):

    def __init__(self, event, context):
        """
        Initializes helper setup class
        :param event: 
        :param context: 
        """
        CustomResource.__init__(self, event, context)

        self.arguments = copy(self.resource_properties)
        self.arguments = {a: self.resource_properties[a] for a in self.resource_properties if a not in ["ServiceToken",
                                                                                                        "Timeout"]}

        self.configuration_bucket = os.getenv(configuration.ENV_CONFIG_BUCKET, None)
        self.automator_role_arn = self.arguments.get("OpsAutomatorLambdaRole")
        self.events_forward_role = self.arguments.get("EventForwardLambdaRole")
        self.ops_automator_topic_arn = self.arguments.get("OpsAutomatorTopicArn")
        self.use_ecs = TaskConfiguration.as_boolean(self.arguments.get("UseEcs", False))
        self.optimize_cross_account_template = TaskConfiguration.as_boolean(
            (self.arguments.get("OptimizeCrossAccountTemplate", False)))

        self.account = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)

        self.stack_version = self.arguments["StackVersion"]

        # setup logging
        dt = datetime.utcnow()
        classname = self.__class__.__name__
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = QueuedLogger(logstream=logstream, context=context, buffersize=50)

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Test if the event is handled by this handler
        :param _:
        :param event: Event to test
        :return: True if the event is an event from cloudformationOpsAutomatorSetupHelper custom resource
        """
        return event.get("StackId") is not None and event.get("ResourceType") == "Custom::OpsAutomatorSetupHelper"

    def handle_request(self):
        """
        Handles the custom resource request from cloudformation
        :return: 
        """

        start = datetime.now()

        self._logger.info("Cloudformation request is {}", safe_json(self._event, indent=2))

        try:
            result = CustomResource.handle_request(self)

            return safe_dict({
                "result": result,
                "datetime": datetime.now().isoformat(),
                "running-time": (datetime.now() - start).total_seconds()
            })
        except Exception as ex:
            self._logger.error(ERR_HANDLING_SETUP_REQUEST, ex, full_stack())
            raise ex

        finally:
            self._logger.flush()

    def _set_lambda_logs_retention_period(self):
        """
        Aligns retention period for default Lambda log streams with settings
        :return: 
        """

        if self._context is None:
            return

        log_client = get_client_with_retries("logs",
                                             methods=[
                                                 "delete_retention_policy",
                                                 "put_retention_policy",
                                                 "create_log_group",
                                                 "describe_log_groups"
                                             ],
                                             context=self.context)

        retention_days = self.arguments.get("LogRetentionDays")

        base_name = self._context.log_group_name[0:-len("Standard")]

        log_groups = [
            base_name + size for size in [
                "Standard",
                "Medium",
                "Large",
                "XLarge",
                "XXLarge",
                "XXXLarge"
            ]
        ]

        existing_groups = [l["logGroupName"] for l in
                           log_client.describe_log_groups_with_retries(logGroupNamePrefix=base_name).get("logGroups", [])]

        for group in log_groups:
            exists = group in existing_groups
            self._logger.info("Setting retention for log group {}", group)
            if retention_days is None:
                if not exists:
                    continue
                self._logger.info(INF_DELETE_LOG_RETENTION_POLICY, group)
                log_client.delete_retention_policy_with_retries(logGroupName=group)
            else:
                if not exists:
                    log_client.create_log_group(logGroupName=group)
                self._logger.info(INF_SET_LOG_RETENTION_POLICY, group, retention_days)
                log_client.put_retention_policy_with_retries(logGroupName=group, retentionInDays=int(retention_days))

    def _setup(self):
        """
        OpsAutomatorSetupHelper setup actions
        :return: 
        """
        self._set_lambda_logs_retention_period()
        if self.configuration_bucket:
            self.generate_templates()

    def _send_create_metrics(self):

        metrics_data = {
            "Type": "stack",
            "Version": self.stack_version,
            "StackHash": sha256(self.stack_id).hexdigest(),
            "Data": {
                "Status": "stack_create",
                "Region": self.region
            }
        }

        send_metrics_data(metrics_data=metrics_data, logger=self._logger)

    def _send_delete_metrics(self):

        metrics_data = {
            "Type": "stack",
            "Version": self.stack_version,
            "StackHash": sha256(self.stack_id).hexdigest(),
            "Data": {
                "Status": "stack_delete",
                "Region": self.region
            }
        }

        send_metrics_data(metrics_data=metrics_data, logger=self._logger)

    def _create_request(self):
        """
        Handles create request from cloudformation custom resource
        :return: 
        """

        try:
            self._setup()
            self.physical_resource_id = self.__class__.__name__.lower()
            if allow_send_metrics():
                self._send_create_metrics()
            return True

        except Exception as ex:
            self.response["Reason"] = str(ex)
            return False

    def _update_request(self):
        """
        Handles update request from cloudformation custom resource
        :return: 
        """

        try:
            self._setup()
            return True

        except Exception as ex:
            self.response["Reason"] = str(ex)
            return False

    def _delete_request(self):
        """
        Handles delete request from cloudformation custom resource
        :return: 
        """

        try:
            self.delete_templates()
            self.delete_external_task_config_stacks()
            if allow_send_metrics():
                self._send_delete_metrics()
            return True

        except Exception as ex:
            self.response["Reason"] = str(ex)
            return False

    def delete_external_task_config_stacks(self):
        """
        Deletes external stacks that were used to create configuration items
        :return: 
        """

        self._logger.info(INF_DELETING_STACKS)

        stacks = TaskConfiguration(context=self.context, logger=self._logger).get_external_task_configuration_stacks()

        if len(stacks) == 0:
            self._logger.info(INF_NO_STACKS)
            return

        self._logger.info(INF_DELETED_STACKS, ", ".join(stacks))

        cfn = boto3.resource("cloudformation")

        for s in stacks:
            self._logger.info(INF_STACK)
            try:
                stack = cfn.Stack(s)
                add_retry_methods_to_resource(stack, ["delete"], context=self.context)
                stack.delete_with_retries()
            except Exception as ex:
                self._logger.error(ERR_DELETING_STACK, s, str(ex))

    def generate_templates(self):
        """
        Generates configuration and cross-account role templates
        :return: 
        """

        def generate_configuration_template(s3, builder, action):
            configuration_template = S3_KEY_ACTION_CONFIGURATION_TEMPLATE.format(action)
            self._logger.info(INF_CREATE_ACTION_TEMPLATE, action, configuration_template)
            template = json.dumps(builder.build_template(action), indent=3)
            s3.put_object_with_retries(Body=template, Bucket=self.configuration_bucket, Key=configuration_template)

        def generate_all_actions_cross_account_role_template_parameterized(s3, builder, all_act, template_description):
            self._logger.info(INF_CREATE_ALL_ACTIONS_CROSS_ROLES_TEMPLATE, S3_KEY_ACCOUNT_CONFIG_WITH_PARAMS)

            template = builder.build_template(action_list=all_act, description=template_description, with_conditional_params=True)
            if self.optimize_cross_account_template:
                template = CrossAccountRoleBuilder.compress_template(template)
            template_json = json.dumps(template, indent=3)
            s3.put_object_with_retries(Body=template_json, Bucket=self.configuration_bucket, Key=S3_KEY_ACCOUNT_CONFIG_WITH_PARAMS)

        # noinspection PyUnusedLocal
        def generate_all_actions_cross_account_role_template(s3, builder, all_act, template_description):
            self._logger.info(INF_CREATE_ALL_ACTIONS_CROSS_ROLES_TEMPLATE, S3_KEY_ACCOUNT_CONFIG_CREATE_ALL)
            template = json.dumps(
                builder.build_template(action_list=all_act, description=template_description, with_conditional_params=False),
                indent=3)
            s3.put_object_with_retries(Body=template, Bucket=self.configuration_bucket, Key=S3_KEY_ACCOUNT_CONFIG_CREATE_ALL)

        def generate_forward_events_template(s3):
            self._logger.info(INF_CREATE_EVENT_FORWARD_TEMPLATE, S3_KEY_ACCOUNT_EVENTS_FORWARD_TEMPLATE)
            template = build_events_forward_template(template_filename="./cloudformation/{}".format(FORWARD_EVENTS_TEMPLATE),
                                                     script_filename="./forward-events.py",
                                                     stack=self.stack_name,
                                                     event_role_arn=self.events_forward_role,
                                                     ops_automator_topic_arn=self.ops_automator_topic_arn,
                                                     version=self.stack_version)

            s3.put_object_with_retries(Body=template, Bucket=self.configuration_bucket, Key=S3_KEY_ACCOUNT_EVENTS_FORWARD_TEMPLATE)

        def generate_scenario_templates(s3):
            self._logger.info("Creating task scenarios templates")

            for template_name, template in list(builders.build_scenario_templates(templates_dir="./cloudformation/scenarios",
                                                                                  stack=self.stack_name)):
                self._logger.info(INF_SCENARIO_TEMPLATE, template_name, S3_KEY_SCENARIO_TEMPLATE_BUCKET)
                s3.put_object_with_retries(Body=template,
                                           Bucket=self.configuration_bucket,
                                           Key=S3_KEY_SCENARIO_TEMPLATE_KEY.format(template_name))

        def generate_custom_resource_builder(s3):
            self._logger.info("Create custom resource builder script {}", S3_KEY_CUSTOM_RESOURCE_BUILDER)

            with open("./build_task_custom_resource.py", "rt") as f:
                script_text = "".join(f.readlines())
                script_text = script_text.replace("%stack%", self.stack_name)
                script_text = script_text.replace("%account%", self.account)
                script_text = script_text.replace("%region%", self.region)
                script_text = script_text.replace("%config_table%", os.getenv("CONFIG_TABLE"))

            s3.put_object_with_retries(Body=script_text, Bucket=self.configuration_bucket, Key=S3_KEY_CUSTOM_RESOURCE_BUILDER)

        def generate_actions_html_page(s3):
            self._logger.info("Generating Actions HTML page {}", S3_KEY_ACTIONS_HTML_PAGE)
            html = builders.generate_html_actions_page(html_file="./builders/actions.html", region=self.region)
            s3.put_object_with_retries(Body=html, Bucket=self.configuration_bucket, Key=S3_KEY_ACTIONS_HTML_PAGE,
                                       ContentType="text/html")

        self._logger.info(INF_GENERATING_TEMPLATES, self.configuration_bucket)
        try:
            stack = os.getenv(handlers.ENV_STACK_NAME, "")
            s3_client = get_client_with_retries("s3", ["put_object"], context=self.context)
            config_template_builder = ActionTemplateBuilder(self.context,
                                                            service_token_arn="arn:aws:region:account:function:used-for-debug-only",
                                                            ops_automator_role=self.automator_role_arn,
                                                            use_ecs=self.use_ecs)
            role_template_builder = CrossAccountRoleBuilder(self.automator_role_arn, stack)

            all_actions = []
            for action_name in actions.all_actions():
                action_properties = actions.get_action_properties(action_name)
                if not action_properties.get(actions.ACTION_INTERNAL, False):
                    generate_configuration_template(s3_client, config_template_builder, action_name)
                    # Enable to generate a template for every individual action
                    # description = TEMPLATE_DESC_CROSS_ACCOUNT_ACTION.format(action_name, stack, account)
                    # generate_action_cross_account_role_template(s3_client, role_template_builder, action_name, description)
                    all_actions.append(action_name)

            if len(all_actions) > 0:
                description = TEMPLATE_DESC_ALL_ACTIONS_PARAMETERS.format(stack, self.account)
                generate_all_actions_cross_account_role_template_parameterized(s3_client, role_template_builder, all_actions,
                                                                               description)
            # enable to generate a template with all actions enabled
            #     description = TEMPLATE_DESC_ALL_ACTIONS.format(stack, account)
            #     generate_all_actions_cross_account_role_template(s3_client, role_template_builder, all_actions, description)

            for action_name in actions.all_actions():
                action_properties = actions.get_action_properties(action_name)
                if action_properties.get(actions.ACTION_EVENTS, None) is not None:
                    generate_forward_events_template(s3_client)
                    break

            generate_actions_html_page(s3_client)

            generate_scenario_templates(s3_client)

            generate_custom_resource_builder(s3_client)

        except Exception as ex:
            self._logger.error(ERR_BUILDING_TEMPLATES, str(ex), full_stack())

    def delete_templates(self):

        s3_client = get_client_with_retries("s3", ["delete_object"], context=self.context)
        s3_key = ""
        try:
            for action_name in actions.all_actions():
                action_properties = actions.get_action_properties(action_name)
                if not action_properties.get(actions.ACTION_INTERNAL, False):
                    self._logger.info(INF_DELETING_ACTION_TEMPLATE, action_name)
                    s3_key = S3_KEY_ACTION_CONFIGURATION_TEMPLATE.format(action_name)
                    s3_client.delete_object_with_retries(Bucket=self.configuration_bucket, Key=s3_key)
        except Exception as ex:
            self._logger.error(ERR_DELETE_CONFIG_ITEM, s3_key, self.configuration_bucket, str(ex))

            self._logger.info(INF_DELETE_ALL_ACTIONS_TEMPLATE)
            for key in [S3_KEY_ACTIONS_HTML_PAGE,
                        S3_KEY_ACCOUNT_CONFIG_WITH_PARAMS,
                        S3_KEY_ACCOUNT_CONFIG_CREATE_ALL,
                        S3_KEY_ACCOUNT_EVENTS_FORWARD_TEMPLATE]:
                try:
                    s3_client.delete_object_with_retries(Bucket=self.configuration_bucket, Key=key)
                except Exception as ex:
                    self._logger.error(ERR_DELETE_CONFIG_ITEM, key, self.configuration_bucket, str(ex))
