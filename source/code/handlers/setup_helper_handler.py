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
import traceback
from copy import copy
from datetime import datetime
from hashlib import sha256

import boto3

import actions
import configuration
import handlers
from boto_retry import add_retry_methods_to_resource, get_client_with_retries
from configuration.task_configuration import TaskConfiguration
from services.aws_service import AwsService
from util import safe_dict, safe_json
from util.action_template_builder import ActionTemplateBuilder
from util.cross_account_role_builder import CrossAccountRoleBuilder
from util.custom_resource import CustomResource
from util.logger import Logger
from util.metrics import send_metrics_data, allow_send_metrics

ALL_ACTIONS_TEMPLATE_NAME = "AllAutomationActions"

ERR_BUILDING_TEMPLATES = "Error building templates ({})\n{}"
ERR_DELETE_TEMPLATE_ = "Unable to delete template {} ({})"
ERR_DELETING_ACTION_TEMPLATE = "Deleting templates for action \"{}\""
ERR_DELETING_STACK = "Error deleting  {}, ({})"

INF_CREATE_ACTION_TEMPLATE = "Creating template for action \"{}\", name is \"{}\""
INF_CREATE_ALL_ACTIONS_CROSS_ROLES_TEMPLAE = "Creating cross role template for all actions, name is \"{}\""
INF_CREATECROSS_ROLE_TEMPLATE = "Creating cross role template for action \"{}\", name is \"{}\""
INF_DELETE_ALL_ACTIONS_TEMPLATE = "Deleting cross account role template \"{}\""
INF_DELETED_STACKS = "Stacks to delete: {}"
INF_DELETING_STACKS = "Deleting external task configuration stacks"
INF_GENERATING_TEMPLATES = "Generating templates in bucket {}"
INF_NO_STACKS = "No stacks to delete"
INF_STACK = "Deleting stack"
INF_SET_LOG_RETENTION_POLICY = "Setting log retention policy for Lambda CloudWatch loggroup {} to {} days"
INF_DELETE_LOG_RETENTION_POLICY = "Deleting log retention policy for Lambda CloudWatch loggroup {}"

WARN_CREATE_TASK_ROLES_FOLDER = "Can not create folder {} in bucket {}, (){}"

TEMPLATE_DESC_ALL_ACTIONS = "Cross account role for all available automation actions for Automation stack \"{}\" in account {}"
TEMPLATE_DESC_CROSS_ACCOUNT_ACTION = "Cross account role for automation action \"{}\" for Automation stack \"{}\" in account {}"

S3_CONFIGURATION_TEMPLATE = "Configuration/{}.template"
S3_ROLES_TEMPLATE = "Roles/{}.template"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class SetupHelperHandler(CustomResource):
    """
    Handling events from SchedulerSetup helper custom resource
    """

    def __init__(self, event, context):
        """
        Initializes helper setup class
        :param event: 
        :param context: 
        """
        CustomResource.__init__(self, event, context)

        # get "clean" set of arguments
        self.arguments = copy(self.resource_properties)
        self.arguments = {a: self.resource_properties[a] for a in self.resource_properties if a not in ["ServiceToken",
                                                                                                        "Timeout"]}

        self.configuration_bucket = os.getenv(configuration.ENV_CONFIG_BUCKET, None)
        self.scheduler_role_arn = self.arguments.get("SchedulerRole")

        # setup logging
        dt = datetime.utcnow()
        classname = self.__class__.__name__
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = Logger(logstream=logstream, context=context, buffersize=10)

    @staticmethod
    def is_handling_request(event):
        """
        Test if the event is handled by this handler
        :param event: Event to test
        :return: True if the event is an event from cloudformation SchedulerSetupHelper custom resource
        """
        return event.get("StackId") is not None and event.get("ResourceType") == "Custom::SchedulerSetupHelper"

    def handle_request(self):
        """
        Handles the custom resource request from cloudformation
        :return: 
        """

        start = datetime.now()
        self._logger.info("Handler {}", self.__class__.__name__)

        self._logger.info("Cloudformation request is {}", safe_json(self._event, indent=2))

        try:
            result = CustomResource.handle_request(self)

            return safe_dict({
                "result": result,
                "datetime": datetime.now().isoformat(),
                "running-time": (datetime.now() - start).total_seconds()
            })
        except Exception as ex:
            self._logger.error("{} {}", ex, traceback.format_exc())
            raise ex

        finally:
            self._logger.flush()

    def _set_lambda_logs_retention_period(self):
        """
        Aligns retention period for default Lambda logstreams with settings 
        :return: 
        """

        if self._context is None:
            return

        loggroup = self._context.log_group_name
        log_client = get_client_with_retries("logs", ["delete_retention_policy", "put_retention_policy"], context=self.context)
        retention_days = self.arguments.get("LogRetentionDays")
        if retention_days is None:
            self._logger.info(INF_DELETE_LOG_RETENTION_POLICY, loggroup)
            log_client.delete_retention_policy_with_retries(self._context.log_group_name)
        else:
            self._logger.info(INF_SET_LOG_RETENTION_POLICY, loggroup, retention_days)
            log_client.put_retention_policy_with_retries(logGroupName=loggroup, retentionInDays=int(retention_days))

    def _create_task_roles_folder(self):
        try:
            s3_client = get_client_with_retries("s3", ["put_object"], context=self.context)
            if self.configuration_bucket:
                s3_client.put_object_with_retries(Bucket=self.configuration_bucket, Body="", Key=configuration.TASK_ROLES_FOLDER)
        except Exception as ex:
            self._logger.warning(WARN_CREATE_TASK_ROLES_FOLDER, configuration.TASK_ROLES_FOLDER, self.configuration_bucket, str(ex))

    def _setup(self):
        """
        SchedulerSetupHelper setup actions
        :return: 
        """
        self._set_lambda_logs_retention_period()
        if self.configuration_bucket:
            self.generate_templates()
        self._create_task_roles_folder()

    def _send_create_metrics(self):

        metrics_data = {
            "Type": "stack",
            "Version": self.arguments["StackVersion"],
            "StackHash": sha256(self.stack_id).hexdigest(),
            "Data": {
                "Status": "stack_create",
                "Region": self.region
            }
        }

        send_metrics_data(metrics=metrics_data, logger=self._logger)

    def _send_delete_metrics(self):

        metrics_data = {
            "Type": "stack",
            "Version": self.arguments["StackVersion"],
            "StackHash": sha256(self.stack_id).hexdigest(),
            "Data": {
                "Status": "stack_delete",
                "Region": self.region
            }
        }

        send_metrics_data(metrics=metrics_data, logger=self._logger)

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
            configuration_template = S3_CONFIGURATION_TEMPLATE.format(action)
            self._logger.info(INF_CREATE_ACTION_TEMPLATE, action, configuration_template)
            template = json.dumps(builder.build_template(action), indent=3)
            s3.put_object_with_retries(Body=template, Bucket=self.configuration_bucket, Key=configuration_template)

        def generate_action_cross_account_role_template(s3, builder, action, template_description):
            role_template = S3_ROLES_TEMPLATE.format(action)
            self._logger.info(INF_CREATECROSS_ROLE_TEMPLATE, action, role_template)
            template = json.dumps(builder.build_template(role_actions=[action], description=template_description), indent=3)
            s3.put_object_with_retries(Body=template, Bucket=self.configuration_bucket, Key=role_template)

        def generate_all_actions_cross_cross_account_role_template(s3, builder, allactions, template_description):
            role_template = S3_ROLES_TEMPLATE.format(ALL_ACTIONS_TEMPLATE_NAME)
            self._logger.info(INF_CREATE_ALL_ACTIONS_CROSS_ROLES_TEMPLAE, role_template)
            template = json.dumps(builder.build_template(role_actions=allactions, description=template_description), indent=3)
            s3.put_object_with_retries(Body=template, Bucket=self.configuration_bucket, Key=role_template)

        self._logger.info(INF_GENERATING_TEMPLATES, self.configuration_bucket)
        try:
            account = AwsService.get_aws_account()
            stack = os.getenv(handlers.ENV_STACK_NAME, "")
            s3_client = get_client_with_retries("s3", ["put_object"], context=self.context)
            config_template_builder = ActionTemplateBuilder(self.context, "arn:aws:region:account:function:debug-only")
            role_template_builder = CrossAccountRoleBuilder(self.scheduler_role_arn)

            all_actions = []
            for action_name in actions.all_actions():
                action_properties = actions.get_action_properties(action_name)
                if not action_properties.get(actions.ACTION_INTERNAL, False):
                    generate_configuration_template(s3_client, config_template_builder, action_name)
                    description = TEMPLATE_DESC_CROSS_ACCOUNT_ACTION.format(action_name, stack, account)
                    generate_action_cross_account_role_template(s3_client, role_template_builder, action_name, description)
                    all_actions.append(action_name)
            if len(all_actions) > 0:
                description = TEMPLATE_DESC_ALL_ACTIONS.format(stack, account)
                generate_all_actions_cross_cross_account_role_template(s3_client, role_template_builder, all_actions, description)

        except Exception as ex:
            self._logger.error(ERR_BUILDING_TEMPLATES, str(ex), traceback.format_exc())

    def delete_templates(self):
        """
        Deletes cross-account role and configuration templates
        :return: 
        """
        s3_client = get_client_with_retries("s3", ["delete_object"], context=self.context)
        s3_key = ""
        try:
            for action_name in actions.all_actions():
                action_properties = actions.get_action_properties(action_name)
                if not action_properties.get(actions.ACTION_INTERNAL, False):
                    self._logger.info(ERR_DELETING_ACTION_TEMPLATE, action_name)
                    s3_key = S3_CONFIGURATION_TEMPLATE.format(action_name)
                    s3_client.delete_object_with_retries(Bucket=self.configuration_bucket, Key=s3_key)
                    s3_key = S3_ROLES_TEMPLATE.format(action_name)
                    s3_client.delete_object_with_retries(Bucket=self.configuration_bucket, Key=s3_key)
        except Exception as ex:
            self._logger.error(ERR_DELETE_TEMPLATE_, s3_key, str(ex))

        try:
            self._logger.info(INF_DELETE_ALL_ACTIONS_TEMPLATE, s3_key)
            s3_key = S3_ROLES_TEMPLATE.format(ALL_ACTIONS_TEMPLATE_NAME)
            s3_client.delete_object_with_retries(Bucket=self.configuration_bucket, Key=s3_key)
        except Exception as ex:
            self._logger.error(ERR_DELETE_TEMPLATE_, s3_key, str(ex))
