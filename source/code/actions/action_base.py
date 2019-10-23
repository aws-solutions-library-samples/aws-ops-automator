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

import os
import types

import actions
import handlers
import services
import tagging
from actions import date_time_provider
from helpers import pascal_to_snake_case


class ActionBase(object):

    def __init__(self, arguments, action_parameters):
        self._assumed_role_ = None
        self._context_ = None
        self._debug_ = None
        self._dryrun_ = None
        self._event_ = None
        self._events_ = None
        self._logger_ = None
        self._resources_ = None
        self._session_ = None
        self._stack_ = None
        self._stack_id_ = None
        self._stack_resources_ = None
        self._start_result_ = None
        self._started_at_ = None
        self._tagfilter_ = None
        self._task_ = None
        self._task_id_ = None
        self._task_timezone_ = None
        self._timeout_ = None
        self._timeout_event_ = None

        self._datetime_ = date_time_provider()

        for a in arguments:
            setattr(self, "_{}_".format(a), arguments[a])

        if not services.get_service_class(self._event_.get(actions.ACTION_SERVICE)).is_regional():
            self._region_ = self._session_.region_name
        else:
            action_properties = actions.get_action_properties(self._event_[actions.ACTION])
            aggregation_level = action_properties.get(actions.ACTION_AGGREGATION, actions.ACTION_AGGREGATION_RESOURCE)
            if aggregation_level is not None and isinstance(aggregation_level, types.FunctionType):
                aggregation_level = aggregation_level(action_parameters)

            if aggregation_level in [actions.ACTION_AGGREGATION_REGION, actions.ACTION_AGGREGATION_RESOURCE]:
                if isinstance(self._resources_, list):
                    if len(self._resources_) > 0:
                        self._region_ = self._resources_[0]["Region"]
                else:
                    if self._resources_ is not None:
                        self._region_ = self._resources_["Region"]
                if self._region_ is None:
                    self._region_ = self._session_.region_name

            else:
                self._region_ = self._session_.region_name

        self._account_ = self.get_account_for_task()

        if self._debug_ is None:
            self._debug_ = False

        if self._dryrun_ is None:
            self._dryrun_ = False

        for ap in action_parameters:
            setattr(self, "_{}_".format(pascal_to_snake_case(ap)), action_parameters[ap])

    def get(self, name, default=None):
        return getattr(self, "_{}_".format(pascal_to_snake_case(name)), default)

    def time_out(self):
        return self._timeout_event_ is not None and self._timeout_event_.is_set()

    def build_tags_from_template(self,
                                 parameter_name,
                                 region=None,
                                 tag_variables=None,
                                 account=None,
                                 restricted_value_set=False,
                                 include_deleted_tags=True):

        tag_str = self.get(parameter_name, "")
        if tag_str is not None:
            tag_str = tag_str.strip()

        if tag_str in [None, ""]:
            return {}
        tags = tagging.build_tags_from_template(tag_str,
                                                task_id=self._task_id_,
                                                task=self._task_,
                                                timezone=self._task_timezone_,
                                                account=account if account is not None else self._account_,
                                                region=region if region is not None else self._region_,
                                                tag_variables=tag_variables if tag_variables is not None else {},
                                                restricted_value_set=restricted_value_set,
                                                include_deleted_tags=include_deleted_tags)
        return tags

    def build_str_from_template(self, parameter_name, region=None, tag_variables=None, account=None):

        string_template = self.get(parameter_name, "")
        if string_template is not None:
            string_template = string_template.strip()

        if string_template in [None, ""]:
            return ""

        tags = tagging.build_tags_from_template("@str@=" + string_template,
                                                task_id=self._task_id_,
                                                task=self._task_,
                                                timezone=self._task_timezone_,
                                                restricted_value_set=True,
                                                account=account if account is not None else self._account_,
                                                region=region if region is not None else self._region_,
                                                tag_variables=tag_variables if tag_variables is not None else {})

        return tags["@str@"]

    def get_action_session(self, account, param_name=None, logger=None):
        self._logger_.debug("Getting action session for account \"{}\", task is \"{}\", parameter is \"{}\"", account,
                            self._task_, param_name)

        try:
            role_name = self.get(param_name, None)
            if role_name is None:
                role_name = self.get(handlers.TASK_ROLE, None)
            if role_name is None:
                if account == os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT):
                    role_name = None
                else:
                    role_name = handlers.default_rolename_for_stack()

            role_arn = handlers.ARN_ROLE_TEMPLATE.format(account, role_name) if role_name is not None else None
            self._logger_.debug("Role arn is \"{}\"", role_arn)

            return services.get_session(role_arn=role_arn, logger=logger)
        except Exception as ex:
            if logger is not None:
                logger.error(handlers.ERR_CREATING_SESSION, ex)
            return None

    def get_account_for_task(self):
        assumed_role = self.get(actions.ACTION_PARAM_ASSUMED_ROLE, None)
        if assumed_role is not None:
            account = services.account_from_role_arn(assumed_role)
        else:
            account = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)
        return account
