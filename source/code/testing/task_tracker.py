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
import os
import time
import types
import uuid
from datetime import datetime

import actions
import handlers
import services


class TaskTracker(object):
    """
    Class that implements logic to create and update the status of action in a dynamodb table.
    """

    def __init__(self, ):
        self._task_items = []

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

    def add_task_action(self, task, assumed_role, action_resources, task_datetime, source, task_group=None):

        item = {
            handlers.TASK_TR_ID: str(uuid.uuid4()),
            handlers.TASK_TR_NAME: task[handlers.TASK_NAME],
            handlers.TASK_TR_ACTION: task[handlers.TASK_ACTION],
            handlers.TASK_TR_CREATED: datetime.now().isoformat(),
            handlers.TASK_TR_CREATED_TS: time.time(),
            handlers.TASK_TR_SOURCE: source,
            handlers.TASK_TR_DT: task_datetime,
            handlers.TASK_TR_STATUS: handlers.STATUS_PENDING,
            handlers.TASK_TR_DEBUG: task[handlers.TASK_DEBUG],
            handlers.TASK_TR_NOTIFICATIONS: task[handlers.TASK_NOTIFICATIONS],
            handlers.TASK_TR_HAS_COMPLETION: getattr(actions.get_action_class(task[handlers.TASK_ACTION]),
                                                     handlers.COMPLETION_METHOD, None) is not None,
            handlers.TASK_TR_METRICS: task[handlers.TASK_METRICS],
            handlers.TASK_TR_DRYRUN: task[handlers.TASK_DRYRUN],
            handlers.TASK_TR_INTERNAL: task[handlers.TASK_INTERNAL],
            handlers.TASK_INTERVAL: task[handlers.TASK_INTERVAL],
            handlers.TASK_TR_TIMEOUT: task[handlers.TASK_TIMEOUT],
            handlers.TASK_TR_TIMEZONE: task[handlers.TASK_TIMEZONE],
            handlers.TASK_TR_STARTED_TS: int(time.time()),
            handlers.TASK_TR_EXECUTE_SIZE: task[handlers.TASK_EXECUTE_SIZE],
            handlers.TASK_TR_SELECT_SIZE: task[handlers.TASK_SELECT_SIZE],
            handlers.TASK_TR_COMPLETION_SIZE: task[handlers.TASK_COMPLETION_SIZE],
            handlers.TASK_TR_TAGFILTER: task[handlers.TASK_TAG_FILTER],
            handlers.TASK_TR_EVENTS: task.get(handlers.TASK_EVENTS, {}),
            handlers.TASK_TR_RUN_LOCAL: True,
            handlers.TASK_TR_GROUP: task_group
        }
        if assumed_role not in [None, ""]:
            item[handlers.TASK_TR_ASSUMED_ROLE] = assumed_role
            item[handlers.TASK_TR_ACCOUNT] = services.account_from_role_arn(assumed_role)
        else:
            item[handlers.TASK_TR_ACCOUNT] = os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT)

        if len(task[handlers.TASK_PARAMETERS]) > 0:
            item[handlers.TASK_TR_PARAMETERS] = task[handlers.TASK_PARAMETERS]

        parameters = item.get(handlers.TASK_TR_PARAMETERS, None)
        if parameters is not None:
            item[handlers.TASK_TR_PARAMETERS] = parameters

        # check if the class has a field or static method that returns true if the action class needs completion
        # this way we can make completion dependent of parameter values
        has_completion = getattr(actions.get_action_class(task[handlers.TASK_ACTION]), actions.ACTION_PARAM_HAS_COMPLETION, None)
        if has_completion is not None:
            # if it is static method call it passing the task parameters
            if isinstance(has_completion, types.FunctionType):
                has_completion = has_completion(parameters)
        else:
            # if it does not have this method test if the class has an us_complete method
            has_completion = getattr(actions.get_action_class(task[handlers.TASK_ACTION]),
                                     handlers.COMPLETION_METHOD, None) is not None

        item[handlers.TASK_TR_HAS_COMPLETION] = has_completion

        item[handlers.TASK_TR_RESOURCES] = action_resources

        self._task_items.append(self.dynamo_safe_attribute_types(item))

        return item

    def update_action(self, action_id, _, __, status=None, status_data=None):
        try:
            item = [t for t in self._task_items if t[handlers.TASK_TR_ID] == action_id][0]
        except Exception as ex:
            raise ("Error updating task {} in task tracking".format(action_id, ex))

        if status is not None:
            item[handlers.TASK_TR_STATUS] = status

        if status_data is not None:
            self.dynamo_safe_attribute_types(status_data)
            item.update(status_data)

    @property
    def items(self):
        return len(self._task_items)

    def get_task_items_for_job(self, task_group):
        return [i for i in self._task_items if i.get(handlers.TASK_TR_GROUP, None) == task_group]

    @property
    def task_items(self):
        return self._task_items

    @classmethod
    def dynamo_safe_attribute_types(cls, data):

        def check_attributes(d):
            for attr in d.keys():
                if isinstance(d[attr], datetime):
                    d[attr] = d[attr].isoformat()
                    continue

                if isinstance(d[attr], basestring) and d[attr].strip() == "":
                    del d[attr]
                    continue

                if isinstance(d[attr], dict):
                    d[attr] = cls.dynamo_safe_attribute_types(d[attr])
                    continue

        if isinstance(data, list):
            for i in data:
                check_attributes(i)
        else:
            check_attributes(data)

        return data

    def flush(self, timeout_event=None):
        pass
