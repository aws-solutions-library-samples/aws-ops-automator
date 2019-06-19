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

from copy import copy
from datetime import datetime

from configuration import CONFIG_TASK_NAME
from configuration.task_admin_api import create_task, delete_task, update_task
from configuration.task_configuration import TaskConfiguration
from handlers.custom_resource import CustomResource
from helpers import safe_dict, safe_json
from outputs import raise_exception
from outputs.queued_logger import QueuedLogger

ERR_DELETING_TASK = "Error deleting task {}, {}"
ERR_UPDATING_TASK = "Error updating task {}, {}"
ERR_CREATING_TASK_ = "Error creating task {}, {}"

ERR_NO_TASK_NAME_RESOURCE_PROPERTY = "Name of Task must be specified in Name property"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class ConfigurationResourceHandler(CustomResource):
    def __init__(self, event, context):
        CustomResource.__init__(self, event, context)

        self.arguments = copy(self.resource_properties)
        self.arguments = {a: self.resource_properties[a] for a in self.resource_properties if a not in ["ServiceToken",
                                                                                                        "Timeout"]}
        # setup logging
        dt = datetime.utcnow()
        classname = self.__class__.__name__
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = QueuedLogger(logstream=logstream, context=context, buffersize=20)

    @classmethod
    def is_handling_request(cls, event, _):
        return event.get("StackId") is not None and event.get("ResourceType") == "Custom::TaskConfig"

    def handle_request(self):

        start = datetime.now()

        self._logger.info("Cloudformation request is {}", safe_json(self._event, indent=2))

        try:
            result = CustomResource.handle_request(self)

            return safe_dict({
                "datetime": datetime.now().isoformat(),
                "running-time": (datetime.now() - start).total_seconds(),
                "result": result
            })

        finally:
            self._logger.flush()

    def _create_request(self):

        name = self.resource_properties[CONFIG_TASK_NAME]
        try:
            self._logger.info("Creating new Task resource with name {}", name)
            self.physical_resource_id = name
            self.task = create_task(**self.arguments)
            self._logger.info("Created new resource with physical resource id {}", self.physical_resource_id)
            return True

        except Exception as ex:
            self.response["Reason"] = str(ex)
            self._logger.error(ERR_CREATING_TASK_, name, ex)
            return False

    def _update_request(self):

        self._logger.info("Updating Task resource")
        name = self.resource_properties.get(CONFIG_TASK_NAME)
        try:
            if name is None:
                raise_exception(ERR_NO_TASK_NAME_RESOURCE_PROPERTY)

            if name != self.physical_resource_id:
                self._logger.info("Name change for resource with physical resource id {}, new value is {}",
                                  name, self.physical_resource_id)
                self.arguments[CONFIG_TASK_NAME] = name
                create_task(**self.arguments)
                self.physical_resource_id = name
                self._logger.info("Created new resource with physical resource id {}", self.physical_resource_id)
            else:
                update_task(name, **self.arguments)
                self._logger.info("Updated resource with physical resource id {}", self.physical_resource_id)
            return True

        except Exception as ex:
            self.response["Reason"] = str(ex)
            self._logger.error(ERR_UPDATING_TASK, name, ex)
            return False

    def _delete_request(self):

        self._logger.info("Deleting Task resource")
        name = self.resource_properties.get(CONFIG_TASK_NAME)
        try:
            self._logger.info("Task name is {}, physical resource id is {}", name, self.physical_resource_id)
            # as the task can be part of a different stack than the scheduler that owns the configuration table the table could
            # be deleted by that stack, so first check if the table still exists
            if TaskConfiguration.config_table_exists():
                delete_task(self.physical_resource_id)
                self._logger.info("Deleted resource {} with physical resource id {}", name, self.physical_resource_id)
            else:
                self._logger.info("Configuration table does not longer exist so deletion of item skipped")
            return True

        except Exception as ex:
            self.response["Reason"] = str(ex)
            self._logger.error(ERR_DELETING_TASK, name, ex)
            return False
