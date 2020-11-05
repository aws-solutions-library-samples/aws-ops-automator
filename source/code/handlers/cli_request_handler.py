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
from datetime import datetime

import jmespath

import configuration
import configuration.task_admin_api
from helpers import safe_json
from outputs.queued_logger import QueuedLogger

CLI_SOURCE = "ops-automator.cli"
TEST_SOURCE = "ops-automator-test"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class CliRequestHandler(object):
    """
    Class to handles requests from admin CLI
    """

    def __init__(self, event, context):
        """
        Initializes handle instance
        :param event: event to handle
        :param context: lambda context
        """
        self._event = event
        self._context = context
        self._logger = None

        self.additional_parameters = {
        }

        self.commands = {
            "describe-tasks": "get_tasks" if self.parameters.get("name") is None else "get_task",
            "start-task": "start_task"
        }

        self.attribute_transformations = {
        }
        self.result_transformations = {
        }

        # Setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = QueuedLogger(logstream=logstream, buffersize=20, context=self._context)

    @property
    def action(self):
        """
        Retrieves admin REST api action from the event
        :return: name of the action of the event
        """
        return self._event["action"]

    @property
    def parameters(self):
        """
        Returns a dict of the parameters.

        Args:
            self: (todo): write your description
        """
        params = self._event.get("parameters", {})
        extra = self.additional_parameters.get(self.action, {})
        params.update(extra)

        return {p.replace("-", "_"): params[p] for p in params}

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Returns True if the handler can handle the event
        :param _:
        :param event: tested event
        :return: True if the handles does handle the tested event
        """

        if event.get("source", "") not in [CLI_SOURCE, TEST_SOURCE]:
            return False
        return "action" in event

    def handle_request(self):
        """
        Handles the event
        :return: result of handling the event, result send back to REST admin api
        """

        def snake_to_pascal_case(s):
            """
            Convert a string to snake case.

            Args:
                s: (str): write your description
            """
            converted = ""
            s = s.strip("_").capitalize()
            i = 0

            while i < len(s):
                if s[i] == "-":
                    pass
                elif s[i] == "_":
                    i += 1
                    converted += s[i].upper()
                else:
                    converted += s[i]
                i += 1

            return converted

        def dict_to_pascal_case(d):
            """
            Convert a dictionary to snake case.

            Args:
                d: (todo): write your description
            """

            ps = {}

            if isinstance(d, dict):
                for i in d:
                    key = snake_to_pascal_case(i)
                    ps[key] = dict_to_pascal_case(d[i])
                return ps

            elif isinstance(d, list):
                return [dict_to_pascal_case(l) for l in d]

            return d

        try:
            self._logger.info("Handler {} : Received CLI request \n{}", self.__class__.__name__, safe_json(self._event, indent=3))

            # get access to admin api module
            admin_module = configuration.task_admin_api

            # get api action and map it to a function in the admin API
            fn_name = self.commands.get(self.action, None)
            if fn_name is None:
                raise ValueError("Command {} does not exist".format(self.action))
            fn = getattr(admin_module, fn_name)

            # calling the mapped admin api method
            self._logger.info("Calling \"{}\" with parameters \n{}", fn.__name__, safe_json(self.parameters, indent=3))

            args = self.parameters
            args["context"] = self._context
            api_result = fn(**args)

            # convert to awscli PascalCase output format
            result = dict_to_pascal_case(api_result)

            # perform output transformation
            if fn_name in self.result_transformations:
                result = jmespath.search(self.result_transformations[fn_name], result)

            for t in self.attribute_transformations:
                if t in result:
                    if self.attribute_transformations[t] is not None:
                        result[self.attribute_transformations[t]] = result[t]
                        del result[t]

            # log formatted result
            json_result = safe_json(result, 3)
            self._logger.info("Call result is {}", json_result)

            return result

        except Exception as ex:
            self._logger.info("Call failed, error is {}", str(ex))
            return {"Error": str(ex)}
        finally:
            self._logger.flush()
