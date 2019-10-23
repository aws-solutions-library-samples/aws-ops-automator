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
import json
import threading
import uuid

import requests

from outputs import raise_exception

ERR_SEND_RESP = "Failed executing HTTP request to respond to CloudFormation stack {}, error code i {}, url is {}, " \
                "response data is {}"


class CustomResource(object):
    EVENT_TYPE_CREATE = "Create"
    EVENT_TYPE_UPDATE = "Update"
    EVENT_TYPE_DELETE = "Delete"

    def __init__(self, event, context, logger=None):
        self._event = event
        self._context = context
        self._logger = logger
        # physical resource is empty for create request, for other requests is it the returned physical id from the create request
        self.physical_resource_id = event.get("PhysicalResourceId")
        self.response = {
            "Data": {},
            "Reason": "",
            "StackId": self.stack_id,
            "RequestId": self.request_id,
            "LogicalResourceId": self.logical_resource_id
        }

    @property
    def response_data(self):
        """
        Return data tot the stack, these are the attributes that can be retrieved by Fn::GetAtt
        :return: Resource attributes
        """
        return self.response["Data"]

    @classmethod
    def is_handling_request(cls, event, _):
        """
        Returns true if the event is a custom resource event
        :param _:
        :param event: Tested event
        :return: true if the event is a custom resource event
        """
        return event.get("StackId") is not None

    @property
    def logical_resource_id(self):
        """
        Returns Logical Resource Id in cloudformation stack
        :return: Logical Resource Id in cloudformation stack
        """
        return self.event.get("LogicalResourceId")

    @property
    def event(self):
        """
        Returns the handled event
        :return: the handled event
        """
        return self._event

    @property
    def context(self):
        """
        Returns the context of the lambda function handling the request
        :return: the context of the lambda function handling the request
        """
        return self._context

    @property
    def request_id(self):
        """
        Returns the id of the cloudformation request
        :return: the id of the cloudformation request
        """
        return self.event.get("RequestId")

    @property
    def resource_properties(self):
        """
        Returns the resource properties of the custom resource, these are used to pass data to te custom resource
        :return: the resource properties of the custom resource
        """
        return self.event.get("ResourceProperties", {})

    @property
    def timeout(self):
        """
        Returns optional timeout property in seconds, max is 300
        :return: optional timeout property
        """
        return self.resource_properties.get("timeout", None)

    @property
    def resource_type(self):
        """
        Returns the type of the custom resource
        :return: the type of the custom resource
        """
        return self.event.get("ResourceType")

    @property
    def response_url(self):
        """
        Returns the URL to send the response to cloudformation with the result of the request
        :return: the URL to send the response
        """
        return self.event.get("ResponseURL")

    @property
    def request_type(self):
        """
        Returns the type of the request which can be one of the following: Create, Update, Delete
        :return: the type of the request
        """
        return self.event.get("RequestType")

    @property
    def service_token(self):
        """
        Returns the service token of the request
        :return:
        """
        return self.event.get("ServiceToken")

    @property
    def stack_id(self):
        """
        Returns the id of the stack
        :return: id of the stack
        """
        return self.event.get("StackId")

    @property
    def stack_name(self):
        """
        Returns the short name of the stack
        :return: short name of the stack
        """
        sid = self.stack_id
        last = sid.split(":")[-1]
        name = last.split("/")[-2]
        return name

    @property
    def region(self):
        return self.stack_id.split(":")[3]

    def new_physical_resource_id(self):
        """
        Builds unique physical resource id
        :return: unique physical resource id
        """
        uu = str(uuid.uuid4()).replace("-", "")[0:14]
        new_id = "{}-{}-{}".format(self.__class__.__name__, self.stack_name, uu)
        return new_id.lower()

    def _create_request(self):
        """
        Handles Create request, overwrite in inherited class to implement create actions
        :return: Return True on success, False if on failure
        """
        self.response["Reason"] = "No handler for Create request"
        return True

    def _update_request(self):
        """
        Handles Update request, overwrite in inherited class to implement update actions
        :return: Return True on success, False if on failure
        """
        self.response["Reason"] = "No handler for Update request"
        return True

    def _delete_request(self):
        """
        Handles Delete request, overwrite in inherited class to implement delete actions
        :return: Return True on success, False if on failure
        """
        self.response["Reason"] = "No handler for Delete request"
        return True

    def fn_timeout(self):
        """
        Function is called when the handling of the request times out
        :return:
        """
        print("Execution is about to time out, sending failure message")
        self.response["Status"] = "FAILED"
        self.response["Reason"] = "Timeout"
        self._send_response()

    def handle_request(self):
        """
        Handles cloudformation custom resource request
        :return: response that is send back containing the result
        """
        timer = None
        if self._context is not None:
            time_left = (self._context.get_remaining_time_in_millis() / 1000.00) - 0.5 if self._context is not None else 300
            if self.timeout is not None:
                time_left = min(time_left, float(self.timeout))
            timer = threading.Timer(time_left, self.fn_timeout)
            timer.start()

        try:
            # Call handler for request type
            if self.request_type == CustomResource.EVENT_TYPE_CREATE:
                result = self._create_request()
            elif self.request_type == CustomResource.EVENT_TYPE_UPDATE:
                result = self._update_request()
            elif self.request_type == CustomResource.EVENT_TYPE_DELETE:
                result = self._delete_request()
            else:
                raise ValueError("\"{}\" is not a valid request type".format(self.request_type))

            # Set status based on return value of handler
            self.response["Status"] = "SUCCESS" if result else "FAILED"

            # set physical resource id or create new one
            self.response["PhysicalResourceId"] = self.physical_resource_id or self.new_physical_resource_id()

        except Exception as ex:
            self.response["Status"] = "FAILED"
            self.response["Reason"] = str(ex)

        if timer is not None:
            timer.cancel()

        return self._send_response()

    def _send_response(self):
        """
        Send the response to cloudformation provided url
        :return:
        """
        # Build the PUT request and the response data
        resp = json.dumps(self.response)

        headers = {
            'content-type': '',
            'content-length': str(len(resp))
        }

        # PUT request to cloudformation provided S3 url
        try:
            response = requests.put(self.response_url, data=json.dumps(self.response), headers=headers)
            response.raise_for_status()
            return {"status_code: {}".format(response.status_code),
                    "status_message: {}".format(response.text)}
        except Exception as exc:
            raise_exception(ERR_SEND_RESP, self.stack_id, str(exc), self.response_url, resp)
