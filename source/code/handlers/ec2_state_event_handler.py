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


from datetime import datetime

import handlers
from boto_retry import get_client_with_retries
from configuration.task_configuration import TaskConfiguration
from main import lambda_handler
from util import safe_dict, safe_json
from util.logger import Logger

EC2_STATE_NOTIFICATION = "EC2 Instance State-change Notification"
EC2_STATE_EVENT = "ec2:state"

INFO_EVENT = "Scheduling task {} for ec2 event with state {} for instance {}, account {} in region {}\nTask definition is {}"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


class Ec2StateEventHandler:
    """
    Class that handles time based events from CloudWatch rules
    """

    def __init__(self, event, context):
        """
        Initializes the instance.
        :param event: event to handle
        :param context: CLambda context
        """
        self._context = context
        self._event = event
        self._table = None

        # setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = Logger(logstream=logstream, buffersize=20, context=context)

    @staticmethod
    def is_handling_request(event):
        """
        Tests if event is handled by instance of this handler.
        :param event: tested event
        :param: Tested event
        :return: True if the event is a cloudwatch rule event
        """
        return event.get("source", "") == "aws.ec2" and event.get("detail-type") == EC2_STATE_NOTIFICATION

    def handle_request(self):
        """
        Handled the cloudwatch rule timer event
        :return: Started tasks, if any, information
        """

        def is_matching_event_state(event_state, ec2event):
            return event_state in [s.strip() for s in ec2event.split(",")] or ec2event != "*"

        try:

            result = []
            start = datetime.now()
            self._logger.info("Handler {}", self.__class__.__name__)

            state = self._event.get("detail", {}).get("state")
            if state is not None:
                state = state.lower()

            account = self._event["account"]
            region = self._event["region"]
            instance_id = self._event["detail"]["instance-id"]
            dt = self._event["time"]
            task = None

            try:

                # for all ec2 events tasks in configuration
                for task in [t for t in TaskConfiguration(context=self._context, logger=self._logger).get_tasks() if
                             t.get("events") is not None
                             and EC2_STATE_EVENT in t["events"]
                             and t.get("enabled", True)]:

                    task_name = task["name"]

                    ec2_event = task["events"][EC2_STATE_EVENT]

                    if not is_matching_event_state(state, ec2_event):
                        continue

                    result.append(task_name)

                    self._logger.info(
                        INFO_EVENT, task_name, state, instance_id, account, region, safe_json(task, indent=2))
                    # create an event for lambda function that scans for resources for this task
                    event = {
                        handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
                        handlers.HANDLER_SELECT_ARGUMENTS: {
                            handlers.HANDLER_EVENT_REGIONS: [region],
                            handlers.HANDLER_EVENT_ACCOUNT: account,
                            "InstanceIds": [instance_id]
                        },
                        handlers.HANDLER_EVENT_SOURCE: EC2_STATE_EVENT,
                        handlers.HANDLER_EVENT_TASK: task,
                        handlers.HANDLER_EVENT_TASK_DT: dt
                    }

                    if self._context is not None:
                        # start lambda function to scan for task resources
                        payload = str.encode(safe_json(event))
                        client = get_client_with_retries("lambda", ["invoke"], context=self._context)
                        client.invoke_with_retries(FunctionName=self._context.function_name,
                                                   Qualifier=self._context.function_version,
                                                   InvocationType="Event", LogType="None", Payload=payload)
                    else:
                        # or if not running in lambda environment pass event to main task handler
                        lambda_handler(event, None)

                return safe_dict({
                    "datetime": datetime.now().isoformat(),
                    "running-time": (datetime.now() - start).total_seconds(),
                    "event-datetime": dt,
                    "started-tasks": result
                })

            except ValueError as ex:
                self._logger.error("{}\n{}".format(ex, safe_json(task, indent=2)))

        finally:
            self._logger.flush()
