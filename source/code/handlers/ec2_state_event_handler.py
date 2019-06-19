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


import services.ec2_service
from handlers import EC2_EVENT_SOURCE
from handlers.event_handler_base import *
from handlers.event_handler_base import EventHandlerBase

ERR_EC2_STATE_EVENT = "Error testing Ec2State event {}"

EC2_STATE_EVENT_TITLE = "EC2 state events"
EC2_STATE_NOTIFICATION = "EC2 Instance State-change Notification"
EC2_EVENT_STATE_PARAM = "Ec2State{}"
EC2_STATE_SCOPE_PARAM = "Ec2State{}Scope"

EVENT_DESCRIPTION_STARTED = "Run task when EC2 instance is started"
EVENT_DESCRIPTION_STOPPED = "Run task when EC2 instance is stopped"
EVENT_DESCRIPTION_TERMINATED = "Run task when EC2 instance is terminated"
EVENT_LABEL_STARTED = "Instance started"
EVENT_LABEL_STOPPED = "Instance stopped"
EVENT_LABEL_TERMINATED = "Instance terminated"

EC2_STATE_RUNNING = "running"
EC2_STATE_STOPPED = "stopped"
EC2_STATE_TERMINATED = "terminated"

# scope for reacting to EC2_State events
EC2_EVENT_STATE_SCOPE = "Ec2ventStateScope"
EC2_EVENT_STATE_SCOPE_RESOURCE = "Resource"
EC2_EVENT_STATE_SCOPE_REGION = "Region"

HANDLED_EVENTS = {
    EVENT_SOURCE_TITLE: EC2_STATE_EVENT_TITLE,
    EVENT_SOURCE: handlers.EC2_EVENT_SOURCE,
    EVENT_PARAMETER: EC2_EVENT_STATE_PARAM,
    EVENT_SCOPE_PARAMETER: EC2_STATE_SCOPE_PARAM,
    EVENT_EVENTS: {
        EC2_STATE_RUNNING: {
            EVENT_LABEL: EVENT_LABEL_STARTED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_STARTED},
        EC2_STATE_STOPPED: {
            EVENT_LABEL: EVENT_LABEL_STOPPED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_STOPPED},
        EC2_STATE_TERMINATED: {
            EVENT_LABEL: EVENT_LABEL_TERMINATED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_TERMINATED}
    }
}


class Ec2StateEventHandler(EventHandlerBase):
    def __init__(self, event, context):
        EventHandlerBase.__init__(self, event=event,
                                  context=context,
                                  resource=services.ec2_service.INSTANCES,
                                  handled_event_source=EC2_EVENT_SOURCE,
                                  handled_event_detail_type=EC2_STATE_NOTIFICATION,
                                  event_name_in_detail="state")

    @staticmethod
    def is_handling_event(event, logger):
        try:
            return event.get("source", "") == EC2_EVENT_SOURCE and \
                   event.get("detail-type") == EC2_STATE_NOTIFICATION
        except Exception as ex:
            logger.error(ERR_EC2_STATE_EVENT, ex)
            return False

    def _resource_ids(self):
        return [r.split("/")[-1] for r in self._event.get("resources")]

    def _select_parameters(self, event_name, task):

        params = {}

        # no specific instance the service of the task is not ec2, this allows rds service tasks to be triggered by ec2 events
        if task[handlers.TASK_SERVICE] != "ec2":
            return params

        # if the scope is regional then no specific is but all instances in the region
        if task.get(handlers.TASK_EVENT_SCOPES, {}).get(EC2_EVENT_SOURCE, {}).get(EC2_STATE_NOTIFICATION, {}).get(
                event_name, "") == handlers.EVENT_SCOPE_REGION:
            return params

        # just the source instance of the event
        params["InstanceIds"] = self._resource_ids()

        return params

    def _source_resource_tags(self, session, task):
        ec2 = get_client_with_retries("ec2", methods=["DescribeTags"], context=self._context, region=self._event_region(),
                                      session=session, logger=self._logger)

        resp = ec2.describe_tags_with_retries(Filters=[{"Name": "resource-id", "Values": self._resource_ids()}])
        return {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
