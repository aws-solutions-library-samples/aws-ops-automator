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
from handlers.event_handler_base import *

ERR_EC2_TAG_EVENT = "Error Ec2Tag event {}"

EC2_TAG_EVENT_PARAM = "E2cTag{}"
EC2_TAGGING_EVENTS_TITLE = "EC2 tag change events"

EC2_TAG_EVENT_SOURCE = "ec2." + handlers.TAG_EVENT_SOURCE

CHANGED_INST_TAG_EVENT_DESCRIPTION_TEXT = "Run task when tags changed for instance"
CHANGED_SNAPSHOT_TAG_EVENT_DESCRIPTION_TEXT = "Run task when tags changed for snapshot"
CHANGED_INSTANCE_TAG_EVENT_LABEL_TEXT = "Tags changed for EC2 Instance"
CHANGED_SNAPSHOT_TAG_EVENT_LABEL_TEXT = "Tags changed for Snapshot"

CHANGE_PREFIX = "Changed{}Tags"

EC2_CHANGED_INSTANCE_TAGS_EVENT = CHANGE_PREFIX.format(services.ec2_service.INSTANCES[0:-1])
EC2_CHANGED_VOLUME_TAGS_EVENT = CHANGE_PREFIX.format(services.ec2_service.VOLUMES[0:-1])
EC2_CHANGED_SNAPSHOT_TAGS_EVENT = CHANGE_PREFIX.format(services.ec2_service.SNAPSHOTS[0:-1])
EC2_CHANGED_IMAGE_TAGS_EVENT = CHANGE_PREFIX.format(services.ec2_service.IMAGES[0:-1])

HANDLED_RESOURCES = [services.ec2_service.INSTANCES, services.ec2_service.SNAPSHOTS]

RESOURCE_MAPPINGS = {
    "instance": services.ec2_service.INSTANCES[0:-1],
    "snapshot": services.ec2_service.SNAPSHOTS[0:-1]
}

HANDLED_EVENTS = {
    EVENT_SOURCE_TITLE: EC2_TAGGING_EVENTS_TITLE,
    EVENT_SOURCE: EC2_TAG_EVENT_SOURCE,
    EVENT_PARAMETER: EC2_TAG_EVENT_PARAM,
    EVENT_EVENTS: {
        EC2_CHANGED_INSTANCE_TAGS_EVENT: {
            EVENT_LABEL: CHANGED_INSTANCE_TAG_EVENT_LABEL_TEXT,
            EVENT_DESCRIPTION: CHANGED_INST_TAG_EVENT_DESCRIPTION_TEXT
        },
        EC2_CHANGED_SNAPSHOT_TAGS_EVENT: {
            EVENT_LABEL: CHANGED_SNAPSHOT_TAG_EVENT_LABEL_TEXT,
            EVENT_DESCRIPTION: CHANGED_SNAPSHOT_TAG_EVENT_DESCRIPTION_TEXT
        },
    }
}


class Ec2TagEventHandler(EventHandlerBase):
    def __init__(self, event, context):
        EventHandlerBase.__init__(self, event=event,
                                  context=context,
                                  resource="",
                                  handled_event_source=EC2_TAG_EVENT_SOURCE,
                                  handled_event_detail_type=handlers.TAG_CHANGE_EVENT,
                                  is_tag_change_event=True,
                                  event_name_in_detail="")

    @staticmethod
    def is_handling_event(event, logger):
        try:
            if event.get("source", "") != handlers.TAG_EVENT_SOURCE:
                return False

            if event.get("detail-type", "") != handlers.TAG_CHANGE_EVENT_SOURCE_DETAIL_TYPE:
                return False

            detail = event.get("detail", {})

            if detail.get("service", "").lower() != "ec2":
                return False

            return detail.get("resource-type", "") in RESOURCE_MAPPINGS
        except Exception as ex:
            logger.error(ERR_EC2_TAG_EVENT, ex)
            return False

    def handle_request(self, use_custom_select=True):
        EventHandlerBase.handle_request(self, use_custom_select=False)

    def _select_parameters(self, event_name, task):
        resource_type = self._event["detail"]["resource-type"]
        id_name = resource_type[0].upper() + resource_type[1:] + "Ids"
        return {id_name: [self._event["resources"][0].split("/")[-1]],
                "_expected_boto3_exceptions_": ["InvalidSnapshot.NotFound",
                                                "InvalidInstanceID.NotFound",
                                                "InvalidVolume.NotFound",
                                                "InvalidAMIID.NotFound"]
                }

    def _event_name(self):
        return CHANGE_PREFIX.format(RESOURCE_MAPPINGS[self._event["detail"]["resource-type"]])

    def _source_resource_tags(self, session, task):
        raise NotImplementedError
