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

import services.rds_service
from handlers.event_handler_base import *

ERR_HANDLING_RDS_TAG_EVENT = "Error handling RDS tag event {}"

RDS_TAG_EVENT_PARAM = "RdsTag{}"
RDS_TAGGING_EVENTS_TITLE = "Rds tag change events"

RDS_TAG_EVENT_SOURCE = "rds." + handlers.TAG_EVENT_SOURCE

CHANGED_DBIBSTANCE_TAG_EVENT_DESCRIPTION_TEXT = "Run task when tags changed for instance"
CHANGED_DBIBSTANCE_TAG_EVENT_LABEL_TEXT = "Tags changed for RDS Instance"

CHANGED_DBCLUSTER_TAG_EVENT_DESCRIPTION_TEXT = "Run task when tags changed for cluster"
CHANGED_DBCLUSTER_TAG_EVENT_LABEL_TEXT = "Tags changed for RDS Cluster"

CHANGE_PREFIX = "Changed{}Tags"

RDS_CHANGED_INSTANCE_TAGS_EVENT = CHANGE_PREFIX.format(services.rds_service.DB_INSTANCES[0:-1])
RDS_CHANGED_CLUSTER_TAGS_EVENT = CHANGE_PREFIX.format(services.rds_service.DB_CLUSTERS[0:-1])

HANDLED_RESOURCES = [services.rds_service.DB_INSTANCES]

RESOURCE_MAPPINGS = {
    "db": services.rds_service.DB_INSTANCES[0:-1],
    "cluster": services.rds_service.DB_CLUSTERS[0:-1]
}

HANDLED_EVENTS = {
    EVENT_SOURCE_TITLE: RDS_TAGGING_EVENTS_TITLE,
    EVENT_SOURCE: RDS_TAG_EVENT_SOURCE,
    EVENT_PARAMETER: RDS_TAG_EVENT_PARAM,
    EVENT_EVENTS: {
        RDS_CHANGED_INSTANCE_TAGS_EVENT: {
            EVENT_LABEL: CHANGED_DBIBSTANCE_TAG_EVENT_LABEL_TEXT,
            EVENT_DESCRIPTION: CHANGED_DBIBSTANCE_TAG_EVENT_DESCRIPTION_TEXT
        },
        RDS_CHANGED_CLUSTER_TAGS_EVENT: {
            EVENT_LABEL: CHANGED_DBCLUSTER_TAG_EVENT_LABEL_TEXT,
            EVENT_DESCRIPTION: CHANGED_DBCLUSTER_TAG_EVENT_DESCRIPTION_TEXT
        }
    }
}


class RdsTagEventHandler(EventHandlerBase):
    def __init__(self, event, context):
        """
        Initialize event.

        Args:
            self: (todo): write your description
            event: (todo): write your description
            context: (str): write your description
        """
        EventHandlerBase.__init__(self, event=event,
                                  context=context,
                                  resource="",
                                  handled_event_source=RDS_TAG_EVENT_SOURCE,
                                  handled_event_detail_type=handlers.TAG_CHANGE_EVENT,
                                  is_tag_change_event=True,
                                  event_name_in_detail="")

    @staticmethod
    def is_handling_event(event, logger):
        """
        Returns true if the event is an event.

        Args:
            event: (dict): write your description
            logger: (todo): write your description
        """
        try:
            if event.get("source", "") != handlers.TAG_EVENT_SOURCE:
                return False

            if event.get("detail-type", "") != handlers.TAG_CHANGE_EVENT_SOURCE_DETAIL_TYPE:
                return False

            detail = event.get("detail", {})

            if detail.get("service", "").lower() != "rds":
                return False

            return detail.get("resource-type", "") in RESOURCE_MAPPINGS
        except Exception as ex:
            logger.error(ERR_HANDLING_RDS_TAG_EVENT, ex)
            return False

    def handle_request(self, use_custom_select=True):
        """
        Handle a request.

        Args:
            self: (todo): write your description
            use_custom_select: (bool): write your description
        """
        EventHandlerBase.handle_request(self, use_custom_select=False)

    def _select_parameters(self, event_name, task):
        """
        Selects parameters of the given event.

        Args:
            self: (todo): write your description
            event_name: (str): write your description
            task: (todo): write your description
        """
        resource_type = self._event["detail"]["resource-type"]
        if resource_type not in RESOURCE_MAPPINGS:
            raise NotImplementedError

        res = RESOURCE_MAPPINGS[self._event["detail"]["resource-type"]]
        res = "DB" + res[2:]
        return {res+"Identifier": self._event["resources"][0].split(":")[-1],
                "_expected_boto3_exceptions_": [res + "NotFound"]
                }

    def _event_name(self):
        """
        Returns the name of the event.

        Args:
            self: (todo): write your description
        """
        return CHANGE_PREFIX.format(RESOURCE_MAPPINGS[self._event["detail"]["resource-type"]])

    def _source_resource_tags(self, session, task):
        """
        Creates a resource tags for a resource.

        Args:
            self: (todo): write your description
            session: (todo): write your description
            task: (todo): write your description
        """
        raise NotImplementedError
