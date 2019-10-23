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
import services.s3_service
from handlers.event_handler_base import *


EVENT_DESCRIPTION_OBJECT_CREATED = "Run task when S3 Object is created"
EVENT_DESCRIPTION_OBJECT_DELETED = "Run task when S3 Object is deleted"
EVENT_LABEL_OBJECT_CREATED = "Object created"

EVENT_LABEL_OBJECT_DELETED = "Object deleted"
S3_EVENTS_PARAM = "S3{}"
S3_EVENTS_TITLE = "S3 events"

S3_EVENT_DETAIL_TYPE = "S3 Object Event"
S3_OBJECT_CREATED = "ObjectCreated"
S3_OBJECT_DELETED = "ObjectDeleted"

S3_OBJECT_EVENTS = [S3_OBJECT_CREATED, S3_OBJECT_DELETED]

HANDLED_EVENTS = {
    EVENT_SOURCE_TITLE: S3_EVENTS_TITLE,
    EVENT_SOURCE: handlers.S3_EVENT_SOURCE,
    EVENT_PARAMETER: S3_EVENTS_PARAM,
    EVENT_EVENTS: {
        S3_OBJECT_CREATED: {
            EVENT_LABEL: EVENT_LABEL_OBJECT_CREATED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_OBJECT_CREATED},
        S3_OBJECT_DELETED: {
            EVENT_LABEL: EVENT_LABEL_OBJECT_DELETED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_OBJECT_DELETED}
    }
}


class S3EventHandler(EventHandlerBase):
    def __init__(self, event, context):
        EventHandlerBase.__init__(self,
                                  event=event,
                                  resource=services.s3_service.OBJECT,
                                  context=context,
                                  handled_event_detail_type=S3_EVENT_DETAIL_TYPE,
                                  handled_event_source=handlers.S3_EVENT_SOURCE)
        self._event["detail-type"] = S3_EVENT_DETAIL_TYPE

    @staticmethod
    def is_handling_event(event, logger):
        return len(event.get("Records", [])) == 1 and \
               event["Records"][0].get("eventSource", "") == handlers.S3_EVENT_SOURCE and \
               event["Records"][0].get("eventName", "").split(":")[0] in S3_OBJECT_EVENTS

    def _select_parameters(self, event_name, task):
        return {}

    def _event_resources(self):
        return [{
            "Bucket": self._event["Records"][0]["s3"]["bucket"]["name"],
            "Key": self._event["Records"][0]["s3"]["object"]["key"],
            "BucketArn": self._event["Records"][0]["s3"]["bucket"]["arn"],
            "Owner": self._event["Records"][0]["s3"]["bucket"]["ownerIdentity"],
            "Size": self._event["Records"][0]["s3"]["object"]["size"],
            "AwsAccount": self._event_account(),
            "Region": self._event_region()
        }]

    def _event_name(self):
        return self._event["Records"][0]["eventName"].split(":")[0]

    def _event_region(self):
        return self._event["Records"][0]["awsRegion"]

    def _event_account(self):
        return None

    def _event_time(self):
        return self._event.get("Records", [{}])[0].get("eventTime")

    def _source_resource_tags(self, session, task):
        raise NotImplementedError

