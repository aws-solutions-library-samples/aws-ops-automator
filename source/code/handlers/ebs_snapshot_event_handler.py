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
import actions
import services.ec2_service
from actions import marker_snapshot_tag_source_source_volume_id
from boto_retry import get_default_retry_strategy
from handlers.event_handler_base import *
from handlers.event_handler_base import EventHandlerBase

ERR_EVENT_EBS_HANDLER = "Error handling event in EBS snapshot event handler {}"

EVENT_EBS_PARAMETER = "Ebs{}"
EVENT_SOURCE_TITLE_TEXT = "EBS snapshot events"

EVENT_DESCRIPTION_SNAPSHOT_CREATED = "Run task when EBS snapshot is created"
EVENT_DESCRIPTION_SNAPSHOT_COPIED_FOR_VOLUME = "Run task when EBS snapshot for volume is copied"
EVENT_DESCRIPTION_SNAPSHOT_CREATED_FOR_VOLUME = "Run task when EBS snapshot for volume is created"
EVENT_DESCRIPTION_SNAPSHOT_SHARED = "Run task when EBS snapshot is shared by the account(event is raised for the account " \
                                    "the snapshot is shared with)"
EVENT_DESCRIPTION_SNAPSHOT_COPIED = "Run task when EBS snapshot is copied (event is raised in region the snapshot is copied to)"

EVENT_LABEL_SNAPSHOT_COPIED = "Snapshot copied"
EVENT_LABEL_SNAPSHOT_COPIED_FOR_VOLUME = "Snapshot for volume copied"
EVENT_LABEL_SNAPSHOT_CREATED = "Snapshot created"
EVENT_LABEL_SNAPSHOT_CREATED_FOR_VOLUME = "Snapshot for volume created"
EVENT_LABEL_SNAPSHOT_SHARED = "Snapshot shared with account"

ERR_GETTING_SOURCE_VOLUME = "Error retrieving source snapshot and volume for copied snapshot {}, {}"

WARN_SOURCE_VOLUME_NOT_FOUND = "Source volume could not be found for copied snapshot {}"

EBS_SNAPSHOT_NOTIFICATION = "EBS Snapshot Notification"

EBS_SNAPSHOT_CREATED = "createSnapshot"
EBS_SNAPSHOT_COPIED = "copySnapshot"
EBS_SNAPSHOT_SHARED = "shareSnapshot"
EBS_SNAPSHOT_FOR_VOLUME_CREATED = "createSnapShotForVolume"
EBS_SNAPSHOT_FOR_VOLUME_COPIED = "copySnapShotForVolume"

HANDLED_EVENTS = {
    EVENT_SOURCE_TITLE: EVENT_SOURCE_TITLE_TEXT,
    EVENT_SOURCE: handlers.EC2_EVENT_SOURCE,
    EVENT_PARAMETER: EVENT_EBS_PARAMETER,
    EVENT_EVENTS: {
        EBS_SNAPSHOT_CREATED: {
            EVENT_LABEL: EVENT_LABEL_SNAPSHOT_CREATED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_SNAPSHOT_CREATED},
        EBS_SNAPSHOT_COPIED: {
            EVENT_LABEL: EVENT_LABEL_SNAPSHOT_COPIED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_SNAPSHOT_COPIED},
        EBS_SNAPSHOT_SHARED: {
            EVENT_LABEL: EVENT_LABEL_SNAPSHOT_SHARED,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_SNAPSHOT_SHARED},
        EBS_SNAPSHOT_FOR_VOLUME_CREATED: {
            EVENT_LABEL: EVENT_LABEL_SNAPSHOT_CREATED_FOR_VOLUME,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_SNAPSHOT_CREATED_FOR_VOLUME},
        EBS_SNAPSHOT_FOR_VOLUME_COPIED: {
            EVENT_LABEL: EVENT_LABEL_SNAPSHOT_COPIED_FOR_VOLUME,
            EVENT_DESCRIPTION: EVENT_DESCRIPTION_SNAPSHOT_COPIED_FOR_VOLUME},
    }
}


class EbsSnapshotEventHandler(EventHandlerBase):

    def __init__(self, event, context):
        """
        Initialize the event.

        Args:
            self: (todo): write your description
            event: (todo): write your description
            context: (str): write your description
        """
        EventHandlerBase.__init__(self, event=event,
                                  resource=services.ec2_service.SNAPSHOTS,
                                  context=context,
                                  handled_event_source=handlers.EC2_EVENT_SOURCE,
                                  handled_event_detail_type=EBS_SNAPSHOT_NOTIFICATION)
        self._volume_event = self._event["detail"]["event"] == EBS_SNAPSHOT_FOR_VOLUME_CREATED
        self._source = self._event.get("detail", {}).get("source", "").split(":")[-1].split("/")[-1]

    @staticmethod
    def is_handling_event(event, logger):
        """
        Returns true if event is event.

        Args:
            event: (dict): write your description
            logger: (todo): write your description
        """
        try:
            return event.get("source", "") == handlers.EC2_EVENT_SOURCE and \
                   event.get("detail-type") == EBS_SNAPSHOT_NOTIFICATION and \
                   event.get("detail", {}).get("result", None) == "succeeded"
        except Exception as ex:
            logger.error(ERR_EVENT_EBS_HANDLER, ex)
            return False

    def _select_parameters(self, event_name, task):
        """
        Creates task parameters

        Args:
            self: (todo): write your description
            event_name: (str): write your description
            task: (todo): write your description
        """
        if self._event_name() == EBS_SNAPSHOT_FOR_VOLUME_CREATED:
            return {
                "Filters": [{"Name": "volume-id", "Values": [self._source]}],
                "_expected_boto3_exceptions_": ["InvalidVolume.NotFound"]
            }

        if self._event_name() == EBS_SNAPSHOT_FOR_VOLUME_COPIED:

            ec2 = services.create_service("ec2", role_arn=self._role_executing_triggered_task,
                                          service_retry_strategy=get_default_retry_strategy("ec2", context=self._context))

            try:
                source_volume = None
                copied_snapshot_id = self._event["detail"]["snapshot_id"].split("/")[-1]
                # get the copied snapshot with tags
                copied_snapshot = ec2.get(services.ec2_service.SNAPSHOTS,
                                          SnapshotIds=[copied_snapshot_id],
                                          OwnerIds=["self"],
                                          region=self._event_region(),
                                          tags=True,
                                          _expected_boto3_exceptions_=["InvalidSnapshot.NotFound"])

                if copied_snapshot is not None:
                    # get the source volume from the tags
                    source_volume = copied_snapshot.get("Tags", {}).get(marker_snapshot_tag_source_source_volume_id(), None)

                if source_volume is None:
                    self._logger.warning(WARN_SOURCE_VOLUME_NOT_FOUND, copied_snapshot_id)
                    return None

                snapshots = list(
                    ec2.describe(services.ec2_service.SNAPSHOTS,
                                 region=self._event_region(),
                                 OwnerIds=["self"],
                                 Filters=[
                                     {
                                         "Name": "volume-id",
                                         "Values": [source_volume]
                                     }
                                 ],
                                 _expected_boto3_exceptions_=["InvalidVolume.NotFound"]))

                snapshots += list(
                    ec2.describe(services.ec2_service.SNAPSHOTS,
                                 region=self._event_region(),
                                 OwnerIds=["self"], tags=True,
                                 Filters=[
                                     {
                                        "Name": "tag:" + actions.marker_snapshot_tag_source_source_volume_id(),
                                        "Values": [source_volume]
                                     }
                                 ],
                                 _expected_boto3_exceptions_=["InvalidVolume.NotFound"]))

                if len(snapshots) == 0:
                    return None

                snapshots = list(set([s["SnapshotId"] for s in snapshots]))

                return {
                    handlers.HANDLER_EVENT_RESOURCE_NAME: services.ec2_service.SNAPSHOTS,
                    handlers.HANDLER_EVENT_REGIONS: [self._event_region()],
                    "SnapshotIds": snapshots,
                    "_expected_boto3_exceptions_": ["InvalidSnapshot.NotFound"]
                }

            except Exception as ex:
                self._logger.error(ERR_GETTING_SOURCE_VOLUME, self._event.get("detail", {}).get("source", ""), ex)
                return None

        return {
            "SnapshotIds": [r.split("/")[-1] for r in self._event.get("resources")],
            "_expected_boto3_exceptions_": ["InvalidSnapshot.NotFound"]
        }

    def handle_request(self, use_custom_select=True):
        """
        Handle a request handler.

        Args:
            self: (todo): write your description
            use_custom_select: (bool): write your description
        """
        # handle regular EBS snapshot events on snapshot level
        EventHandlerBase.handle_request(self, use_custom_select)
        if self._event.get("detail", {}).get("event", None) == EBS_SNAPSHOT_CREATED:
            # if there is a source volume then also handle events at volume level for the created snapshot
            if self._source not in ["", None]:
                self._event["detail"]["event"] = EBS_SNAPSHOT_FOR_VOLUME_CREATED
                EventHandlerBase.handle_request(self, use_custom_select)

        if self._event.get("detail", {}).get("event", None) == EBS_SNAPSHOT_COPIED:
            # if there is a source volume then also handle events at volume level for the created snapshot

            if self._source not in ["", None]:
                self._event["detail"]["event"] = EBS_SNAPSHOT_FOR_VOLUME_COPIED
                EventHandlerBase.handle_request(self, use_custom_select)

    def _source_resource_tags(self, session, task):
        """
        Creates a resource tags for a resource.

        Args:
            self: (todo): write your description
            session: (todo): write your description
            task: (todo): write your description
        """
        raise NotImplementedError
