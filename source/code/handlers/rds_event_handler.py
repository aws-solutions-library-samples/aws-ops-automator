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


import services.rds_service
from handlers.event_handler_base import *

ERR_RDS_CLOUD_TRAIL_EVENT_ = "Error handling RDS CloudTrail event {}"

RDS_AWS_API_CALL = "AWS API Call via CloudTrail"

# Note that when the cloudtrail event happens the action may not be completed yet
RDS_INSTANCE_STARTED = "StartDBInstance"
RDS_INSTANCE_CREATED = "CreateDBInstance"
RDS_INSTANCE_RESTORED = "RestoreDBInstanceFromDBSnapshot"
RDS_INSTANCE_STOPPED = "StopDBInstance"
RDS_INSTANCE_DELETED = "DeleteDBInstance"
RDS_CLUSTER_STARTED = "StartDBCluster"
RDS_CLUSTER_CREATED = "CreateDBCluster"
RDS_CLUSTER_RESTORED = "RestoreDBClusterFromSnapshot"
RDS_CLUSTER_STOPPED = "StopDBCluster"
RDS_CLUSTER_DELETED = "DeleteDBCluster"

RDS_HANDLED_EVENTS = [
    RDS_INSTANCE_STARTED,
    RDS_INSTANCE_STOPPED,
    RDS_INSTANCE_RESTORED,
    RDS_INSTANCE_CREATED,
    RDS_INSTANCE_DELETED,
    RDS_CLUSTER_STARTED,
    RDS_CLUSTER_STOPPED,
    RDS_CLUSTER_RESTORED,
    RDS_CLUSTER_CREATED,
    RDS_CLUSTER_DELETED
]

HANDLED_EVENTS = {
    EVENT_SOURCE_TITLE: "RDS events",
    EVENT_SOURCE: handlers.RDS_EVENT_SOURCE,
    EVENT_PARAMETER: "Rds{}",
    EVENT_SCOPE_PARAMETER: "Rds{}Scope",
    EVENT_EVENTS: {
        RDS_INSTANCE_STARTED: {
            EVENT_LABEL: "Instance started",
            EVENT_DESCRIPTION: "Run task when instance is started"
        },
        RDS_INSTANCE_CREATED: {
            EVENT_LABEL: "Instance created",
            EVENT_DESCRIPTION: "Run task when instance is created"
        },
        RDS_INSTANCE_RESTORED: {
            EVENT_LABEL: "Instance restored",
            EVENT_DESCRIPTION: "Run task when instance is restored from snapshot"
        },
        RDS_INSTANCE_STOPPED: {
            EVENT_LABEL: "Instance stopped",
            EVENT_DESCRIPTION: "Run task when RDS instance is stopped"
        },
        RDS_INSTANCE_DELETED: {
            EVENT_LABEL: "Instance deleted",
            EVENT_DESCRIPTION: "Run task when instance is deleted"
        },
        RDS_CLUSTER_STARTED: {
            EVENT_LABEL: "Cluster started",
            EVENT_DESCRIPTION: "Run task when cluster is started"
        },
        RDS_CLUSTER_CREATED: {
            EVENT_LABEL: "Cluster created",
            EVENT_DESCRIPTION: "Run task when cluster is created"
        },
        RDS_CLUSTER_RESTORED: {
            EVENT_LABEL: "Cluster restored",
            EVENT_DESCRIPTION: "Run task when cluster is restored from snapshot"
        },
        RDS_CLUSTER_STOPPED: {
            EVENT_LABEL: "Cluster stopped",
            EVENT_DESCRIPTION: "Run task when cluster is stopped"
        },
        RDS_CLUSTER_DELETED: {
            EVENT_LABEL: "Cluster deleted",
            EVENT_DESCRIPTION: "Run task when cluster is deleted"
        }
    }
}


class RdsEventHandler(EventHandlerBase):
    def __init__(self, event, context):
        EventHandlerBase.__init__(self, event=event,
                                  context=context,
                                  resource=services.rds_service.DB_INSTANCES,
                                  handled_event_source="aws.rds",
                                  handled_event_detail_type="AWS API Call via CloudTrail",
                                  event_name_in_detail="eventName")

    @staticmethod
    def is_handling_event(event, logger):
        try:
            if event.get("source", "") != handlers.RDS_EVENT_SOURCE:
                return False

            if event.get("detail-type", "") != RDS_AWS_API_CALL:
                return False

            event_name = event.get("detail", {}).get("eventName", "")
            return event_name in RDS_HANDLED_EVENTS
        except Exception as ex:
            logger.error(ERR_RDS_CLOUD_TRAIL_EVENT_, ex)
            return False

    def _event_name(self):
        return self._event["detail"].get(self.event_name_in_detail, "")

    def _select_parameters(self, event_name, task):

        # allows triggering by ec2 events
        if task[handlers.TASK_SERVICE] != "rds":
            return {}

        r = self._get_db_resource_id()
        return {r[1]: r[2]}

    def _get_db_resource_id(self):
        request_parameters = self._event["detail"].get("requestParameters", {})
        if "dBInstanceIdentifier" in request_parameters:
            return "db", "DBInstanceIdentifier", request_parameters.get("dBInstanceIdentifier", None)
        else:
            return "cluster", "DBClusterIdentifier", request_parameters.get("dBClusterIdentifier", None)

    def _source_resource_tags(self, session, task):

        if self.event_name_in_detail not in [RDS_INSTANCE_STARTED, RDS_INSTANCE_STOPPED]:
            raise NotImplemented

        rds = get_client_with_retries("rds", methods=["ListTagsForResource"], context=self._context, region=self._event_region(),
                                      session=session, logger=self._logger)
        r = self._get_db_resource_id()
        arn = "arn:aws:rds:{}:{}:{}:{}".format(self._event_region(), self._event_account(), r[0], r[2])
        resp = rds.list_tags_for_resource_with_retries(ResourceName=arn)
        return {t["Key"]: t["Value"] for t in resp.get("TagList", [])}
