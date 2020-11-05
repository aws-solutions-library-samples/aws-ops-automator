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
import time
from datetime import timedelta

import dateutil.parser
from botocore.exceptions import ClientError

import actions
import handlers.ebs_snapshot_event_handler
import pytz
import services.ec2_service
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from outputs import raise_value_error

GROUP_TITLE_DELETE_OPTIONS = "Snapshot delete options. Choose either Retention Count or Retention Days, but not both"

PARAM_DESC_RETENTION_COUNT = "Number of snapshots to keep for a volume. Set to 0 to use Retention Days instead."
PARAM_DESC_RETENTION_DAYS = "Snapshot retention period (in days). Set to 0 to use Retention Count instead"

PARAM_LABEL_RETENTION_COUNT = "Retention Count"
PARAM_LABEL_RETENTION_DAYS = "Retention Days"

INF_ACCOUNT_SNAPSHOTS = "Processing set of {} snapshots for account {} in region {}"
INF_SNAPSHOTS_FOR_VOLUME = "Processing snapshots {} for volume {}"
INF_KEEP_RETENTION_COUNT = "Retaining latest {} snapshots for each Ec2 volume"
INF_REGION = "Deleting snapshots in region {}"
INF_RETENTION_DAYS = "Deleting Ec2 snapshots older than {}"
INF_SNAPSHOT_DELETED = "Deleted {}snapshot {} for volume {}"
INF_SNAPSHOT_NOT_FOUND = "Snapshot \"{}\" was not found and could not be deleted"
INF_NO_LONGER_AVAILABLE = "Snapshot {} is not longer available"

WARN_NO_SOURCE_VOLUME_WITH_RETENTION = "Original volume can not be retrieved for snapshot {}, original volume is required for " \
                                       "use with Retention count parameter not equal to 0, snapshot skipped"

DEBUG_SN_DELETE_RETENTION_COUNT = "Deleting snapshot {} ({}), because count for its volume {} is {}"
DEBUG_SN_RETENTION_DAYS_DELETE = "Deleting snapshot {} ({}) for volume {} because it is older than retention period of {} days"
DEBUG_SN_RETENTION_DAYS_KEEP = "Keeping snapshot {} ({}) for volume {} because it is created before {}"
DEBUG_SN_KEEP_RETENTION_COUNT = "Keeping snapshot {} ({}) because it's count for volume {} is {}"

ERR_RETENTION_PARAM_BOTH = "Only one of {} or {} parameters can be specified"
ERR_RETENTION_PARAM_NONE = "{} or {} parameter must be specified"

PARAM_RETENTION_DAYS = "RetentionDays"
PARAM_RETENTION_COUNT = "RetentionCount"


class Ec2DeleteSnapshotAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Delete Snapshot",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Deletes EC2 snapshots after retention period or count",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "8034f6b8-de65-4b8a-ad31-8ac8d13a532f",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.SNAPSHOTS,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_ALLOW_TAGFILTER_WILDCARD: False,

        ACTION_SELECT_EXPRESSION: "Snapshots[?State=='completed'].{SnapshotId:SnapshotId, VolumeId:VolumeId, StartTime:StartTime,"
                                  "Tags:Tags}",
        ACTION_KEEP_RESOURCE_TAGS: True,

        ACTION_SELECT_PARAMETERS: {'OwnerIds': ["self"]},

        ACTION_EVENTS: {
            handlers.EC2_EVENT_SOURCE: {
                handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_NOTIFICATION: [
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_FOR_VOLUME_CREATED,
                    handlers.ebs_snapshot_event_handler.EBS_SNAPSHOT_FOR_VOLUME_COPIED
                ]
            }
        },

        ACTION_SELECT_SIZE: [ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_MEDIUM,
                              ACTION_SIZE_LARGE,
                              ACTION_SIZE_XLARGE,
                              ACTION_SIZE_XXLARGE,
                              ACTION_SIZE_XXXLARGE] + [ACTION_USE_ECS],
        ACTION_COMPLETION_SIZE: [ACTION_SIZE_MEDIUM],

        ACTION_MIN_INTERVAL_MIN: 15,

        ACTION_PARAMETERS: {
            PARAM_RETENTION_DAYS: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_DAYS,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_LABEL: PARAM_LABEL_RETENTION_DAYS
            },
            PARAM_RETENTION_COUNT: {
                PARAM_DESCRIPTION: PARAM_DESC_RETENTION_COUNT,
                PARAM_TYPE: type(0),
                PARAM_REQUIRED: False,
                PARAM_MIN_VALUE: 0,
                PARAM_MAX_VALUE: 1000,
                PARAM_LABEL: PARAM_LABEL_RETENTION_COUNT
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_DELETE_OPTIONS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_RETENTION_DAYS,
                    PARAM_RETENTION_COUNT

                ],
            }],

        ACTION_PERMISSIONS: [
            "ec2:DescribeSnapshots",
            "ec2:DeleteSnapshot"
        ]

    }

    # noinspection PyUnusedLocal
    @staticmethod
    def custom_aggregation(resources, parameters, logger):
        """
        Yields snapshots of snapshots.

        Args:
            resources: (dict): write your description
            parameters: (dict): write your description
            logger: (todo): write your description
        """
        if parameters.get(PARAM_RETENTION_COUNT, 0) > 0:
            snapshots_sorted_by_volumeid = sorted(resources, key=lambda k: k['VolumeId'])
            volume = snapshots_sorted_by_volumeid[0]["VolumeId"] if len(snapshots_sorted_by_volumeid) > 0 else None
            snapshots_for_volume = []
            for snapshot in snapshots_sorted_by_volumeid:
                if volume != snapshot["VolumeId"]:
                    yield snapshots_for_volume
                    volume = snapshot["VolumeId"]
                    snapshots_for_volume = [snapshot]
                else:
                    snapshots_for_volume.append(snapshot)
            yield snapshots_for_volume
        else:
            snapshots = []
            for s in resources:
                snapshots.append(s)
                if len(snapshots) >= 800:
                    yield snapshots
                    snapshots = []
            if len(snapshots) > 0:
                yield snapshots

    # noinspection PyUnusedLocal
    @staticmethod
    def process_and_select_resource(service, logger, resource_name, resource, context, task, task_assumed_role):
        """
        Processes a single resource and return an instance

        Args:
            service: (str): write your description
            logger: (todo): write your description
            resource_name: (str): write your description
            resource: (dict): write your description
            context: (todo): write your description
            task: (dict): write your description
            task_assumed_role: (todo): write your description
        """
        volume_id = resource["VolumeId"]
        if volume_id == actions.DUMMY_VOLUME_IF_FOR_COPIED_SNAPSHOT:
            volume_from_tag = resource.get("Tags", {}).get(actions.marker_snapshot_tag_source_source_volume_id(), None)
            if volume_from_tag is not None:
                resource["VolumeId"] = volume_from_tag
                resource["IsCopied"] = True
            else:
                if task.get("parameters", {}).get(PARAM_RETENTION_COUNT, 0) > 0:
                    logger.warning(WARN_NO_SOURCE_VOLUME_WITH_RETENTION, resource["SnapshotId"])
                    return None
        return resource

    # noinspection PyUnusedLocal
    @staticmethod
    def action_validate_parameters(parameters, task_settings, logger):
        """
        Validate that the parameters are valid.

        Args:
            parameters: (dict): write your description
            task_settings: (todo): write your description
            logger: (todo): write your description
        """

        retention_days = parameters.get(PARAM_RETENTION_DAYS)
        retention_count = parameters.get(PARAM_RETENTION_COUNT)
        if not retention_count and not retention_days:
            raise_value_error(ERR_RETENTION_PARAM_NONE, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        if retention_days and retention_count:
            raise_value_error(ERR_RETENTION_PARAM_BOTH, PARAM_RETENTION_COUNT, PARAM_RETENTION_DAYS)

        return parameters

    def __init__(self, action_arguments, action_parameters):
        """
        Initialize the state

        Args:
            self: (dict): write your description
            action_arguments: (str): write your description
            action_parameters: (todo): write your description
        """

        ActionBase.__init__(self, action_arguments, action_parameters)
        
        #self.snapshots = sorted(self._resources_)
        self.snapshots = sorted(self._resources_, key=lambda snap: snap["Region"])
        self.retention_days = int(self.get(PARAM_RETENTION_DAYS))
        self.retention_count = int(self.get(PARAM_RETENTION_COUNT))

        self.dryrun = self.get(ACTION_PARAM_DRYRUN, False)

        self._ec2_client = None

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_
        }

    @property
    def ec2_client(self):
        """
        Return ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec2 ec

        Args:
            self: (todo): write your description
        """
        if self._ec2_client is None:
            self._ec2_client = get_client_with_retries("ec2",
                                                       methods=[
                                                           "delete_snapshot"
                                                       ],
                                                       region=self._region_,
                                                       context=self._context_,
                                                       session=self._session_,
                                                       logger=self._logger_)
        return self._ec2_client

    @staticmethod
    def action_logging_subject(arguments, _):
        """
        Logging actions for the given arguments

        Args:
            arguments: (dict): write your description
            _: (todo): write your description
        """
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        retention_count = int(arguments["event"][ACTION_PARAMETERS].get(PARAM_RETENTION_COUNT, 0))
        if retention_count == 0:
            if len(arguments.get(ACTION_PARAM_EVENT, {})) > 0:
                volumes = list(set([s["VolumeId"] for s in arguments.get(ACTION_PARAM_RESOURCES, [])]))
                if len(volumes) == 1:
                    return "{}-{}-{}-{}".format(account, region, volumes[0], log_stream_date())
                else:
                    return "{}-{}-{}-{}".format(account, region, arguments[ACTION_ID], log_stream_date())
            else:
                return "{}-{}-{}".format(account, region, log_stream_date())
        else:
            return "{}-{}-{}-{}".format(account,
                                        region,
                                        arguments[ACTION_PARAM_RESOURCES][0].get("VolumeId", ""),
                                        log_stream_date())

    def execute(self):
        """
        Executes all snapshots.

        Args:
            self: (todo): write your description
        """

        def get_start_time(sn):
            """
            Get the start time of the start datetime object.

            Args:
                sn: (todo): write your description
            """
            if isinstance(sn["StartTime"], datetime):
                return sn["StartTime"]
            return dateutil.parser.parse(sn["StartTime"])

        def snapshots_to_delete():
            """
            Delete snapshots that all snapshots

            Args:
            """

            def by_retention_days():
                """
                Yield retention days

                Args:
                """

                delete_before_dt = self._datetime_.utcnow().replace(tzinfo=pytz.timezone("UTC")) - timedelta(
                    days=int(self.retention_days))
                self._logger_.info(INF_RETENTION_DAYS, delete_before_dt)

                for sn in sorted(self.snapshots, key=lambda snap: snap["Region"]):
                    snapshot_dt = get_start_time(sn)
                    if snapshot_dt < delete_before_dt:
                        
                        self._logger_.debug(DEBUG_SN_RETENTION_DAYS_DELETE, sn["SnapshotId"], get_start_time(sn),
                                            sn["VolumeId"], self.retention_days)
                        yield sn
                    else:
                        self._logger_.debug(DEBUG_SN_RETENTION_DAYS_KEEP, sn, s, ["VolumeId"],
                                            get_start_time(sn), delete_before_dt.isoformat())

            def by_retention_count():
                """
                Return a generator over all retention snapshots

                Args:
                """

                self._logger_.info(INF_KEEP_RETENTION_COUNT, self.retention_count)

                sorted_snapshots = sorted(self.snapshots,
                                          key=lambda snap: (snap["VolumeId"], snap["StartTime"]),
                                          reverse=True)
                volume = None
                count_for_volume = 0
                for sn in sorted_snapshots:
                    if sn["VolumeId"] != volume:
                        volume = sn["VolumeId"]
                        count_for_volume = 0

                    count_for_volume += 1
                    if count_for_volume > self.retention_count:
                        self._logger_.debug(DEBUG_SN_DELETE_RETENTION_COUNT,
                                            sn["SnapshotId"],
                                            sn["StartTime"],
                                            sn["VolumeId"],
                                            count_for_volume)
                        yield sn
                    else:
                        self._logger_.debug(DEBUG_SN_KEEP_RETENTION_COUNT,
                                            sn["SnapshotId"],
                                            sn["StartTime"],
                                            sn["VolumeId"],
                                            count_for_volume)

            return list(by_retention_days()) if self.retention_days != 0 else list(by_retention_count())

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        deleted_count = 0

        self._logger_.debug("Snapshots : {}", self.snapshots)

        snapshot_id = ""

        self._logger_.info(INF_SNAPSHOTS_FOR_VOLUME,
                           ",".join(["{} ({})".format(s["SnapshotId"], s["StartTime"]) for s in self.snapshots]),
                           self.snapshots[0].get("VolumeId", ""))

        ec2 = services.create_service("ec2", session=self._session_,
                                      service_retry_strategy=get_default_retry_strategy("ec2", context=self._context_))

        for deleted_snapshot in snapshots_to_delete():

            if self.time_out():
                break

            if "deleted" not in self.result:
                self.result["deleted"] = {}
                self.result["deleted"][self._region_] = []

            try:
                snapshot_id = deleted_snapshot["SnapshotId"]
                if ec2.get(services.ec2_service.SNAPSHOTS,
                           region=self._region_,
                           SnapshotIds=[snapshot_id]) is None:
                    self._logger_.info(INF_NO_LONGER_AVAILABLE, snapshot_id)
                else:
                    self.ec2_client.delete_snapshot_with_retries(DryRun=self.dryrun,
                                                                 SnapshotId=snapshot_id,
                                                                 _expected_boto3_exceptions_=["InvalidSnapshot.NotFound"])
                    time.sleep(0.2)
                    deleted_count += 1
                    copied = deleted_snapshot.get("IsCopied", False)
                    self._logger_.info(INF_SNAPSHOT_DELETED, "copied " if copied else "", snapshot_id, deleted_snapshot["VolumeId"])
                    self.result["deleted"][self._region_].append(snapshot_id)
            except ClientError as ex_client:
                if ex_client.response.get("Error", {}).get("Code", "") == "InvalidSnapshot.NotFound":
                    self._logger_.info(INF_SNAPSHOT_NOT_FOUND, snapshot_id)
                else:
                    raise ex_client
            except Exception as ex:
                if self.dryrun:
                    self._logger_.debug(str(ex))
                    self.result["delete_snapshot"] = str(ex)
                    return self.result
                else:
                    raise ex

        self.result.update({
            "snapshots": len(self.snapshots),
            "snapshots-deleted": deleted_count,
            METRICS_DATA: build_action_metrics(self, DeletedSnapshots=deleted_count)

        })

        return self.result
