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

from datetime import timedelta

import dateutil.parser

import actions
import pytz
import services.ec2_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries
from configuration import CONFIG_INTERVAL
from outputs import raise_value_error
from scheduling.cron_expression import CronExpression

DAY_IN_MINUTES = 24 * 60

ERR_INTERVAL_TOO_LONG = "Interval expression must be set so that task executes with a max interval of 1 day"
ERR_MISSING_INTERVAL = "Interval parameter {} must be set"
ERR_PARAM_HIGH_LOW = "Value of parameter {} must be higher than value of parameter {}"
ERR_TAG_BOTH_EMPTY = "Parameter {} and {} cannot both be empty"
ERR_INTERVAL_BETWEEN_TASK_TO_SHORT = "Interval {} between task executions must be at least {} minutes"

INF_ACTION_START = "Checking utilisation of EC2 instances for account {} in region {} for task {}"
INF_TAG_HIGH = "Tagging overutilized instance(s) {} with tags {}"
INF_TAG_LOW = "Tagging underutilized instance(s) {} with tags {}"
INF_SKIP_INSTANCE_TOO_NEW = "Skipping instance {} as insufficient metrics data is available since launch at {} "
INF_SKIP_NO_METRICS = "Skipping instance {} as no metrics data was available"
INF_CPU_DATA = "CPU utilisation for instance {} is {}%"
INF_NO_METRICS_DATA = "No metrics data for instance {}"
INF_METRICS_PERIOD = "Metrics data collection period is from {} to {}"

PARAM_CPU_HIGH_TAGS = "CpuHighTags"
PARAM_CPU_LOW_TAGS = "CpuLowTags"
PARAM_CPU_PERC_HIGH = "CpuHigh"
PARAM_CPU_PERC_LOW = "CpuLow"

PARAM_DEC_CPU_HIGH_TAGS = "List of tag names and values, in the format Key=Value, to set on instances with average CPU load " \
                          "above threshold."
PARAM_DESC_CPU_LOW_TAGS = "List of tag names and values, in the format Key=Value, to set on instances with average CPU load " \
                          "below threshold."
PARAM_DESC_CPU_PERC_HIGH = "CPU High threshold in %. If the average CPU load for an instance, over the running interval is " \
                           "above this threshold, the Instance will be tagged."
PARAM_DESC_CPU_PERC_LOW = "CPU Low threshold in %. If the average CPU load for an instance, over the running interval is below " \
                          "this threshold, the Instance will be tagged."

PARAM_LABEL_CPU_HIGH_TAGS = "CPU high tags"
PARAM_LABEL_CPU_LOW_TAGS = "CPU low tags"
PARAM_LABEL_CPU_PERC = "CPU threshold high"
PARAM_LABEL_CPU_PERC_LOW = "CPU threshold low"
PARAM_LABEL_THRESHOLD_DAYS = "Max low utilization days"

GROUP_TITLE_CPU_THRESHOLDS = "CPU utilization thresholds and tagging"

DEFAULT_LOW_CPU = 10
DEFAULT_HIGH_CPU = 80


class Ec2TagCpuInstanceAction(ActionBase):
    properties = {
        ACTION_TITLE: "EC2 Tag Instance by CPU Utilisation",
        ACTION_VERSION: "1.0",
        ACTION_DESCRIPTION: "Tags EC2 instances that do have an average CPU load above or below specified thresholds",
        ACTION_AUTHOR: "AWS",
        ACTION_ID: "77939277-96c1-40ef-80a9-4f4239516314",

        ACTION_SERVICE: "ec2",
        ACTION_RESOURCES: services.ec2_service.INSTANCES,
        ACTION_AGGREGATION: ACTION_AGGREGATION_REGION,

        ACTION_SELECT_EXPRESSION: "Reservations[*].Instances[].{InstanceId:InstanceId, LaunchTime:LaunchTime, Tags:Tags,"
                                  "State:State.Name}|[?State=='running']",

        ACTION_MIN_INTERVAL_MIN: 5,

        ACTION_SELECT_SIZE: [ACTION_SIZE_STANDARD,
                             ACTION_SIZE_MEDIUM,
                             ACTION_SIZE_LARGE,
                             ACTION_SIZE_XLARGE,
                             ACTION_SIZE_XXLARGE,
                             ACTION_SIZE_XXXLARGE
                             ] + [ACTION_USE_ECS],
        ACTION_EXECUTE_SIZE: [ACTION_SIZE_STANDARD],

        ACTION_PARAMETERS: {

            PARAM_CPU_PERC_HIGH: {
                PARAM_DESCRIPTION: PARAM_DESC_CPU_PERC_HIGH,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: 99,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: str(DEFAULT_HIGH_CPU),
                PARAM_LABEL: PARAM_LABEL_CPU_PERC
            },
            PARAM_CPU_PERC_LOW: {
                PARAM_DESCRIPTION: PARAM_DESC_CPU_PERC_LOW,
                PARAM_TYPE: int,
                PARAM_MIN_VALUE: 1,
                PARAM_MAX_VALUE: 99,
                PARAM_REQUIRED: True,
                PARAM_DEFAULT: str(DEFAULT_LOW_CPU),
                PARAM_LABEL: PARAM_LABEL_CPU_PERC_LOW
            },
            PARAM_CPU_HIGH_TAGS: {
                PARAM_DESCRIPTION: PARAM_DEC_CPU_HIGH_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_CPU_HIGH_TAGS
            },
            PARAM_CPU_LOW_TAGS: {
                PARAM_DESCRIPTION: PARAM_DESC_CPU_LOW_TAGS,
                PARAM_TYPE: str,
                PARAM_REQUIRED: False,
                PARAM_LABEL: PARAM_LABEL_CPU_LOW_TAGS
            }
        },

        ACTION_PARAMETER_GROUPS: [
            {
                ACTION_PARAMETER_GROUP_TITLE: GROUP_TITLE_CPU_THRESHOLDS,
                ACTION_PARAMETER_GROUP_LIST: [
                    PARAM_CPU_PERC_HIGH,
                    PARAM_CPU_HIGH_TAGS,
                    PARAM_CPU_PERC_LOW,
                    PARAM_CPU_LOW_TAGS
                ]
            }

        ],

        ACTION_PERMISSIONS: ["cloudwatch:GetMetricData",
                             "ec2:DescribeInstances",
                             "ec2:CreateTags",
                             "ec2:DeleteTags"],

    }

    @staticmethod
    def action_validate_parameters(parameters, task_settings, __):
        high = int(parameters.get(PARAM_CPU_PERC_HIGH))
        low = int(parameters.get(PARAM_CPU_PERC_LOW))
        if high <= low:
            raise_value_error(ERR_PARAM_HIGH_LOW.format(PARAM_CPU_PERC_HIGH, PARAM_CPU_PERC_LOW))

        cpu_high_tags = parameters.get(PARAM_CPU_HIGH_TAGS)
        cpu_low_tags = parameters.get(PARAM_CPU_LOW_TAGS)
        if cpu_high_tags is None and cpu_low_tags is None:
            raise_value_error(ERR_TAG_BOTH_EMPTY.format(PARAM_CPU_LOW_TAGS, PARAM_CPU_HIGH_TAGS))

        interval_cron = task_settings.get(CONFIG_INTERVAL, None)
        if interval_cron is None:
            raise_value_error(ERR_MISSING_INTERVAL.format(actions.ACTION_PARAM_INTERVAL))

        e = CronExpression(interval_cron)
        last = None
        for i in e.within_next(timespan=timedelta(days=365),
                               start_dt=date_time_provider().now().replace(hour=0,
                                                                           minute=0,
                                                                           second=0,
                                                                           microsecond=0)):
            if last is not None:
                between = i - last
                if between > timedelta(minutes=DAY_IN_MINUTES):
                    raise_value_error(ERR_INTERVAL_TOO_LONG)
            last = i
        return parameters

    @staticmethod
    def action_logging_subject(arguments, _):
        account = arguments[ACTION_PARAM_RESOURCES][0]["AwsAccount"]
        region = arguments[ACTION_PARAM_RESOURCES][0]["Region"]
        return "{}-{}-{}".format(account, region, log_stream_date())

    @property
    def ec2_client(self):
        if self._ec2_client is None:
            methods = ["create_tags",
                       "delete_tags"]

            self._ec2_client = get_client_with_retries("ec2", methods, region=self._region_,
                                                       session=self._session_, logger=self._logger_)
        return self._ec2_client

    @property
    def metrics_client(self):
        if self._metrics_client is None:
            methods = ["get_metric_data"]
            self._metrics_client = get_client_with_retries("cloudwatch", methods, region=self._region_,
                                                           session=self._session_, logger=self._logger_)
        return self._metrics_client

    def __init__(self, action_arguments, action_parameters):

        ActionBase.__init__(self, action_arguments, action_parameters)

        self.instances = self._resources_

        self._ec2_client = None
        self._metrics_client = None

        self.cpu_high = int(self.get(PARAM_CPU_PERC_HIGH))
        self.cpu_low = int(self.get(PARAM_CPU_PERC_LOW))

        self.cpu_high_tags = self.get(PARAM_CPU_HIGH_TAGS)
        self.cpu_low_tags = self.get(PARAM_CPU_LOW_TAGS)

        self.interval = self.get(actions.ACTION_PARAM_INTERVAL)

        e = CronExpression(self.interval)
        previous_executions = list(e.within_last(timespan=timedelta(hours=24),
                                                 end_dt=date_time_provider().utcnow() - timedelta(minutes=1)))
        self.period_in_minutes = max(5, int(
            (previous_executions[1] - previous_executions[0]).total_seconds()) / 60)

        self.under_utilized_instances = []
        self.over_utilized_instances = []

        self.result = {
            "account": self._account_,
            "region": self._region_,
            "task": self._task_,
            "instances-checked": len(self.instances),
        }

    def _get_meta_data_queries(self, start):
        query_data = []

        for instance in self.instances:
            launch_time = instance["LaunchTime"]
            if not isinstance(launch_time, datetime):
                launch_time = dateutil.parser.parse(launch_time)
            launch_time = launch_time.replace(tzinfo=pytz.utc)
            instance_id = instance["InstanceId"]
            if launch_time > start:
                self._logger_.info(INF_SKIP_INSTANCE_TOO_NEW, instance_id, instance["LaunchTime"])
                continue
            i = instance_id.replace("-", "")

            query_data += [
                {
                    "Id": "cpu{}".format(i),
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "CPUUtilization",
                            "Dimensions": [
                                {
                                    "Name": "InstanceId",
                                    "Value": instance_id
                                }
                            ]
                        },
                        "Period": self.period_in_minutes * 60,
                        "Stat": "Average"
                    },
                    "ReturnData": True
                }]

            if len(query_data) < 96:
                continue

            yield query_data
            query_data = []

        if len(query_data) > 0:
            yield query_data

    def _collect_instances_metric_data(self):

        end = self._datetime_.utcnow()
        end = end.replace(second=0, microsecond=0, tzinfo=pytz.utc) - timedelta(minutes=5)
        start = end - (timedelta(minutes=self.period_in_minutes))

        self._logger_.info(INF_METRICS_PERIOD, start.isoformat(), end.isoformat())

        for queries in self._get_meta_data_queries(start):
            args = {
                "MetricDataQueries": queries,
                "StartTime": start,
                "EndTime": end
            }

            while True:
                resp = self.metrics_client.get_metric_data_with_retries(**args)
                metrics_data = resp.get("MetricDataResults", [])

                for metrics_item in metrics_data:

                    inst_id = "i-{}".format(metrics_item["Id"][4:])
                    if len(metrics_item["Values"]) == 0:
                        self._logger_.info(INF_SKIP_NO_METRICS, inst_id)
                        continue

                    average_cpu = metrics_item.get("Values")

                    if average_cpu is None:
                        self._logger_.info(INF_NO_METRICS_DATA, inst_id)
                        continue

                    self._logger_.info(INF_CPU_DATA, inst_id, average_cpu[0])

                    if average_cpu[0] < self.cpu_low:
                        self.under_utilized_instances.append(inst_id)
                        continue

                    if average_cpu[0] > self.cpu_high:
                        self.over_utilized_instances.append(inst_id)
                if "NextToken" not in resp:
                    break

                args["NextToken"] = resp["NextToken"]

    def _tag_instances(self):

        if len(self.over_utilized_instances) and self.get(PARAM_CPU_HIGH_TAGS) is not None:
            high_tags = self.build_tags_from_template(parameter_name=PARAM_CPU_HIGH_TAGS)

            self._logger_.info(INF_TAG_HIGH, ",".join(self.over_utilized_instances), high_tags)
            tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                 resource_ids=self.over_utilized_instances,
                                 tags=high_tags,
                                 logger=self._logger_)

        if len(self.under_utilized_instances) and self.get(PARAM_CPU_LOW_TAGS) is not None:
            low_tags = self.build_tags_from_template(parameter_name=PARAM_CPU_LOW_TAGS)

            self._logger_.info(INF_TAG_LOW, ",".join(self.under_utilized_instances), low_tags)
            tagging.set_ec2_tags(ec2_client=self.ec2_client,
                                 resource_ids=self.under_utilized_instances,
                                 tags=low_tags,
                                 logger=self._logger_)

    def execute(self):

        self._logger_.info("{}, version {}", self.properties[ACTION_TITLE], self.properties[ACTION_VERSION])

        self._logger_.info(INF_ACTION_START, self._account_, self._region_, self._task_)

        self._collect_instances_metric_data()

        self._tag_instances()

        self.result["underutilized-instances"] = self.under_utilized_instances
        self.result["overutilized-instances"] = self.over_utilized_instances

        self.result[METRICS_DATA] = build_action_metrics(
            action=self,
            CheckedInstances=len(self.instances),
            OverUtilizedInstances=len(self.over_utilized_instances),
            UnderUtilizedInstances=len(self.under_utilized_instances))

        return self.result
