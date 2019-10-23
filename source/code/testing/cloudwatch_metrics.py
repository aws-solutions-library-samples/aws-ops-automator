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
from datetime import datetime, timedelta

import boto3
import boto3.exceptions

from helpers.timer import Timer

ONE_DAY = 24 * 3600
MEGA_BYTE = 1024 * 1024


class CloudwatchMetrics(object):

    def __init__(self, region=None, session=None):
        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.metrics_client = self.session.client("cloudwatch", region_name=self.region)

    def get_daily_volume_iops(self, volume_id, days):
        query_data = [
            {
                "Id": "rw",
                "Expression": "r+w",
            },
            {

                "Id": "r",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeReadOps",
                        "Dimensions": [
                            {
                                "Name": "VolumeId",
                                "Value": volume_id
                            }
                        ]
                    },
                    "Period": 24 * 3600,
                    "Stat": "Sum"
                },
                "ReturnData": False
            },
            {

                "Id": "w",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EBS",
                        "MetricName": "VolumeWriteOps",
                        "Dimensions": [
                            {
                                "Name": "VolumeId",
                                "Value": volume_id
                            }
                        ]
                    },
                    "Period": 24 * 3600,
                    "Stat": "Sum"
                },
                "ReturnData": False
            }

        ]
        metrics_data = self.metrics_client.get_metric_data(
            MetricDataQueries=query_data,
            StartTime=datetime.now() - timedelta(days=days),
            EndTime=datetime.now()).get("MetricDataResults", [])

        return metrics_data[0]["Values"]

    def get_daily_cpu_utilization(self, instance_id, days):
        query_data = [

            {
                "Id": "cpu",
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
                    "Period": ONE_DAY,
                    "Stat": "Average"
                },
                "ReturnData": True
            }

        ]
        metrics_data = self.metrics_client.get_metric_data(
            MetricDataQueries=query_data,
            StartTime=datetime.now() - timedelta(days=days),
            EndTime=datetime.now()).get("MetricDataResults", [])

        return metrics_data[0]["Values"]

    def get_daily_network_io(self, instance_id, days):
        query_data = [
            {
                "Id": "io",
                "Expression": "in+out"
            },
            {

                "Id": "in",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EC2",
                        "MetricName": "NetworkIn",
                        "Dimensions": [
                            {
                                "Name": "InstanceId",
                                "Value": instance_id
                            }
                        ]
                    },
                    "Period": ONE_DAY,
                    "Stat": "Sum"
                },
                "ReturnData": False
            },
            {

                "Id": "out",
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/EC2",
                        "MetricName": "NetworkOut",
                        "Dimensions": [
                            {
                                "Name": "InstanceId",
                                "Value": instance_id
                            }
                        ]
                    },
                    "Period": ONE_DAY,
                    "Stat": "Sum"
                },
                "ReturnData": False
            }
        ]

        metrics_data = self.metrics_client.get_metric_data(
            MetricDataQueries=query_data,
            StartTime=datetime.now() - timedelta(days=days),
            EndTime=datetime.now()).get("MetricDataResults", [])

        return metrics_data[0]["Values"]

    def wait_for_volume_iops(self, volume_id, timeout, min_iops):
        with Timer(timeout, start=True) as t:
            while not t.timeout:
                iops = self.get_daily_volume_iops(volume_id, 1)
                if len(iops) > 0 and iops[0] >= min_iops:
                    return True
                time.sleep(15)

        return False

    def wait_for_cpu_load(self, instance_id, timeout, load):
        with Timer(timeout, start=True) as t:
            while not t.timeout:
                cpu = self.get_daily_cpu_utilization(instance_id, 1)
                if len(cpu) > 0 and cpu[0] >= load:
                    return True
                time.sleep(15)

        return False

    def wait_for_network_io(self, instance_id, timeout, io_mb):
        with Timer(timeout, start=True) as t:
            while not t.timeout:
                io = self.get_daily_network_io(instance_id, 1)
                if len(io) > 0 and io[0] >= io_mb * MEGA_BYTE:
                    return True
                time.sleep(15)

        return False

    def get_daily_database_connections(self, db_instance_id, days):
        query_data = [

            {
                "Id": db_instance_id.replace("-", ""),
                "MetricStat": {
                    "Metric": {
                        "Namespace": "AWS/RDS",
                        "MetricName": "DatabaseConnections",
                        "Dimensions": [
                            {
                                "Name": "DBInstanceIdentifier",
                                "Value": db_instance_id
                            }
                        ]
                    },
                    "Period": 24 * 3600,
                    "Stat": "Sum",
                    "Unit": "Count"
                },
                "ReturnData": True
            }
        ]

        args = {
            "MetricDataQueries": query_data,
            "StartTime": datetime.now() - timedelta(days=days),
            "EndTime": datetime.now()
        }

        return self.metrics_client.get_metric_data(**args).get("MetricDataResults", [{}])[0].get("Values", [])

    def wait_for_db_connections(self, db_instance_id, timeout):
        with Timer(timeout_seconds=timeout, start=True) as t:
            while not t.timeout:
                c = self.get_daily_database_connections(db_instance_id, 1)
                if len(c) > 0 and c[0] > 0:
                    return True
                time.sleep(15)
            return False
