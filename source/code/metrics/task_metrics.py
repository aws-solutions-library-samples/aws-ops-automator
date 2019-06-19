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
import os
from datetime import datetime

import boto_retry
import handlers
from helpers import safe_json


class TaskMetrics(object):
    """
    Implements wrapper to write metrics data
    """
    NAMESPACE = "OpsAutomator"

    METRIC_COMPLETED = "Completed"
    METRIC_TASKS_STARTED = "Started"
    METRIC_WAITING_FOR_COMPLETION = "Waiting for completion"
    METRIC_WAITING_TO_EXECUTE = "Waiting for execution"
    METRIC_FAILED = "Failed"
    METRIC_TIMED_OUT = "Timed out"
    METRIC_EXECUTING = "Executing"
    METRIC_TIME_TO_COMPLETE = "Time to complete"
    METRIC_RESOURCES = "Found resources"
    METRIC_SELECTED_RESOURCES = "Selected resources"
    METRIC_TIME_TO_SELECT = "Time to select"
    METRIC_ERRORS = "Errors"
    METRIC_WARNINGS = "Warnings"

    def __init__(self, dt=None, logger=None, context=None):
        """
        Initializes instance of metrics wrapper
        :param dt: date and time of the metrics data (typically the scheduling moment)
        """
        self._dt = dt if dt is not None else datetime.utcnow()
        self._metrics = []
        self._context = context
        self._logger = logger
        self._stack = os.getenv(handlers.ENV_STACK_NAME)
        self._stack_level = os.getenv(handlers.ENV_CLOUDWATCH_METRICS)
        self._namespace = "{}:{}".format(TaskMetrics.NAMESPACE, self._stack)

        self._metrics_client = None

    def __enter__(self):
        self.return_ = """
        Returns itself as the managed resource.
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Writes all cached  items to metrics when going out of scope
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.flush()

    def flush(self):
        if len(self._metrics) > 0 and self._stack is not None:
            if self._logger is not None:
                self._logger.debug("CloudWatch Metrics data is :\n{}", safe_json(self._metrics, indent=3))
                self._logger.debug("Putting {} CloudWatch Metrics items", len(self._metrics))
            self.metrics_client.put_metric_data_with_retries(Namespace=self._namespace, MetricData=self._metrics)
            self._metrics = []

    @property
    def metrics_client(self):
        if self._metrics_client is None:
            self._metrics_client = boto_retry.get_client_with_retries("cloudwatch", ["put_metric_data"], context=self._context,
                                                                      logger=self._logger)
        return self._metrics_client

    def put_task_select_data(self, task_name, items, selected_items, selection_time):
        self._metrics += [
            {
                # per task metrics
                "MetricName": TaskMetrics.METRIC_RESOURCES,
                "Dimensions": [{"Name": "Task", "Value": "{}:{}".format(self._stack, task_name)}],
                "Timestamp": self._dt,
                "Value": items,
                "Unit": "Count"
            },
            {
                "MetricName": TaskMetrics.METRIC_SELECTED_RESOURCES,
                "Dimensions": [{"Name": "Task", "Value": "{}:{}".format(self._stack, task_name)}],
                "Timestamp": self._dt,
                "Value": selected_items,
                "Unit": "Count"
            },
            {
                # per task metrics
                "MetricName": TaskMetrics.METRIC_TIME_TO_SELECT,
                "Dimensions": [{"Name": "Task", "Value": "{}:{}".format(self._stack, task_name)}],
                "Timestamp": self._dt,
                "Value": selection_time,
                "Unit": "Seconds"
            }]

    def put_task_state_metrics(self, task_name, metric_state_name, task_level, count=1, data=None):

        if task_level:
            self._metrics.append({
                # per task metrics
                "MetricName": metric_state_name,
                "Dimensions": [{"Name": "Task", "Value": "{}:{}".format(self._stack, task_name)}],
                "Timestamp": self._dt,
                "Value": count,
                "Unit": "Count"
            })
            if data is not None and metric_state_name == TaskMetrics.METRIC_COMPLETED:
                execution_time = data.get("ExecutionTime", None)
                if execution_time is not None:
                    self._metrics.append(
                        {
                            # per task metrics
                            "MetricName": TaskMetrics.METRIC_TIME_TO_COMPLETE,
                            "Dimensions": [{"Name": "Task", "Value": "{}:{}".format(self._stack, task_name)}],
                            "Timestamp": self._dt,
                            "Value": float(execution_time),
                            "Unit": "Seconds"
                        })

        if self._stack_level:
            self._metrics.append({
                # total for all tasks
                "MetricName": metric_state_name,
                "Dimensions": [{"Name": "Stack", "Value": self._stack}],
                "Timestamp": self._dt,
                "Value": count,
                "Unit": "Count"
            })

    def put_general_errors_and_warnings(self, error_count=0, warning_count=0):

        for i in [(TaskMetrics.METRIC_ERRORS, error_count),
                  (TaskMetrics.METRIC_WARNINGS, warning_count)]:
            self._metrics.append({

                "MetricName": i[0],
                "Dimensions": [{"Name": "Stack", "Value": self._stack}],
                "Timestamp": self._dt,
                "Value": i[1],
                "Unit": "Count"
            })
