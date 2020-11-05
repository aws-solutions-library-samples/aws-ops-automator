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
from datetime import datetime

import actions
import handlers
from metrics.task_metrics import TaskMetrics

ENV_METRICS_URL = "METRICS_URL"
ENV_SOLUTION_ID = "SOLUTION_ID"
ENV_SEND_METRICS = "SEND_METRICS"

METRICS_STATUS_NAMES = {
    handlers.STATUS_PENDING: "Submitted",
    handlers.STATUS_STARTED: "Executing",
    handlers.STATUS_WAIT_FOR_COMPLETION: "Waiting to complete",
    handlers.STATUS_COMPLETED: "Completed",
    handlers.STATUS_TIMED_OUT: "Timed out",
    handlers.STATUS_FAILED: "Failed",
    handlers.STATUS_WAITING: "Waiting for execution"
}


def put_task_state_metrics(task_name, metric_state_name, task_level, count=1, logger=None, data=None, context=None, ):
    """
    Store task metrics.

    Args:
        task_name: (str): write your description
        metric_state_name: (str): write your description
        task_level: (todo): write your description
        count: (int): write your description
        logger: (todo): write your description
        data: (array): write your description
        context: (todo): write your description
    """
    with TaskMetrics(datetime.utcnow(), context=context, logger=logger) as metrics:
        metrics.put_task_state_metrics(task_name=task_name,
                                       metric_state_name=metric_state_name,
                                       task_level=task_level,
                                       count=count,
                                       data=data)


def put_task_select_data(task_name, items, selected_items, selection_time, logger=None, context=None):
    """
    Put selected items in the queue

    Args:
        task_name: (str): write your description
        items: (todo): write your description
        selected_items: (str): write your description
        selection_time: (str): write your description
        logger: (todo): write your description
        context: (todo): write your description
    """
    with TaskMetrics(datetime.utcnow(), logger=logger, context=context) as metrics:
        metrics.put_task_select_data(task_name=task_name, items=items, selected_items=selected_items, selection_time=selection_time)


def put_general_errors_and_warnings(error_count=0, warning_count=0, logger=None, context=None):
    """
    This function to general general error.

    Args:
        error_count: (todo): write your description
        warning_count: (int): write your description
        logger: (todo): write your description
        context: (todo): write your description
    """
    with TaskMetrics(datetime.utcnow(), logger=logger, context=context) as metrics:
        metrics.put_general_errors_and_warnings(error_count=error_count, warning_count=warning_count)


def setup_tasks_metrics(task, action_name, task_level_metrics, logger=None, context=None):
    """
    Setup all the metrics for the given task.

    Args:
        task: (todo): write your description
        action_name: (str): write your description
        task_level_metrics: (todo): write your description
        logger: (todo): write your description
        context: (dict): write your description
    """
    with TaskMetrics(dt=datetime.utcnow(), logger=logger, context=context)as metrics:

        task_class = actions.get_action_class(action_name)

        # number of submitted task instances for task
        metrics.put_task_state_metrics(task_name=task,
                                       metric_state_name=METRICS_STATUS_NAMES[handlers.STATUS_PENDING],
                                       count=0,
                                       task_level=task_level_metrics)

        # init metrics for results
        for s in [handlers.STATUS_STARTED, handlers.STATUS_COMPLETED, handlers.STATUS_FAILED]:
            metrics.put_task_state_metrics(task_name=task,
                                           metric_state_name=METRICS_STATUS_NAMES[s],
                                           count=0,
                                           task_level=task_level_metrics)

        # init metrics for tasks with completion handling
        if getattr(task_class, handlers.COMPLETION_METHOD, None) is not None:
            metrics.put_task_state_metrics(task_name=task,
                                           metric_state_name=METRICS_STATUS_NAMES[handlers.STATUS_WAIT_FOR_COMPLETION],
                                           count=0,
                                           task_level=task_level_metrics)
            metrics.put_task_state_metrics(task_name=task,
                                           metric_state_name=METRICS_STATUS_NAMES[handlers.STATUS_TIMED_OUT],
                                           count=0,
                                           task_level=task_level_metrics)

        # init metrics for tasks with concurrency handling
        if getattr(task_class, handlers.ACTION_CONCURRENCY_KEY_METHOD, None) is not None:
            metrics.put_task_state_metrics(task_name=task,
                                           metric_state_name=METRICS_STATUS_NAMES[handlers.STATUS_WAITING],
                                           count=0,
                                           task_level=task_level_metrics)
