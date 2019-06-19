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


import json
import os
import traceback
from datetime import datetime

import boto3

import actions
import boto_retry
import handlers
import handlers.task_tracking_table as tracking
from boto_retry import add_retry_methods_to_resource
from handlers.task_tracking_table import TaskTrackingTable
from main import lambda_handler
from util import safe_dict, safe_json
from util.logger import Logger

ACTIVE_INSTANCES = "InstanceCount"
CONCURRENCY_ID = tracking.TASK_TR_CONCURRENCY_ID

NEW_TASK = 0
FINISHED_CONCURRENY_TASK = 1
CHECK_COMPLETION = 2

DEBUG_ACTION = "Action is \"{}\" for task \"{}\", task-id is {}"
DEBUG_DRYRUN = "Action will be executed in in dry-run mode"
DEBUG_LAMBDA = "Lambda function invoked {}"
DEBUG_ACTION_PARAMETERS = "Action parameters are {}"

INFO_NUMBER_OF_EXECUTING = "{} action item{} dispatched for execution"
INFO_NUMBER_OF_WAIING = "{} action item{} put in waiting state"
INFO_RESULT = "Handling actions tracking update took {:>.3f} seconds"
INFO_MEMORY_SIZE = "Task memory size for lambda is {} MB"
INFO_LAMBDA_FUNCTION_ = "Executing action with Lambda function {}, payload is {}"
INFO_START_WAITING = "Waiting list count for ConcurrencyKey \"{}\" is {}, action is \"{}\", starting waiting task \"{}\" with id {}"
INFO_WAITING = "The waiting list for action \"{}\" with concurrency key \"{}\" is {}, the maximum number of concurrent " \
               "running actions for this key is {}, action with id \"{}\" has been put in waiting state"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"
SCHEDULER_LAMBDA_FUNTION_DEFAULT = "SchedulerDefault"
SIZED_SCHEDULER_NAME_TEMPLATE = "Scheduler{:0>04d}"


class TaskTrackingHandler:
    """
    Class to handle events triggered by inserting new items in the actions tracking table.
    """

    def __init__(self, event, context):
        """
        Initializes the instance.
        :param event: Handled event
        :param context: Context if running in Lambda
        """
        self._context = context
        self._event = event
        self._tracking_table = None
        self._concurrency_table = None
        self.started_tasks = 0
        self.started_waiting_tasks = 0
        self.waiting_for_execution_tasks = 0
        self.started_completion_checks = 0
        self.finished_concurrency_tasks = 0
        self.done_work = False
        self.invoked_lambda_functions = []

        self.events_client = None

        # setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = Logger(logstream=logstream, context=self._context, buffersize=20, debug=False)

    @staticmethod
    def is_handling_request(event):
        """
        Tests if the event is handled by this handler class
        :param event:
        :return: True if the event is handled by this class
        """

        if "Records" not in event:
            return False

        source_arn = event["Records"][0]["eventSourceARN"]
        table_name = source_arn.split("/")[1]
        return table_name == os.getenv(handlers.ENV_ACTION_TRACKING_TABLE)

    @property
    def tracking_table(self):
        """
        Gets an instance of the tracking table and use it in subsequent calls
        :return: Instance tracking table
        """
        if self._tracking_table is None:
            self._tracking_table = TaskTrackingTable(self._context)
        return self._tracking_table

    @staticmethod
    def _get_action_concurrency_key(item):
        """
        Gets the concurrency key for a tasks action
        :param item: The task item
        :return: The concurrency key for the tasks action
        """
        action = item[tracking.TASK_TR_ACTION]
        # get the name of the optional method to return the concurrency key
        action_class = actions.get_action_class(action)
        concurrency_key_method = getattr(action_class, actions.ACTION_CONCURRERNCY_KEY_METHOD, None)

        # prepare parameters for calling static function that returns the concurrency key
        if concurrency_key_method is not None:
            get_key_params = {
                actions.ACTION_PARAM_RESOURCES: json.loads(item.get(tracking.TASK_TR_RESOURCES, "{}")),
                actions.ACTION_PARAM_ACCOUNT: item[tracking.TASK_TR_ACCOUNT],
                actions.ACTION_PARAM_STACK: os.getenv(handlers.ENV_STACK_NAME),
                actions.ACTION_PARAM_STACK_ID: os.getenv(handlers.ENV_STACK_ID),
                actions.ACTION_PARAM_ACTION_ID: item[tracking.TASK_TR_ID]
            }
            get_key_params.update(json.loads(item.get(tracking.TASK_TR_PARAMETERS)))
            return concurrency_key_method(get_key_params)
        else:
            # if this method is not available for action then use the name of the action as the key
            return action

    def _enter_waiting_list(self, concurrency_key):
        """
        Adds 1 to waiting list counter for the specified concurrency key and returns new value
        :param concurrency_key: Concurrency key for counter
        :return: Updated counter
        """
        # update/read counter for the concurrency key
        resp = self.concurrency_table.update_item_with_retries(Key={CONCURRENCY_ID: concurrency_key},
                                                               UpdateExpression="ADD InstanceCount :one",
                                                               ExpressionAttributeValues={":one": 1},
                                                               ReturnValues="UPDATED_NEW")
        return int(resp["Attributes"].get("InstanceCount", 0))

    def _leave_waiting_list(self, concurrency_key):
        """
        Subtracts 1 from waiting list counter for the specified concurrency key and returns new value. If the counter reaches 0
        then the entry for the concurrency key is removed
        :param concurrency_key: Concurrency key for counter
        :return: Updated counter
        """

        resp = self.concurrency_table.update_item_with_retries(Key={CONCURRENCY_ID: concurrency_key},
                                                               UpdateExpression="ADD InstanceCount :minus_one",
                                                               ExpressionAttributeValues={":minus_one": -1},
                                                               ReturnValues="UPDATED_NEW")

        count = max(0, int(resp["Attributes"].get(ACTIVE_INSTANCES, 0)))
        # remove entry if no more waiting items for this key
        if count == 0:
            self.concurrency_table.delete_item_with_retries(Key={CONCURRENCY_ID: concurrency_key})

        return count

    @property
    def concurrency_table(self):
        """
        Returns table to store last execution time for this handler.
        :return: table to store last execution time for this handler
        """
        if self._concurrency_table is None:
            tablename = os.getenv(handlers.ENV_CONCURRENCY_TABLE)

            self._logger.debug("Using concurrency table {}", tablename)

            self._concurrency_table = boto3.Session().resource("dynamodb").Table(tablename)
            add_retry_methods_to_resource(self._concurrency_table, ["update_item", "delete_item"], context=self._context)

        return self._concurrency_table

    def _is_waitlisted(self, item):
        """
        Test if there is a max concurrency level for the tasks action. If this is the case then a concurrency key is retrieved
        from the action and it is used to update the counter in the concurrency table for that key. The updated counter is tested
        against the max concurrency level for the tasks action
        :param item: task item
        :return: True if counter for tasks action concurrency key > mac concurrency level, False if it is less or equal or the
        action has no max concurrency level
        """

        action = item[tracking.TASK_TR_ACTION]
        action_properies = actions.get_action_properties(action)

        # test if there are concurrency restrictions
        max_action_concurrency = action_properies.get(actions.ACTION_MAX_CONCURRENCY)
        if max_action_concurrency is None:
            return False

        # get the key for the tasks action
        concurrency_key = TaskTrackingHandler._get_action_concurrency_key(item)
        # enter the waiting list for that key
        count = self._enter_waiting_list(concurrency_key)

        # set status to waiting if count > max concurrency level
        status = tracking.STATUS_WAITING if count > max_action_concurrency else None

        # store the concurrency key twice, the concurrency id is used for the index in the GSI and is removed after the
        # action is handled so it does not longer show in the GSI, but we keep another  copy in the task tracking table that
        # we need to decrement the counter in the waiting list and possible start waiting instances with the same key
        self.tracking_table.update_action(item[tracking.TASK_TR_ID], status, {
            tracking.TASK_TR_CONCURRENCY_KEY: concurrency_key,
            tracking.TASK_TR_CONCURRENCY_ID: concurrency_key
        })

        if count > max_action_concurrency:
            self._logger.info(INFO_WAITING, item[tracking.TASK_TR_ACTION], concurrency_key, count, max_action_concurrency,
                              item[tracking.TASK_TR_ID])
            return True

        return False

    def _start_task_execution(self, task_item, action=handlers.HANDLER_ACTION_EXECUTE):
        """
        Creates an instance of the lambda function that executes the tasks action. It first checks is the action has specific memory
        requirements and based on this it creates a copy of this instance or one configured for the required memory. All
        information for executing the action is passed in the event.
        :param task_item: Task item for which action is executed
        :return:
        """

        try:

            # get the action for the task
            action_properties = actions.get_action_properties(task_item[tracking.TASK_TR_ACTION])

            # check if there are specific memory requirements
            action_memory_size = action_properties.get(actions.ACTION_MEMORY, None)
            if action_memory_size is not None:
                self._logger.info(INFO_MEMORY_SIZE, action_memory_size)

            # Create event for execution of the action and set its action so that is picked up by the execution handler
            event = {i: task_item.get(i) for i in task_item}
            event[handlers.HANDLER_EVENT_ACTION] = action

            self._logger.debug(DEBUG_ACTION, task_item[tracking.TASK_TR_ACTION],
                               task_item[tracking.TASK_TR_NAME],
                               task_item[tracking.TASK_TR_ID])
            self._logger.debug(DEBUG_ACTION_PARAMETERS, safe_json(task_item.get(tracking.TASK_TR_PARAMETERS, {})))

            if self._context is not None:
                # if running in a Lambda environment the action will be executed asynchronously in a new instance of a
                # lambda function. This gives each individual action 5 minutes to execute and allows parallel execution of
                # multiple actions

                # create event payload
                payload = str.encode(safe_json(event))
                lambda_name = self._context.function_name

                # based on the memory requirements determine the lambda function to use
                if action_memory_size is not None and action_memory_size != actions.LAMBDA_DEFAULT_MEMORY:
                    lambda_name = lambda_name.replace(SCHEDULER_LAMBDA_FUNTION_DEFAULT,
                                                      SIZED_SCHEDULER_NAME_TEMPLATE.format(action_memory_size))

                self._logger.info(INFO_LAMBDA_FUNCTION_, lambda_name, payload)
                # start lambda function
                lambda_client = boto_retry.get_client_with_retries("lambda", ["invoke"], context=self._context)
                resp = lambda_client.invoke_with_retries(FunctionName=lambda_name,
                                                         InvocationType="Event",
                                                         LogType="None",
                                                         Payload=payload)

                task_info = {
                    "id": task_item[tracking.TASK_TR_ID],
                    "task": task_item[tracking.TASK_TR_NAME],
                    "action": task_item[tracking.TASK_TR_ACTION],
                    "payload": payload,
                    "status-code": resp["StatusCode"]
                }

                self._logger.info(DEBUG_LAMBDA, safe_json(task_info, indent=2))
                self.invoked_lambda_functions.append(task_info)

            else:
                # if not running in Lambda, for debugging purposes, the event is passed to the main handler
                # that created and instance of the execution handler to execute the action. Note tha execution of actions in
                # this scenario are serialized.
                lambda_handler(event, None)

        except Exception as ex:
            self._logger.error("Error running task {}, {}, {}", task_item, str(ex), traceback.format_exc())

    def _handle_new_task_item(self, task_item):
        """
        Handles stream updates for new tasks added to the task tracking table
        :param task_item:
        :return:
        """
        self._logger.debug("Handling new task logic")
        # tasks can be wait listed if there is a max concurrency level for its action
        if self._is_waitlisted(task_item):
            self.waiting_for_execution_tasks += 1
            return

        # if not wait listed start the action for the task
        self.started_tasks += 1
        self._start_task_execution(task_item)

    def _handle_completed_concurrency_item(self, task_item):
        """
        Handles stream updated for tasks that have finished (completed or failed) and that have a concurrency key
        :param task_item: Task item
        :return:
        """

        self._logger.debug("Handling completed concurrency logic")
        # gets the concurrency key for the task
        concurrency_key = task_item[tracking.TASK_TR_CONCURRENCY_KEY]
        self._logger.debug("Handling completed task with ConcurrencyKey {}", concurrency_key)

        # use the concurrency key to decrement the counter for that key in the waiting list
        count = self._leave_waiting_list(concurrency_key)
        self._logger.debug("Concurrency count for ConcurrencyKey {} is {}", concurrency_key, count)

        self.finished_concurrency_tasks += 1

        if count == 0:
            return

        waiting_list = self.tracking_table.get_waiting_tasks(concurrency_key)
        self._logger.debug("List of waiting tasks for ConcurrencyKey {} is {}", concurrency_key, waiting_list)
        if len(waiting_list) > 0:
            oldest_waiting_task = sorted(waiting_list, key=lambda w: w[tracking.TASK_TR_CREATED_TS])[0]
            self._logger.info(INFO_START_WAITING, concurrency_key, count, task_item[tracking.TASK_TR_ACTION],
                              oldest_waiting_task[tracking.TASK_TR_NAME], oldest_waiting_task[tracking.TASK_TR_ID])
            self.started_waiting_tasks += 1
            self._start_task_execution(oldest_waiting_task)

    def _handle_check_completion(self, task_item):
        self._logger.debug("Handling test for completion logic")
        self.started_completion_checks += 1
        self._start_task_execution(task_item=task_item, action=handlers.HANDLER_ACTION_TEST_COMPLETION)

    def handle_request(self):
        """
        Handles the event triggered by updates to the actions tracking table.
        :return: results of handling selected updates
        """

        def tasks_items_to_execute():
            """
            Generator function that selects all record items from the event that need processing.
            :return:
            """

            def is_new_action(task_record):
                if task_record["eventName"] == "INSERT":
                    return task_record["dynamodb"]["NewImage"].get(tracking.TASK_TR_STATUS).get("S")
                return False

            def is_completed_with_concurrency(task_record):
                if task_record["eventName"] in ["UPDATE", "MODIFY"]:
                    new_task_item = task_record["dynamodb"]["NewImage"]
                    concurrency_key = new_task_item.get(tracking.TASK_TR_CONCURRENCY_KEY, {}).get("S")
                    status = new_task_item.get(tracking.TASK_TR_STATUS, {}).get("S")
                    return concurrency_key is not None and status in [tracking.STATUS_COMPLETED, tracking.STATUS_FAILED,
                                                                      tracking.STATUS_TIMED_OUT]

            def is_wait_for_completion(task_record):
                if task_record["eventName"] in ["UPDATE", "MODIFY"]:
                    old_task_item = task_record["dynamodb"]["OldImage"]
                    old_status = old_task_item.get(tracking.TASK_TR_STATUS, {}).get("S")
                    old_wait_ts = old_task_item.get(tracking.TASK_TR_LAST_WAIT_COMPLETION, {}).get("S")
                    new_task_item = task_record["dynamodb"]["NewImage"]
                    new_status = new_task_item.get(tracking.TASK_TR_STATUS, {}).get("S")
                    new_wait_ts = new_task_item.get(tracking.TASK_TR_LAST_WAIT_COMPLETION, {}).get("S")
                    return old_status == tracking.STATUS_WAIT_FOR_COMPLETION and new_status == tracking.STATUS_WAIT_FOR_COMPLETION \
                           and old_wait_ts is not None and old_wait_ts != new_wait_ts

            for record in self._event.get("Records"):

                self._logger.debug("Record to process is {}", safe_json(record, indent=2))

                if record.get("eventSource") == "aws:dynamodb":
                    if record["eventName"] == "REMOVE":
                        continue

                    update_to_handle = None
                    self._logger.debug_enabled = record["dynamodb"]["NewImage"].get(tracking.TASK_TR_DEBUG, {}).get("BOOL", False)
                    if is_new_action(record):
                        update_to_handle = NEW_TASK
                    elif is_completed_with_concurrency(record):
                        update_to_handle = FINISHED_CONCURRENY_TASK
                    elif is_wait_for_completion(record):
                        update_to_handle = CHECK_COMPLETION

                    if update_to_handle is not None:
                        yield update_to_handle, record
                    else:
                        self._logger.debug("No action for this record")

        try:
            start = datetime.now()

            self._logger.info("Handler {}", self.__class__.__name__)

            for task_tracking_update_type, task_tracking_record in tasks_items_to_execute():

                self.done_work = True

                new_image = task_tracking_record["dynamodb"]["NewImage"]
                task_item = {i: new_image[i][new_image[i].keys()[0]] for i in new_image}

                self._logger.debug_enabled = task_item.get(tracking.TASK_TR_DEBUG, False)

                if task_tracking_update_type == NEW_TASK:
                    self._handle_new_task_item(task_item)
                elif task_tracking_update_type == FINISHED_CONCURRENY_TASK:
                    self._handle_completed_concurrency_item(task_item)
                elif task_tracking_update_type == CHECK_COMPLETION:
                    self._handle_check_completion(task_item)

            else:
                if not self.done_work:
                    self._logger.clear()

            running_time = float((datetime.now() - start).total_seconds())
            if self.done_work:
                self._logger.info(INFO_RESULT, running_time)

            return safe_dict({
                "datetime": datetime.now().isoformat(),
                "waiting-for-execution": self.waiting_for_execution_tasks,
                "started-check-for-completion": self.started_completion_checks,
                "started-execution": self.started_tasks,
                "started-waiting": self.started_waiting_tasks,
                "completed-concurrency-tasks": self.finished_concurrency_tasks,
                "running-time": running_time
            })

        finally:
            self._logger.flush()
