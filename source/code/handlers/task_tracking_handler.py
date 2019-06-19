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
import types
from datetime import datetime

import actions
import boto_retry
import handlers
import handlers.task_tracking_table
import services
from handlers.task_tracking_table import TaskTrackingTable
from helpers import safe_dict, safe_json, full_stack
from helpers.dynamodb import unpack_record
from main import lambda_handler
from outputs.queued_logger import QueuedLogger
from outputs.result_notifications import ResultNotifications

ACTIVE_INSTANCES = "InstanceCount"
CONCURRENCY_ID = handlers.TASK_TR_CONCURRENCY_ID

ENV_DEBUG_TASK_TACKING_HANDLER = "DEBUG_TASK_TRACKING_HANDLER"

NEW_TASK = 0
FINISHED_TASK = 1
FINISHED_CONCURRENCY_TASK = 2
CHECK_COMPLETION = 3
DELETE_ITEM = 4
START_WAITING_ACTION = 5

TASK_ACTION_STRINGS = [
    "New task",
    "Finished task",
    "Finished task with concurrency handling",
    "Check task completion",
    "Delete task item",
    "Start waiting task"
]

WARN_DELETING_RESOURCES = "Error deleting resources from bucket {} with key {}"

DEBUG_ACTION = "Action is \"{}\" for task \"{}\", task-id is {}"
DEBUG_DRYRUN = "Action will be executed in in dry-run mode"
DEBUG_LAMBDA = "Lambda function invoked {}"
DEBUG_ACTION_PARAMETERS = "Action parameters are {}"
DEBUG_RUNNING_ECS_TASK = "Running {} step of task {} as ECS job"
DEBUG_RESULT = "Handling actions tracking update took {:>.3f} seconds"
DEBUG_MEMORY_SIZE = "Task memory allocation for executing lambda is {}"
DEBUG_LAMBDA_FUNCTION_ = "Executing action with Lambda function {}, payload is {}"
DEBUG_START_WAITING = "Waiting list count for ConcurrencyId \"{}\" is {}, action is \"{}\", starting waiting " \
                      "task \"{}\" with id {}"
DEBUG_WAITING = "The waiting list for action \"{}\" with concurrency key \"{}\" is {}, the maximum number of concurrent " \
                "running actions for this key is {}, action with id \"{}\" has been put in waiting state"

DEBUG_DELETING_RESOURCES_FROM_S3 = "Deleting resource object {} from bucket {}, {}"
ERR_RUNNING_TASK = "Error running task {}, {}, {}"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"
SCHEDULER_LAMBDA_FUNCTION_DEFAULT = "SchedulerDefault"
SIZED_SCHEDULER_NAME_TEMPLATE = "Scheduler{:0>04d}"


class TaskTrackingHandler(object):
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
        self._s3_client = None
        self._db_client = None

        # setup logging
        classname = self.__class__.__name__
        dt = datetime.utcnow()
        logstream = LOG_STREAM.format(classname, dt.year, dt.month, dt.day)
        self._logger = QueuedLogger(logstream=logstream,
                                    context=self._context,
                                    buffersize=20,
                                    debug=os.getenv(ENV_DEBUG_TASK_TACKING_HANDLER, "false").lower() == "true")

    @classmethod
    def is_handling_request(cls, event, context):

        # In simulation the handler is called directly when inserting or updating items in the table
        if handlers.running_local(context):
            return False

        if event.get("Records", [{}])[0].get("eventSource", "") != "aws:dynamodb":
            return False

        source_arn = event["Records"][0]["eventSourceARN"]
        table_name = source_arn.split("/")[1]
        return table_name in [os.getenv(handlers.ENV_ACTION_TRACKING_TABLE), os.getenv(handlers.ENV_CONCURRENCY_TABLE)]

    @classmethod
    def task_string(cls, action):
        return TASK_ACTION_STRINGS[action] if 0 <= action < len(TASK_ACTION_STRINGS) else "Unknown"

    @property
    def tracking_table(self):
        """
        Gets an instance of the tracking table and use it in subsequent calls
        :return: Instance tracking table
        """
        if self._tracking_table is None:
            self._tracking_table = TaskTrackingTable(self._context, self._logger)
        return self._tracking_table

    @property
    def s3_client(self):

        if self._s3_client is None:
            self._s3_client = boto_retry.get_client_with_retries("s3", ["delete_item"], logger=self._logger)
        return self._s3_client

    @property
    def db_client(self):

        if self._db_client is None:
            self._db_client = boto_retry.get_client_with_retries("dynamodb", ["delete_item"], logger=self._logger)
        return self._db_client

    def _get_action_concurrency_key(self, item):
        """
        Gets the concurrency key for a tasks action
        :param item: The task item
        :return: The concurrency key for the tasks action
        """
        action = item[handlers.TASK_TR_ACTION]
        # get the name of the optional method to return the concurrency key
        action_class = actions.get_action_class(action)
        concurrency_key_method = getattr(action_class, handlers.ACTION_CONCURRENCY_KEY_METHOD, None)

        # prepare parameters for calling static function that returns the concurrency key
        if concurrency_key_method is not None:
            get_key_params = {
                actions.ACTION_PARAM_RESOURCES: handlers.get_item_resource_data(item, self._context),
                actions.ACTION_PARAM_ACCOUNT: item[handlers.TASK_TR_ACCOUNT],
                actions.ACTION_PARAM_STACK: os.getenv(handlers.ENV_STACK_NAME),
                actions.ACTION_PARAM_STACK_ID: os.getenv(handlers.ENV_STACK_ID),
                actions.ACTION_PARAM_TASK_ID: item[handlers.TASK_TR_ID],
                actions.ACTION_PARAM_TASK: item[handlers.TASK_TR_NAME]
            }
            get_key_params.update(item.get(handlers.TASK_TR_PARAMETERS))
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
        if not handlers.running_local(self._context):
            resp = self.concurrency_table.update_item_with_retries(Key={CONCURRENCY_ID: concurrency_key},
                                                                   UpdateExpression="ADD InstanceCount :one SET RunNext=:run",
                                                                   ExpressionAttributeValues={":one": 1, ":run": False},
                                                                   ReturnValues="UPDATED_NEW")
            return int(resp["Attributes"].get("InstanceCount", 0))
        else:
            resp = self.concurrency_table.get_item_with_retries(Key={CONCURRENCY_ID: concurrency_key})
            return resp.get("Item", {}).get(ACTIVE_INSTANCES, 0)

    def _leave_waiting_list(self, task_id, concurrency_key):
        """
        Subtracts 1 from waiting list counter for the specified concurrency key and returns new value. If the counter reaches 0
        then the entry for the concurrency key is removed
        :param concurrency_key: Concurrency key for counter
        :return: Updated counter
        """

        # make a consistent read of the task
        self.tracking_table.get_task_item(task_id)

        if not handlers.running_local(self._context):

            resp = self.concurrency_table.update_item_with_retries(Key={CONCURRENCY_ID: concurrency_key},
                                                                   UpdateExpression="ADD InstanceCount :min_one SET RunNext=:run",
                                                                   ExpressionAttributeValues={":min_one": -1, ":run": True},
                                                                   ReturnValues="UPDATED_NEW")
            count = max(0, int(resp["Attributes"].get(ACTIVE_INSTANCES, 0)))

            # remove entry if no more waiting items for this key
            if count == 0:
                self.concurrency_table.delete_item_with_retries(Key={CONCURRENCY_ID: concurrency_key})
        else:
            resp = self.concurrency_table.get_item_with_retries(Key={CONCURRENCY_ID: concurrency_key})
            count = resp.get("Item", {}).get(ACTIVE_INSTANCES, 0)
            TaskTrackingTable._run_local_stream_event(os.getenv(handlers.ENV_CONCURRENCY_TABLE), "UPDATE",
                                                      {"ConcurrencyId": concurrency_key, "InstanceCount": count},
                                                      {"ConcurrencyId": concurrency_key, "InstanceCount": count + 1},
                                                      self._context)

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

            self._concurrency_table = services.get_session().resource("dynamodb").Table(tablename)
            boto_retry.add_retry_methods_to_resource(self._concurrency_table, ["update_item", "get_item", "delete_item"],
                                                     context=self._context)

        return self._concurrency_table

    def _is_wait_listed(self, item):
        """
        Test if there is a max concurrency level for the tasks action. If this is the case then a concurrency key is retrieved
        from the action and it is used to update the counter in the concurrency table for that key. The updated counter is tested
        against the max concurrency level for the tasks action
        :param item: task item
        :return: True if counter for tasks action concurrency key > mac concurrency level, False if it is less or equal or the
        action has no max concurrency level
        """

        action = item.get(handlers.TASK_TR_ACTION, None)
        if action is None:
            return False

        action_properties = actions.get_action_properties(action)

        # test if there are concurrency restrictions
        max_action_concurrency = action_properties.get(actions.ACTION_MAX_CONCURRENCY)

        # no maximum
        if max_action_concurrency in [None, 0]:
            return False

        # property may be a lambda function, call the function with parameters of task as lambda parameters
        if types.FunctionType == type(max_action_concurrency):
            parameters = item[handlers.TASK_TR_PARAMETERS]
            max_action_concurrency = max_action_concurrency(parameters)
            if max_action_concurrency in [None, 0]:
                return False

        # get the key for the tasks action
        concurrency_key = self._get_action_concurrency_key(item)
        # enter the waiting list for that key
        count = int(self._enter_waiting_list(concurrency_key))

        # set status to waiting if count > max concurrency level
        status = handlers.STATUS_WAITING if count >= int(max_action_concurrency) else None

        # store the concurrency key twice, the concurrency id is used for the index in the GSI and is removed after the
        # action is handled so it does not longer show in the GSI, but we keep another  copy in the task tracking table that
        # we need to decrement the counter in the waiting list and possible start waiting instances with the same key
        self.tracking_table.update_task(item[handlers.TASK_TR_ID],
                                        task=item[handlers.TASK_TR_NAME],
                                        task_metrics=item.get(handlers.TASK_TR_METRICS, False),
                                        status=status,
                                        status_data={
                                            handlers.TASK_TR_CONCURRENCY_KEY: concurrency_key,
                                            handlers.TASK_TR_CONCURRENCY_ID: concurrency_key
                                        })

        if count > max_action_concurrency:
            self._logger.debug(DEBUG_WAITING, item[handlers.TASK_TR_ACTION], concurrency_key, count,
                               max_action_concurrency, item[handlers.TASK_TR_ID])
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

            self._logger.debug("Entering start_task_execution ({}) with task {}", action, safe_json(task_item, indent=3))

            # Create event for execution of the action and set its action so that is picked up by the execution handler
            event = {i: task_item.get(i) for i in task_item}
            event[handlers.HANDLER_EVENT_ACTION] = action

            self._logger.debug(DEBUG_ACTION, task_item[handlers.TASK_TR_ACTION],
                               task_item[handlers.TASK_TR_NAME],
                               task_item[handlers.TASK_TR_ID])

            self._logger.debug(DEBUG_ACTION_PARAMETERS,
                               safe_json(task_item.get(handlers.TASK_TR_PARAMETERS, {}), indent=3))

            # get memory allocation for executing the task
            lambda_size = handlers.TASK_TR_COMPLETION_SIZE \
                if action == handlers.HANDLER_ACTION_TEST_COMPLETION \
                else handlers.TASK_TR_EXECUTE_SIZE

            execute_lambda_size = task_item.get(lambda_size, actions.ACTION_SIZE_STANDARD)

            if execute_lambda_size == actions.ACTION_USE_ECS:
                ecs_memory = task_item.get(handlers.TASK_EXECUTE_ECS_MEMORY
                                           if action == handlers.HANDLER_ACTION_EXECUTE
                                           else handlers.TASK_COMPLETION_ECS_MEMORY, None)
            else:
                ecs_memory = None

            if not handlers.running_local(self._context):

                self._logger.debug(DEBUG_MEMORY_SIZE, execute_lambda_size)

                if execute_lambda_size != actions.ACTION_USE_ECS:

                    # create event payload
                    payload = str.encode(safe_json(event))

                    # determine which lambda to execute on
                    function_name = "{}-{}-{}".format(os.getenv(handlers.ENV_STACK_NAME),
                                                     os.getenv(handlers.ENV_LAMBDA_NAME),
                                                     execute_lambda_size)

                    self._logger.debug("Running execution of task on lambda function {}", function_name)

                    self._logger.debug(DEBUG_LAMBDA_FUNCTION_, function_name, payload)
                    # start lambda function
                    lambda_client = boto_retry.get_client_with_retries("lambda", ["invoke"], context=self._context,
                                                                       logger=self._logger)
                    resp = lambda_client.invoke_with_retries(FunctionName=function_name,
                                                             InvocationType="Event",
                                                             LogType="None",
                                                             Payload=payload)

                    task_info = {
                        "id": task_item[handlers.TASK_TR_ID],
                        "task": task_item[handlers.TASK_TR_NAME],
                        "action": task_item[handlers.TASK_TR_ACTION],
                        "payload": payload,
                        "status-code": resp["StatusCode"]
                    }

                    self._logger.debug(DEBUG_LAMBDA, safe_json(task_info, indent=2))
                    self.invoked_lambda_functions.append(task_info)
                else:
                    # run as ECS job
                    ecs_args = {
                        handlers.HANDLER_EVENT_ACTION: action,
                        handlers.TASK_NAME: task_item[handlers.TASK_TR_NAME],
                        handlers.TASK_TR_ID: task_item[handlers.TASK_TR_ID]}

                    self._logger.debug(DEBUG_RUNNING_ECS_TASK, action, task_item[handlers.TASK_TR_NAME])
                    handlers.run_as_ecs_job(ecs_args, ecs_memory_size=ecs_memory, context=self._context, logger=self._logger)

            else:
                lambda_handler(event, self._context)

            ResultNotifications(context=self._context, logger=self._logger).publish_started(task_item)

        except Exception as ex:
            self._logger.error(ERR_RUNNING_TASK, task_item, str(ex), full_stack())

    def _handle_new_task_item(self, task_item):
        """
        Handles stream updates for new tasks added to the task tracking table
        :param task_item:
        :return:
        """
        self._logger.debug("Handling new task logic")
        # tasks can be wait listed if there is a max concurrency level for its action
        if self._is_wait_listed(task_item):
            self.waiting_for_execution_tasks += 1
            return

        # if not wait listed start the action for the task
        self.started_tasks += 1
        self._start_task_execution(task_item)

    def _handle_completed_concurrency_item(self, task_item):

        self._logger.debug("Handling completed concurrency logic")
        # gets the concurrency key for the task
        concurrency_key = task_item[handlers.TASK_TR_CONCURRENCY_KEY]
        self._logger.debug("Handling completed task with ConcurrencyKey {}", concurrency_key)

        # use the concurrency key to decrement the counter for that key in the waiting list
        count = self._leave_waiting_list(task_item[handlers.TASK_TR_ID], concurrency_key)
        self._logger.debug("Concurrency count for ConcurrencyKey {} is {}", concurrency_key, count)

        self.finished_concurrency_tasks += 1

        ResultNotifications(context=self._context, logger=self._logger).publish_ended(task_item)

    def _handle_finished_task_without_completion(self, task_item):
        ResultNotifications(context=self._context, logger=self._logger).publish_ended(task_item)

    def _handle_start_waiting_action(self, concurrency_item):

        self._logger.debug("Handling start waiting task logic")
        # gets the concurrency key for the task
        concurrency_id = concurrency_item[handlers.TASK_TR_CONCURRENCY_ID]
        self._logger.debug("Handling completed task with ConcurrencyId {}", concurrency_id)

        waiting_list = self.tracking_table.get_waiting_tasks(concurrency_id)

        self._logger.debug(" List of waiting tasks for ConcurrencyKey {} is {}", concurrency_id, safe_json(waiting_list, indent=3))
        if len(waiting_list) > 0:
            count = concurrency_item.get(ACTIVE_INSTANCES, 0)
            oldest_waiting_task = sorted(waiting_list, key=lambda w: w[handlers.TASK_TR_CREATED_TS])[0]
            self._logger.debug(DEBUG_START_WAITING, concurrency_id, count,
                               oldest_waiting_task[handlers.TASK_TR_ACTION],
                               oldest_waiting_task[handlers.TASK_TR_NAME],
                               oldest_waiting_task[handlers.TASK_TR_ID])
            self.started_waiting_tasks += 1
            self._start_task_execution(oldest_waiting_task)

    def _handle_check_completion(self, task_item):
        self._logger.debug("Handling test for completion logic")
        if task_item.get(handlers.TASK_TR_RUN_LOCAL, False) and not handlers.running_local(self._context):
            self._logger.debug("Item running in local mode skipped")
            return

        self.started_completion_checks += 1
        self._start_task_execution(task_item=task_item, action=handlers.HANDLER_ACTION_TEST_COMPLETION)

    def _handle_deleted_item(self, task_item):

        if task_item.get(handlers.TASK_TR_S3_RESOURCES, False):

            bucket = os.getenv(handlers.ENV_RESOURCE_BUCKET)
            key = task_item[handlers.TASK_TR_ID] + ".json"
            try:
                self._logger.debug(DEBUG_DELETING_RESOURCES_FROM_S3, bucket, key)
                self.s3_client.delete_object_with_retries(Bucket=bucket, Key=key)
            except Exception as ex:
                self._logger.warning(WARN_DELETING_RESOURCES, bucket, key, ex)

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

            def table_name(rec):
                source_arn = rec["eventSourceARN"]
                return source_arn.split("/")[1]

            def from_tracking_table(rec):
                return table_name(rec) == os.getenv(handlers.ENV_ACTION_TRACKING_TABLE)

            def from_concurrency_table(rec):
                return table_name(rec) == os.getenv(handlers.ENV_CONCURRENCY_TABLE)

            def get_old_image(task_record):
                return task_record["dynamodb"].get("OldImage", {})

            def get_new_image(task_record):
                return task_record["dynamodb"].get("NewImage", {})

            def get_new_status(task_record):
                return get_new_image(task_record).get(handlers.TASK_TR_STATUS, {}).get("S")

            def get_old_status(task_record):
                return get_new_image(task_record).get(handlers.TASK_TR_STATUS, {}).get("S")

            def is_task_tracking_table_update(task_record):
                if not from_tracking_table(task_record):
                    return False
                return task_record["eventName"] in ["UPDATE", "MODIFY"]

            def is_task_done(task_record):

                if not is_task_tracking_table_update(task_record):
                    return False

                new_status = get_new_status(task_record)
                old_status = get_old_status(task_record)

                if old_status != new_status:
                    return False
                return new_status in handlers.task_tracking_table.NOT_LONGER_ACTIVE_STATUSES

            def is_task_with_concurrency(task_record):
                return get_new_image(task_record).get(handlers.TASK_TR_CONCURRENCY_KEY, {}).get("S") is not None

            def get_old_last_update(task_record):
                return get_old_image(task_record).get(handlers.TASK_TR_LAST_WAIT_COMPLETION, {}).get("S")

            def get_new_last_update(task_record):
                return get_new_image(task_record).get(handlers.TASK_TR_LAST_WAIT_COMPLETION, {}).get("S")

            def is_delete_task(task_record):
                return from_tracking_table(r) and task_record["eventName"] == "REMOVE"

            def is_new_task(task_record):
                if from_tracking_table(r) and task_record["eventName"] == "INSERT":
                    return get_new_status(task_record) == handlers.STATUS_PENDING
                return False

            def is_completed_with_concurrency(task_record):
                return is_task_done(task_record) and is_task_with_concurrency(task_record)

            def is_completed_without_concurrency(task_record):

                return is_task_done(task_record) and not is_task_with_concurrency(task_record)

            def is_wait_for_completion(task_record):

                if not is_task_tracking_table_update(task_record):
                    return False

                if get_old_status(task_record) != handlers.STATUS_WAIT_FOR_COMPLETION or \
                        get_new_status(task_record) != handlers.STATUS_WAIT_FOR_COMPLETION:
                    return False

                return get_old_last_update(task_record) != get_new_last_update(task_record)

            def is_concurrency_task_completed(concurrency_record):
                if not from_concurrency_table(concurrency_record):
                    return False

                if concurrency_record["eventName"] == "REMOVE":
                    return False

                return concurrency_record["dynamodb"].get("NewImage", {}).get("RunNext", {}).get("BOOL", False)

            def get_action_type(rec):

                if is_new_task(rec):
                    return NEW_TASK

                if is_completed_without_concurrency(rec):
                    return FINISHED_TASK

                if is_completed_with_concurrency(rec):
                    return FINISHED_CONCURRENCY_TASK

                if is_wait_for_completion(rec):
                    return CHECK_COMPLETION

                if is_delete_task(rec):
                    return DELETE_ITEM

                if is_concurrency_task_completed(rec):
                    return START_WAITING_ACTION

                return None

            for r in self._event.get("Records"):

                self._logger.debug("Record to process is {}", safe_json(r, indent=2))

                if r.get("eventSource") == "aws:dynamodb":

                    image_used = "NewImage" if "NewImage" in r["dynamodb"] else "OldImage"

                    if r["dynamodb"].get("NewImage", {}).get(handlers.TASK_TR_ACTION) is None and \
                            r["dynamodb"].get("OldImage", {}).get(handlers.TASK_TR_ACTION) is not None:
                        continue

                    self._logger.debug_enabled = r["dynamodb"][image_used].get(handlers.TASK_TR_DEBUG, {}).get("BOOL", False)

                    update_to_handle = get_action_type(r)

                    if update_to_handle is not None:
                        yield update_to_handle, r
                    else:
                        self._logger.debug("No action for record")

        try:

            start = datetime.now()

            task_handlers = [
                self._handle_new_task_item,
                self._handle_finished_task_without_completion,
                self._handle_completed_concurrency_item,
                self._handle_check_completion,
                self._handle_deleted_item,
                self._handle_start_waiting_action
            ]

            for task_tracking_update_type, record in tasks_items_to_execute():
                self.done_work = True

                used_image = "OldImage" if record["eventName"] == "REMOVE" else "NewImage"
                image = record["dynamodb"][used_image]
                handled_item = unpack_record(image)
                self._logger.debug_enabled = handled_item.get(handlers.TASK_TR_DEBUG, False)

                self._logger.debug("Executing handler function {} for type {} ({})",
                                   task_handlers[task_tracking_update_type].__name__, self.task_string(task_tracking_update_type),
                                   task_tracking_update_type)
                task_handlers[task_tracking_update_type](handled_item)

            if not self.done_work:
                self._logger.clear()

            running_time = float((datetime.now() - start).total_seconds())
            if self.done_work:
                self._logger.debug(DEBUG_RESULT, running_time)

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
