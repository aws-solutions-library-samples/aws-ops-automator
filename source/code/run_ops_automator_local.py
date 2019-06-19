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
from __future__ import print_function

import argparse
import os
import sys
import time
from datetime import datetime

import boto3

import handlers
import handlers.task_tracking_table
import outputs.queued_logger
import services.cloudformation_service
from boto_retry import get_client_with_retries
from configuration.task_configuration import TaskConfiguration
from handlers.schedule_handler import ScheduleHandler
from helpers import safe_json
from main import lambda_handler
from services import create_service
from testing.console_logger import ConsoleLogger
from testing.context import Context

LOG_FORMAT = "# {:0>4d}-{:0>2d}-{:0>2d} - {:0>2d}:{:0>2d}:{:0>2d}.{:0>3s} : {}"

verbose = False

used_context = Context()


def print_verbose(msg, *a):
    if verbose:
        s = msg if len(a) == 0 else msg.format(*a)
        t = time.time()
        dt = datetime.fromtimestamp(t)
        s = LOG_FORMAT.format(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, str(dt.microsecond)[0:3], s)
        print(s)


def setup_parser():
    new_parser = argparse.ArgumentParser(description="Ops Automator runner")
    new_parser.add_argument("--stack", required=True, help="Name of the stack to retrieve environment variables from.")
    group = new_parser.add_mutually_exclusive_group()
    group.add_argument("--task", help="Name of the task to run.")
    group.add_argument("--complete", action="store_true", help="Run completion handler.")
    group.add_argument("--reset", action="store_true", help="Deletes log streams and task tracking table entries for stack")
    new_parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return new_parser


def setup_environment(stack_name):
    try:

        cfn = create_service("cloudformation", session=boto3.Session())

        print_verbose("Retrieving Lambda resource  \"SchedulerDefault\" from stack \"{}\"", stack_name)

        stack = cfn.get(services.cloudformation_service.STACKS, StackName=stack_name)

        lambda_name = [o["OutputValue"] for o in stack["Outputs"] if o["OutputKey"] == "OpsAutomatorLambdaFunctionStandard"][0]

        print_verbose("Lambda physical resource id is {}", lambda_name)

        lambda_service = create_service("lambda", session=boto3.Session())
        lambda_function = lambda_service.get("Function", FunctionName=lambda_name)

        environment = lambda_function["Configuration"]["Environment"]["Variables"]

        print_verbose("Lambda environment variables are:")

        for env_var in environment:
            os.environ[env_var] = environment[env_var]
            print_verbose("{}=\"{}\"", env_var, environment[env_var])

        role_resource = cfn.get("StackResource", StackName=stack_name, LogicalResourceId="OpsAutomatorLambdaRole")
        role = boto3.client("iam").get_role(RoleName=role_resource["PhysicalResourceId"]).get("Role", {})

        os.environ["ROLE_ARN"] = role["Arn"]

    except Exception as ex:
        print("error setting up environment {}".format(str(ex)))


def run_scheduler_with_rule_event():
    print_verbose("Running Scheduler by sending simulated CloudWatch rule event")

    event = {
        "source": "aws.events",
        "resources": [
            "arn:aws:events:region:000000000000:rule/{0}".format("OpsAutomatorRule-" + os.getenv(handlers.ENV_STACK_NAME))]
    }

    print_verbose("Event is {}", safe_json(event, indent=3))

    lambda_handler(event, used_context)


def run_completion_handler():
    print_verbose("Running Scheduler completion handling by sending simulated CloudWatch rule event")

    event = {
        "source": "aws.events",
        "resources": ["arn:aws:events:region:123456789012:rule/{0}".format(os.getenv(handlers.ENV_COMPLETION_RULE))]
    }

    print_verbose("Event is {}", safe_json(event, indent=3))

    lambda_handler(event, used_context)


def run_scheduler_task(task_name, stack_name):
    configuration = TaskConfiguration()
    task_item = configuration.get_config_item(task_name)

    if task_item is None:
        raise ValueError("Task \"{}\" is not configured in stack \"{}\"".format(task_name, stack_name))
    print_verbose("Configuration item is\n{}", safe_json(task_item, indent=3))

    task = TaskConfiguration(context=used_context, logger=ConsoleLogger()).configuration_item_to_task(task_item)

    event = {
        handlers.HANDLER_EVENT_ACTION: handlers.HANDLER_ACTION_SELECT_RESOURCES,
        handlers.HANDLER_EVENT_TASK: task,
        handlers.HANDLER_EVENT_SOURCE: sys.argv[0],
        handlers.HANDLER_EVENT_TASK_DT: datetime.now().isoformat()
    }

    for sub_task in ScheduleHandler.task_account_region_sub_tasks(task):
        event[handlers.HANDLER_EVENT_SUB_TASK] = sub_task

        print_verbose("Event is \n{}", safe_json(event, indent=3))

        handler = handlers.create_handler("SelectResourcesHandler", event, used_context)
        result = handler.handle_request()

        print_verbose("(Sub) Task result is\n{}", safe_json(result, indent=3))


def run_reset():
    def purge_table(table_name, key_name):
        db = get_client_with_retries("dynamodb", ["scan", "delete_item", "batch_write_item"], session=boto3.Session())

        def keys_to_delete():
            key_ids = []

            delete_args = {
                "TableName": table_name,
                "Select": "SPECIFIC_ATTRIBUTES",
                "ConsistentRead": True,
                "AttributesToGet": [key_name]
            }

            while True:
                resp = db.scan_with_retries(**delete_args)
                for item in resp.get("Items", []):
                    key_ids.append(item[key_name]["S"])

                delete_args["ExclusiveStartKey"] = resp.get("LastEvaluatedKey", None)
                if delete_args["ExclusiveStartKey"] is None:
                    break
            return key_ids

        keys = keys_to_delete()
        print("{} items to delete".format(len(keys)))

        # delete items in batches of max 25 items
        while True:
            delete_requests = []
            while len(keys) > 0:

                delete_requests.append({
                    'DeleteRequest': {
                        'Key': {
                            key_name: {
                                "S": keys.pop(0)
                            }
                        }
                    }
                })

                if len(keys) == 0 or len(delete_requests) == 25:
                    db.batch_write_item_with_retries(RequestItems={table_name: delete_requests})
                    print(".", end="")
                    time.sleep(1)
                    delete_requests = []

            time.sleep(1)
            keys = keys_to_delete()
            if len(keys) == 0:
                break

        print("")

    streams = create_service("Cloudwatchlogs", session=boto3.Session()).describe("LogStreams",
                                                                                 logGroupName=os.getenv(
                                                                                     outputs.queued_logger.ENV_LOG_GROUP))
    print("Purging table {}".format(handlers.ENV_ACTION_TRACKING_TABLE))
    purge_table(os.getenv(handlers.ENV_ACTION_TRACKING_TABLE), handlers.TASK_TR_ID)

    print("Purging table {}".format(handlers.ENV_CONCURRENCY_TABLE))
    purge_table(os.getenv(handlers.ENV_CONCURRENCY_TABLE), handlers.TASK_TR_CONCURRENCY_ID)

    cwl = get_client_with_retries("logs", ["delete_log_stream"], session=boto3.Session())
    for stream in streams:
        print("Deleting logstream {}".format(stream["logStreamName"]))
        cwl.delete_log_stream_with_retries(logGroupName=os.getenv(outputs.queued_logger.ENV_LOG_GROUP),
                                           logStreamName=stream["logStreamName"])


if __name__ == "__main__":
    parser = setup_parser()
    args = parser.parse_args(sys.argv[1:])
    verbose = args.verbose

    setup_environment(args.stack)

    DEFAULT_TIMEOUT = 900
    #
    # event = {
    # }
    # lambda_handler(event, Context())
    # exit(0)

    if args.task is None:
        if args.complete:
            run_completion_handler()
        elif args.reset:
            run_reset()
        else:
            run_scheduler_with_rule_event()
    else:
        run_scheduler_task(args.task, args.stack)
