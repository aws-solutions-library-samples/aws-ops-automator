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


# import imp
import importlib
import types
import json
import os
import sys
import zipfile

import boto3
import requests


def get_lambda_code(cmdline_args):
    """
    Downloads and unzips the code of the Lambda
    :param cmdline_args:
    :return:
    """
    stack_name = cmdline_args["stack"]
    stack_region = cmdline_args["stack_region"]

    lambda_client = boto3.client("lambda", region_name=stack_region)

    os.environ["AWS_DEFAULT_REGION"] = stack_region

    lambda_function_name = "{}-{}".format(stack_name, "OpsAutomator-Standard")
    lambda_function = lambda_client.get_function(FunctionName=lambda_function_name)
    lambda_environment = lambda_function["Configuration"]["Environment"]["Variables"]

    for ev in lambda_environment:
        os.environ[ev] = lambda_environment[ev]

    code_url = lambda_function["Code"]["Location"]
    code_stream = requests.get(code_url, stream=True)

    temp_code_directory = "./"
    lambda_code_zip_file = os.path.join(temp_code_directory, "code.zip")
    with open(lambda_code_zip_file, 'wb') as fd:
        for chunk in code_stream.iter_content(chunk_size=10240):
            fd.write(chunk)

    zip_ref = zipfile.ZipFile(lambda_code_zip_file, 'r')
    zip_ref.extractall(temp_code_directory)
    zip_ref.close()

    return temp_code_directory


def run_ops_automator_step(cmdline_args):
    """
    Runs ecs_handler
    :param cmdline_args: arguments used by ecs_handler to rebuild event for Ops Automator select or execute handler
    :return: result of the Ops Automator handler
    """
    code_directory = get_lambda_code(cmdline_args)

    # load main module
    main_module_file = os.path.join(code_directory, "main.py")

    spec = importlib.util.find_spec("main")
    try:
       lambda_main_module = spec.loader.create_module(spec)
    except AttributeError:
        lambda_main_module = None
    if lambda_main_module is None:
        lambda_main_module = types.ModuleType(spec.name)
    # No clear way to set import-related attributes.
    spec.loader.exec_module(lambda_main_module)

    lambda_function_ecs_handler = lambda_main_module.ecs_handler

    # get and run ecs_handler method
    return lambda_function_ecs_handler(cmdline_args)


if __name__ == "__main__":

    print("Running Ops Automator ECS Job, version %version%")

    if len(sys.argv) < 2:
        print("No task arguments passed as first parameter")
        exit(1)

    args = {}

    try:
        args = json.loads(sys.argv[1])
    except Exception as ex:
        print(("\"{}\" is not valid JSON, {}", sys.argv[1], ex))
        exit(1)

    try:
        print(("Task arguments to run the job are\n {}".format(json.dumps(args, indent=3))))
        print(("Result is {}".format(run_ops_automator_step(args))))
        exit(0)

    except Exception as e:
        print(e)
        exit(1)
