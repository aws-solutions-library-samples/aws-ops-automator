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

import copy
import json
import re
import sys
import uuid
from collections import OrderedDict

import actions
import services

MIN_LAMBDA_MEMORY = 128
MAX_LAMBDA_MEMORY = 1536


def get_action_memory_size(action_name, action_properties):
    size = action_properties.get(actions.ACTION_MEMORY, MIN_LAMBDA_MEMORY)
    if size < MIN_LAMBDA_MEMORY:
        return MIN_LAMBDA_MEMORY
    else:
        size = (((size - 1) / 64) + 1) * 64
        if size > MAX_LAMBDA_MEMORY:
            raise ValueError("Maximum memory size for action {} is {} MB".format(action_name, MAX_LAMBDA_MEMORY))
        return size


def build_action_policy_statement(action_name, action_permissions):
    statements = []
    simple_statement_statements = []

    for permission in action_permissions:
        if isinstance(permission, str):
            simple_statement_statements.append(permission)
        if isinstance(permission, dict):
            # default statement
            statement = {
                "Sid": re.sub("[^0-9A-Za-z]", "", action_name + str(uuid.uuid4())),
                "Effect": "Allow",
                "Resource": "*"
            }
            # go through list of items for statement, these may overwrite the default attributes of the statement set above
            statement.update({k: permission[k] for k in permission})
            statements.append(statement)

    if len(simple_statement_statements) > 0:
        statements.append({
            "Sid": re.sub("[^0-9A-Za-z]", "", action_name + str(uuid.uuid4())),
            "Effect": "Allow",
            "Resource": "*",
            "Action": list(simple_statement_statements)
        })
    return statements


def get_versioned_template(template_filename, version, bucket):
    with open(template_filename, "rt") as f:
        template_text = "".join(f.readlines())
        template_text = template_text.replace("%version%", version)
        template_text = template_text.replace("%bucket%", bucket)
        return json.loads(template_text, object_pairs_hook=OrderedDict)


def add_additional_lambda_functions(template, all_actions):
    additional_lambda_sizes = {}

    for action_name in all_actions:

        action_properties = actions.get_action_properties(action_name)

        memory_requirements = get_action_memory_size(action_name, action_properties)
        if memory_requirements != actions.LAMBDA_DEFAULT_MEMORY:
            if memory_requirements not in additional_lambda_sizes:
                additional_lambda_sizes[memory_requirements] = [action_name]
            else:
                additional_lambda_sizes[memory_requirements].append(action_name)

    if len(additional_lambda_sizes) > 0:

        scheduler_role = template["Resources"]["SchedulerRole"]
        action_statement = scheduler_role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]

        # get item in statement that give persmission to lambda to invoke
        temp = [s for s in action_statement if s.get("Sid", "") == "SchedulerInvoke"]
        if len(temp) == 0:
            raise Exception("Can not find statement with Sid named \"SchedulerInvoke\"")
        scheduler_invoke = temp[0]

        default = template["Resources"]["SchedulerDefault"]
        for memory_size in additional_lambda_sizes:

            new_name = "Scheduler{:0>04d}".format(memory_size)

            new_lambda = copy.deepcopy(default)
            new_lambda["Properties"]["MemorySize"] = memory_size
            new_lambda["Properties"]["Description"] = \
                default["Properties"]["Description"].replace("(default)", "({} MB".format(memory_size)) \
                + " for execution of actions: {})".format(",".join(additional_lambda_sizes[memory_size]))
            new_lambda["Properties"]["FunctionName"]["Fn::Join"][1][-1] = new_name
            template["Resources"][new_name] = new_lambda


            # add permission to invoke this lambda
            new_resource = copy.deepcopy(scheduler_invoke["Resource"][0])
            new_resource["Fn::Join"][1][-1]["Fn::Join"][1][-1] = new_name
            scheduler_invoke["Resource"].append(new_resource)


def add_actions_permissions(template, all_actions):
    def action_select_resources_permissions(action_properties):
        return services.get_resource_describe_permissions(action_properties[actions.ACTION_SERVICE],
                                                          action_properties[actions.ACTION_RESOURCES])

    scheduler_role = template["Resources"]["SchedulerRole"]
    action_statement = scheduler_role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    for action_name in all_actions:

        action_properties = actions.get_action_properties(action_name)

        # get permissions from action properties
        action_permissions = action_properties.get(actions.ACTION_PERMISSIONS, [])
        # get the permissions to retrieve the resources for that action
        # with possible additional permissions to retrieve tags
        action_permissions.append(action_select_resources_permissions(action_properties))

        if len(action_permissions) is not 0:
            statements = build_action_policy_statement(action_name, action_permissions)
            action_statement += statements


def add_actions_stack_resource_permissions(template, all_actions):
    scheduler_role = template["Resources"]["SchedulerRole"]
    action_statement = scheduler_role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    for action_name in all_actions:

        action_properties = actions.get_action_properties(action_name)

        # get permissions from action properties
        action_permissions = action_properties.get(actions.ACTION_STACK_RESOURCES_PERMISSIONS, [])

        if len(action_permissions) is not 0:
            statements = build_action_policy_statement(action_name, action_permissions)
            action_statement += statements


def add_action_stack_resources(template, all_actions):
    def fix_resource_references(resources, old, new):

        def update_list(l, old_name, new_name):
            for i in l:
                if isinstance(i, dict):
                    fix_resource_references(i, old_name, new_name)
                elif isinstance(i, list):
                    update_list(i, old_name, new_name)

        for key, val in resources.iteritems():
            if key == "Ref" and val == old:
                resources[key] = new
            if isinstance(val, dict):
                fix_resource_references(val, old, new)

            elif isinstance(val, list):
                update_list(val, old, new)

    template_resources = template["Resources"]
    action_statement = template_resources["SchedulerRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]

    for action_name in all_actions:

        action_properties = actions.get_action_properties(action_name)
        action_class_name = action_properties[actions.ACTION_CLASS_NAME]

        stack_resources = action_properties.get(actions.ACTION_PARAM_STACK_RESOURCES)
        stack_resource_permissions = action_properties.get(actions.ACTION_STACK_RESOURCES_PERMISSIONS, [])

        if stack_resources:

            resource_names = []

            action_resources_to_add = {}
            # get additional resources and build new dict with prefixed names
            for resource_name, value in stack_resources.iteritems():
                prefixed_resource_name = action_class_name + resource_name
                resource_names.append((resource_name, prefixed_resource_name))
                action_resources_to_add[prefixed_resource_name] = stack_resources[resource_name]

            # fix names of prefixed resource names in references
            for resource_name in resource_names:
                # references in stack resources
                fix_resource_references(action_resources_to_add, resource_name[0], resource_name[1])
                # references in list of permissions for stack resources
                for i in stack_resource_permissions:
                    if isinstance(i, dict):
                        fix_resource_references(i, resource_name[0], resource_name[1])

            # add the resources for this action to the template
            for resource_name in action_resources_to_add:
                template_resources[resource_name] = action_resources_to_add[resource_name]

            if len(stack_resource_permissions) is not 0:
                statements = build_action_policy_statement(action_name, stack_resource_permissions)
                action_statement += statements


def main(template_file, version, bucket):
    template = get_versioned_template(template_file, version,bucket)
    all_actions = actions.all_actions()
    add_actions_permissions(template, all_actions)
    add_additional_lambda_functions(template, all_actions)
    add_action_stack_resources(template, all_actions)
    print(json.dumps(template, indent=4))


main(template_file=sys.argv[1], version=sys.argv[2], bucket=sys.argv[3])

exit(0)
