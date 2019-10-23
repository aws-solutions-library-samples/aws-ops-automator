import copy
import json
import re
import sys
from collections import OrderedDict

import actions
import handlers
import services
from services.ec2_service import Ec2Service

SETUP_HELPER = "OpsAutomatorSetupHelper"

LAMBDA_FUNCTION_SETTING_NAME = "OpsAutomatorLambdaFunction"

LAMBDA_FUNCTION_RESOURCE_BASE_NAME = "OpsAutomatorLambdaFunction"
LAMBDA_FUNCTION_STANDARD_RESOURCE_NAME = LAMBDA_FUNCTION_RESOURCE_BASE_NAME + actions.ACTION_SIZE_STANDARD

LAMBDA_ROLE = "OpsAutomatorLambdaRole"

LAMBDA_TIMEOUT = 900

LAMBDA_DESC_SIZE = "({}, Memory {} MB)"

LAMBDA_SIZE = "%size%"


def build_action_policy_statement(action_name, action_permissions):
    statements = []

    if len(action_permissions) > 0:
        statements.append({
            "Sid": re.sub("[^0-9A-Za-z]", "", action_name),
            "Effect": "Allow",
            "Resource": "*",
            "Action": sorted(list(set(action_permissions)))
        })
    return statements


def get_versioned_template(template_filename, bucket, solution, version):

    with open(template_filename, "rt") as f:
        template_text = "".join(f.readlines())
        template_text = template_text.replace("%bucket%", bucket)
        template_text = template_text.replace("%solution%", solution)
        template_text = template_text.replace("%version%", version)
        return json.loads(template_text, object_pairs_hook=OrderedDict)


def add_additional_lambda_functions(template):
    ops_automator_role = template["Resources"]["OpsAutomatorLambdaRole"]
    action_statement = ops_automator_role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    # get item in statement that give permission to lambda to invoke
    temp = [s for s in action_statement if s.get("Sid", "") == "OpsAutomatorLambdaInvoke"]
    if len(temp) == 0:
        raise Exception("Can not find statement with Sid named \"OpsAutomatorLambdaInvoke\"")
    ops_automator_invoke = temp[0]

    settings = template["Mappings"]["Settings"]
    memory_sizes = settings["ActionMemory"]
    default = template["Resources"][LAMBDA_FUNCTION_STANDARD_RESOURCE_NAME]

    for lambda_size in memory_sizes:
        if lambda_size not in [actions.ACTION_SIZE_STANDARD, actions.ACTION_USE_ECS]:
            new_name = "{}{}".format(LAMBDA_FUNCTION_RESOURCE_BASE_NAME, lambda_size)

            new_lambda = copy.deepcopy(default)
            new_lambda["Properties"]["MemorySize"] = memory_sizes[lambda_size]
            new_lambda["Properties"]["Timeout"] = LAMBDA_TIMEOUT
            new_lambda["Properties"]["Description"] = \
                default["Properties"]["Description"].replace(LAMBDA_SIZE, LAMBDA_DESC_SIZE.format(lambda_size,
                                                                                                  memory_sizes[lambda_size]))

            new_lambda["Properties"]["FunctionName"]["Fn::Join"][1][-1]= lambda_size
            template["Resources"][new_name] = new_lambda

            # add permission to invoke this lambda
            new_resource = copy.deepcopy(ops_automator_invoke["Resource"][0])
            new_resource["Fn::Join"][1][-1]["Fn::Join"][-1][-1] = lambda_size
            ops_automator_invoke["Resource"].append(new_resource)

    description = default["Properties"]["Description"]
    description = description.replace(LAMBDA_SIZE, LAMBDA_DESC_SIZE.format(actions.ACTION_SIZE_STANDARD,
                                                                           memory_sizes[actions.ACTION_SIZE_STANDARD]))
    default["Properties"]["Description"] = description


def add_actions_permissions(template, all_actions):
    def action_select_resources_permissions(action_prop):
        return services.get_resource_describe_permissions(action_prop[actions.ACTION_SERVICE],
                                                          [action_prop[actions.ACTION_RESOURCES]])

    ops_automator_role = template["Resources"][LAMBDA_ROLE]
    action_statement = ops_automator_role["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]

    required_actions = set()

    for action_name in all_actions:

        action_properties = actions.get_action_properties(action_name)

        # get permissions from action properties
        action_permissions = action_properties.get(actions.ACTION_PERMISSIONS, [])
        # get the permissions to retrieve the resources for that action
        # with possible additional permissions to retrieve tags
        action_permissions += list(action_select_resources_permissions(action_properties))

        if len(action_permissions) is not 0:
            required_actions.update(action_permissions)
            # if using these lines individual statemens are built for every action
            # statements = build_action_policy_statement(action_name, action_permissions)
            # action_statement += statements

    action_statement += build_action_policy_statement("ActionPermissions", required_actions)


def add_action_stack_resources(template, all_actions):
    def fix_resource_references(resources, old, new):

        def update_list(l, old_name, new_name):
            for item in l:
                if isinstance(item, dict):
                    fix_resource_references(item, old_name, new_name)
                elif isinstance(item, list):
                    update_list(item, old_name, new_name)

        for key in resources:
            val = resources[key]
            if key == "Ref" and val == old:
                resources[key] = new
            if isinstance(val, dict):
                fix_resource_references(val, old, new)

            elif isinstance(val, list):
                update_list(val, old, new)

    template_resources = template["Resources"]
    action_statement = template_resources[LAMBDA_ROLE]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    setup_helper_dependencies = template_resources[SETUP_HELPER]["DependsOn"]

    for action_name in all_actions:

        action_properties = actions.get_action_properties(action_name)
        action_class_name = action_properties[actions.ACTION_CLASS_NAME]

        stack_resources = action_properties.get(actions.ACTION_STACK_RESOURCES)
        stack_resource_permissions = action_properties.get(actions.ACTION_STACK_RESOURCES_PERMISSIONS, {})

        if stack_resources:

            resource_names = []

            action_resources_to_add = {}
            # get additional resources and build new dict with prefixed names
            for resource_name in stack_resources:
                prefixed_resource_name = action_class_name[0:-len("Action")] + resource_name
                resource_names.append((resource_name, prefixed_resource_name))
                action_resources_to_add[prefixed_resource_name] = stack_resources[resource_name]
                setup_helper_dependencies.append(prefixed_resource_name)

            # fix names of prefixed resource names in references
            for resource_name in resource_names:
                # references in stack resources
                fix_resource_references(action_resources_to_add, resource_name[0], resource_name[1])
                # references in list of permissions for stack resources
                for i in stack_resource_permissions["Resource"]:
                    if isinstance(i, dict):
                        fix_resource_references(i, resource_name[0], resource_name[1])

            # add the resources for this action to the template
            for resource_name in action_resources_to_add:
                template_resources[resource_name] = action_resources_to_add[resource_name]

            if len(stack_resource_permissions) is not 0:
                # statements = build_action_policy_statement(action_name, stack_resource_permissions)
                stack_resource_permissions["Sid"] = re.sub("[^0-9A-Za-z]", "", action_name + "Resources")
                action_statement.append(stack_resource_permissions)


def main(template_file, bucket, solution, version):

    template = get_versioned_template(template_file, bucket, solution, version)

    all_actions = actions.all_actions()
    add_actions_permissions(template, all_actions)
    add_additional_lambda_functions(template)
    add_action_stack_resources(template, all_actions)
    print((json.dumps(template, indent=4)))


if __name__ == "__main__":
    main(template_file=sys.argv[1], bucket=sys.argv[2], solution=sys.argv[3], version=sys.argv[4])

    exit(0)
