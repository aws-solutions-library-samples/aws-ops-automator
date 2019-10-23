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
import json
import os
import urllib.request, urllib.parse, urllib.error
from collections import OrderedDict
from os import path

import actions
import handlers
import services

CFN_CONSOLE_URL_TEMPLATE = \
    "https://{}.console.aws.amazon.com/cloudformation/home?region={}#/stacks/create/review?" \
    "param_Description={}&templateURL=https:%2F%2Fs3-{}.amazonaws.com%2F{}%2FTaskConfiguration%2F{}.template"

HTML_ACTION_LIST_ITEM = \
    "\t\t\t<li class='actions-item'>\n\t\t\t\t<a href='{}' target='_blank'>{}</a>\n\t\t\t</li>\n"
HTML_ACTIONS_GROUPS_LISTS = \
    "<ul class='groups-list'>{}\n</ul>"
HTML_GROUP_LIST_ITEM = \
    "\n\t<li class='actions-list'>\n\t\t<div class='actions-list-header'>{}</div>\n\t\t<ul>\n{}\n\t\t</ul>\n\t</li>"


def build_events_forward_template(template_filename, script_filename, stack, event_role_arn, ops_automator_topic_arn, version):
    with open(script_filename, "rt") as f:
        script_text = f.readlines()

    with open(template_filename, "rt") as f:
        template = json.loads("".join(f.readlines()), object_pairs_hook=OrderedDict)

        code = template["Resources"]["EventsForwardFunction"]["Properties"]["Code"]
        code["ZipFile"]["Fn::Join"][1] = script_text

    return json.dumps(template, indent=3) \
        .replace("%version%", version) \
        .replace("%ops-automator-stack%", stack) \
        .replace("%ops-automator-region%", services.get_session().region_name) \
        .replace("%ops-automator-account%", services.get_aws_account()) \
        .replace("%ops-automator-topic-arn%", ops_automator_topic_arn) \
        .replace("%event-forward-role%", event_role_arn)


def build_scenario_templates(templates_dir, stack):
    for template_name in os.listdir(templates_dir):
        with open(path.join(templates_dir, template_name), "rt") as f:
            template = json.loads("".join(f.readlines()), object_pairs_hook=OrderedDict)

            yield template_name, json.dumps(template, indent=3).replace("%ops-automator-stack%", stack)


def group_name_from_action_name(action_name):
    i = 1
    while i < len(action_name) and (action_name[i].islower() or action_name[i].isdigit()):
        i += 1
    group_name = action_name[0:i].upper()
    return group_name


def generate_html_actions_page(html_file, region):
    with open(html_file) as f:
        html_template = "".join(f.readlines())

    bucket = os.getenv(handlers.ENV_CONFIG_BUCKET)
    stack = os.getenv(handlers.ENV_STACK_NAME)

    action_groups = {}
    for a in actions.all_actions():
        ap = actions.get_action_properties(a)
        if ap.get(actions.ACTION_INTERNAL):
            continue
        href = CFN_CONSOLE_URL_TEMPLATE.format(region, region, urllib.parse.quote(ap.get(actions.PARAM_DESCRIPTION, "")), region, bucket,
                                               a)

        group_name = group_name_from_action_name(a)
        if group_name not in action_groups:
            action_groups[group_name] = {}

        action_groups[group_name][a] = (href, ap.get(actions.ACTION_TITLE))

    action_list = ""
    for g in sorted(action_groups.keys()):
        actions_list = ""
        for a in sorted(action_groups[g].keys()):
            actions_list += HTML_ACTION_LIST_ITEM.format(action_groups[g][a][0],
                                                         action_groups[g][a][1])
        action_list += HTML_GROUP_LIST_ITEM.format(g, actions_list)
    action_list = HTML_ACTIONS_GROUPS_LISTS.format(action_list)

    return html_template.replace("%actions%", action_list).replace("%stack%", stack)
