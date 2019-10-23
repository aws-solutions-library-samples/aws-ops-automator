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
import datetime
import os

import boto3

import handlers
import services
import tagging
from tagging.tag_filter_expression import TagFilterExpression

YYYYMMDD = r"([0-9][0-9](:[0-9][0-9](:[0-9][0-9])?)?)?"
YYYY = r"[0-9]{4}"
MM = r"[0-1][0-9]"
DD = r"[0-3][0-9]"
HH = r"[0-2][0-9]"
MN = r"[0-5][0-9]"
SS = r"[0-5][0-9]"

DATE = YYYY + MM + DD
TIME = HH + MN + SS
DATETIME = DATE + TIME
DAY = DD
HOUR = HH

ISO_DATE = YYYY + "-" + MM + "-" + DD
ISO_TIME = HH + ":" + MN + ":" + SS
ISO_DATETIME = ISO_DATE + "T" + ISO_TIME + "((\+|\-)\d{2}:\d{2})?"
ISO_WEEKDAY = r"[1-7]"

MONTH_NAME_LONG = "(" + "|".join([datetime.date(2018, m, 1).strftime("%B") for m in range(1, 13)]) + ")"
MONTH_NAME_SHORT = "(" + "|".join([datetime.date(2018, m, 1).strftime("%b") for m in range(1, 13)]) + ")"
REGIONS = "(" + "|".join(boto3.Session().get_available_regions("ec2", "aws")) + ")"
TASK = r"\w+"
TAG_NAME = r"\w+"
TASK_ID = r"[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?4[0-9a-fA-F]{3}-?[89abAB][0-9a-fA-F]{3}-?[0-9a-fA-F]{12}"
TIMEZONE = r"\w+"
WEEK_NAME_LONG = "(" + "|".join([datetime.date(2018, 1, d).strftime("%A") for d in range(1, 8)]) + ")"
WEEK_NAME_SHORT = "(" + "|".join([datetime.date(2018, 1, d).strftime("%a") for d in range(1, 8)]) + ")"

NAME_OF_TAG_TO_DELETE = "tag-deleted"
VALUE_OF_TAG_TO_DELETE = "THIS TAG WILL BE DELETED"


def set_ec2_tag_to_delete(ec2_client, resource_ids):
    ids = resource_ids if isinstance(resource_ids, list) else [resource_ids]
    ec2_client.create_tags(resource_ids=ids, tags={NAME_OF_TAG_TO_DELETE: VALUE_OF_TAG_TO_DELETE})


def set_dynamodb_tag_to_delete(ddb_client, tables):
    tables_list = tables if isinstance(tables, list) else [tables]
    for table in tables_list:
        ddb_client.create_tags(table_name=table, tags={NAME_OF_TAG_TO_DELETE: "THIS TAG WILL BE DELETED"})


def set_rds_db_tag_to_delete(rds_client, db_instances):
    db_list = db_instances if isinstance(db_instances, list) else [db_instances]
    for db in db_list:
        rds_client.create_db_tags(db, tags={NAME_OF_TAG_TO_DELETE: "THIS TAG WILL BE DELETED"})

def set_rds_cluster_tag_to_delete(rds_client, db_clusters):
    cl_list = db_clusters if isinstance(db_clusters, list) else [db_clusters]
    for cl in cl_list:
        rds_client.create_cluster_tags(cl, tags={NAME_OF_TAG_TO_DELETE: "THIS TAG WILL BE DELETED"})


def set_rds_snapshot_tag_to_delete(rds_client, snapshots):
    snapshot_list = snapshots if isinstance(snapshots, list) else [snapshots]
    for snapshot in snapshot_list:
        rds_client.create_instance_snapshot_tags(snapshot, tags={NAME_OF_TAG_TO_DELETE: "THIS TAG WILL BE DELETED"})


def set_storagegateway_volume_tag_to_delete(swg_client, storage_gateway_volume_arn):
    swg_client.add_tags(arn=storage_gateway_volume_arn, tags={NAME_OF_TAG_TO_DELETE: "THIS TAG WILL BE DELETED"})




def common_placeholder_tags(placeholders=None, test_delete=True):
    tag_names = [
        tagging.TAG_VAL_ACCOUNT,
        tagging.TAG_VAL_AUTOMATOR_STACK,
        tagging.TAG_VAL_DATE,
        tagging.TAG_VAL_DATE_TIME,
        tagging.TAG_VAL_DAY,
        tagging.TAG_VAL_HOUR,
        tagging.TAG_VAL_ISO_DATE,
        tagging.TAG_VAL_ISO_DATETIME,
        tagging.TAG_VAL_ISO_TIME,
        tagging.TAG_VAL_ISO_WEEKDAY,
        tagging.TAG_VAL_MINUTE,
        tagging.TAG_VAL_MONTH,
        tagging.TAG_VAL_MONTH_NAME,
        tagging.TAG_VAL_MONTH_NAME_LONG,
        tagging.TAG_VAL_REGION,
        tagging.TAG_VAL_SECOND,
        tagging.TAG_VAL_TASK_TAG,
        tagging.TAG_VAL_TASK,
        tagging.TAG_VAL_TASK_ID,
        tagging.TAG_VAL_TIME,
        tagging.TAG_VAL_TIMEZONE,
        tagging.TAG_VAL_WEEKDAY,
        tagging.TAG_VAL_WEEKDAY_LONG,
        tagging.TAG_VAL_YEAR,
        "{{{}}}".format(tagging.TAG_VAL_TASK)]

    if placeholders is not None:
        tag_names += placeholders

    tag_str = ",".join(["tag-" + t + "={" + t + "}" for t in tag_names]).replace("{{", "{").replace("}}", "}")
    if test_delete:
        tag_str += ",{}={}".format(NAME_OF_TAG_TO_DELETE, tagging.TAG_DELETE)
    return tag_str


def verify_placeholder_tags(tags, action_placeholders=None, exclude_tags=None):
    checked_tags = [
        (tagging.TAG_VAL_ACCOUNT, services.get_aws_account()),
        (tagging.TAG_VAL_AUTOMATOR_STACK, os.getenv(handlers.ENV_STACK_NAME, "\.*")),
        (tagging.TAG_VAL_DATE, DATE),
        (tagging.TAG_VAL_DATE_TIME, DATETIME),
        (tagging.TAG_VAL_DAY, DD),
        (tagging.TAG_VAL_HOUR, HOUR),
        (tagging.TAG_VAL_ISO_DATE, ISO_DATE),
        (tagging.TAG_VAL_ISO_DATETIME, ISO_DATETIME),
        (tagging.TAG_VAL_ISO_TIME, ISO_TIME),
        (tagging.TAG_VAL_ISO_WEEKDAY, ISO_WEEKDAY),
        (tagging.TAG_VAL_MINUTE, MN),
        (tagging.TAG_VAL_MONTH, MM),
        (tagging.TAG_VAL_MONTH_NAME, MONTH_NAME_SHORT),
        (tagging.TAG_VAL_MONTH_NAME_LONG, MONTH_NAME_LONG),
        (tagging.TAG_VAL_REGION, REGIONS),
        (tagging.TAG_VAL_SECOND, SS),
        (tagging.TAG_VAL_TASK_TAG, TAG_NAME),
        (tagging.TAG_VAL_TASK, TASK),
        (tagging.TAG_VAL_TASK_ID, TASK_ID),
        (tagging.TAG_VAL_TIME, TIME),
        (tagging.TAG_VAL_TIMEZONE, TIMEZONE),
        (tagging.TAG_VAL_WEEKDAY, WEEK_NAME_SHORT),
        (tagging.TAG_VAL_WEEKDAY_LONG, WEEK_NAME_LONG),
        (tagging.TAG_VAL_YEAR, YYYY),
        (tags.get("tag-" + tagging.TAG_VAL_TASK), TASK)]

    if exclude_tags is not None:
        for e in exclude_tags:
            for c in checked_tags:
                if c[0] == e:
                    checked_tags.remove(c)
                    break

    expression = "&".join(
        ["(tag-{}=\\^{}$)".format(e[0], (e[1] if e[1] else "").replace("(", "%(").replace(")", "%)").replace(" ", "\\s")) for e in
         checked_tags])

    expression += "&!{}=*".format(NAME_OF_TAG_TO_DELETE)

    if action_placeholders is not None:
        action_tags = [(p, action_placeholders[p]) for p in action_placeholders]

        expression += "&" + "&".join(
            ["(tag-{}={})".format(e[0], (e[1] if e[1] else "")
                                  .replace("(", "%(")
                                  .replace(")", "%)")
                                  .replace(" ", "\\s" if e[1] and e[1][0] == '\\' else " "))
             for e in action_tags])

    return TagFilterExpression(expression).is_match(tags)
