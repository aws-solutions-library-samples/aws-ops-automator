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
import copy
import os
import re

import boto3

import handlers
import pytz
from actions import date_time_provider, RESTRICTED_TAG_VALUE_SET_CHARACTERS
from helpers import safe_json

WARN_TAGS_CANNOT_BE_DELETED = "Tags {} can not be deleted"

TAG_VAL_STR = "{{{}}}"
TAG_VAL_ACCOUNT = "account"
TAG_VAL_AUTOMATOR_STACK = "stack"
TAG_VAL_DATE = "date"
TAG_VAL_DATE_TIME = "datetime"
TAG_VAL_DAY = "day"
TAG_VAL_HOUR = "hour"
TAG_VAL_ISO_DATE = "iso-date"
TAG_VAL_ISO_DATETIME = "iso-datetime"
TAG_VAL_ISO_TIME = "iso-time"
TAG_VAL_ISO_WEEKDAY = "iso-weekday"
TAG_VAL_MINUTE = "minute"
TAG_VAL_MONTH = "month"
TAG_VAL_MONTH_NAME = "monthname"
TAG_VAL_MONTH_NAME_LONG = "monthname-long"
TAG_VAL_REGION = "region"
TAG_VAL_SECOND = "second"
TAG_VAL_TASK_TAG = "task-tag"
TAG_VAL_TASK = "task"
TAG_VAL_TASK_ID = "task-id"
TAG_VAL_TASK_GROUP = "task-group"
TAG_VAL_TIME = "time"
TAG_VAL_TIMEZONE = "timezone"
TAG_VAL_WEEKDAY = "weekday"
TAG_VAL_WEEKDAY_LONG = "weekday-long"
TAG_VAL_YEAR = "year"

TAG_DELETE = "{delete}"


def build_tags_from_template(tags_str,
                             task, task_id,
                             timezone="UTC",
                             account=None,
                             region=None,
                             tag_variables=None,
                             restricted_value_set=False,
                             include_deleted_tags=True):
    """
    Build template tags object from template_str

    Args:
        tags_str: (str): write your description
        task: (todo): write your description
        task_id: (str): write your description
        timezone: (todo): write your description
        account: (todo): write your description
        region: (str): write your description
        tag_variables: (str): write your description
        restricted_value_set: (str): write your description
        include_deleted_tags: (bool): write your description
    """

    tag_vars = {} if tag_variables is None else copy.copy(tag_variables)

    tz = timezone if timezone not in ["", None] else "UTC"

    dt = date_time_provider().now(tz=pytz.timezone(tz))
    dt = dt.replace(microsecond=0)

    # variables used in tag names/values
    tag_vars.update({
        TAG_VAL_ACCOUNT: account if account is not None else "",
        TAG_VAL_AUTOMATOR_STACK: os.getenv(handlers.ENV_STACK_NAME, ""),
        TAG_VAL_DATE: "{:0>4d}{:0>2d}{:0>2d}".format(dt.year, dt.month, dt.day),
        TAG_VAL_DATE_TIME: "{:0>4d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}{:0>2d}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                                                                               dt.second),
        TAG_VAL_DAY: "{:0>2d}".format(dt.day),
        TAG_VAL_HOUR: "{:0>2d}".format(dt.hour),
        TAG_VAL_ISO_DATE: dt.date().isoformat(),
        TAG_VAL_ISO_DATETIME: dt.isoformat(),
        TAG_VAL_ISO_TIME: dt.time().isoformat(),
        TAG_VAL_ISO_WEEKDAY: dt.isoweekday(),
        TAG_VAL_MINUTE: "{:0>2d}".format(dt.minute),
        TAG_VAL_MONTH: "{:0>2d}".format(dt.month),
        TAG_VAL_MONTH_NAME: dt.strftime("%b"),
        TAG_VAL_MONTH_NAME_LONG: dt.strftime("%B"),
        TAG_VAL_REGION: region if region is not None else "",
        TAG_VAL_SECOND: "{:0>2d}".format(dt.second),
        TAG_VAL_TASK_TAG: os.getenv(handlers.ENV_AUTOMATOR_TAG_NAME),
        TAG_VAL_TASK: task,
        TAG_VAL_TASK_ID: task_id,
        TAG_VAL_TIME: "{:0>2d}{:0>2d}{:0>2d}".format(dt.hour, dt.minute, dt.second),
        TAG_VAL_TIMEZONE: dt.tzname(),
        TAG_VAL_WEEKDAY: dt.strftime("%a"),
        TAG_VAL_WEEKDAY_LONG: dt.strftime("%A"),
        TAG_VAL_YEAR: "{:0>4d}".format(dt.year)
    })

    # get ssm parameter values and add to variables
    names = re.findall("{ssm:(.+?)\}", tags_str)
    if len(names) > 0:
        resp = boto3.client("ssm").get_parameters(Names=list(set(names)))
        for p in resp.get("Parameters", []):
            tag_vars["ssm:{}".format(p["Name"])] = p["Value"].split(",") if p["Type"] == "StringList" else p["Value"]

    # variables as strings
    for v in list(tag_vars.keys()):
        if tag_vars[v] is None:
            value = ""
        elif isinstance(tag_vars[v], list):
            value = ",".join(tag_vars[v])
        elif isinstance(tag_vars[v], dict):
            value = safe_json(tag_vars[v])
        else:
            value = str(tag_vars[v])
        tag_vars[v] = value

    # build tag names with unprocessed values
    lastkey = None
    tags = {}
    for t in tags_str.split(","):
        t = t.strip()
        if "=" in t:
            t = t.partition("=")
            key = t[0].strip()
            for v in tag_vars:
                key = key.replace(TAG_VAL_STR.format(v), tag_vars[v])
            tags[key] = t[2].strip()
            lastkey = key
        elif lastkey is not None:
            tags[lastkey] = ",".join([tags[lastkey], t])

    # process values
    for t in tags:
        if tags[t] not in ["", None]:
            for v in tag_vars:
                tags[t] = tags[t].replace(TAG_VAL_STR.format(v), tag_vars[v])
        else:
            if tags[t] is None:
                del tags[t]

    if restricted_value_set:
        clean_tag_set(tags)

    if not include_deleted_tags:
        for t in list(tags.keys()):
            if tags[t] == TAG_DELETE:
                del tags[t]
    return tags


def build_str_from_template(tags_str, task, task_id, timezone="UTC", account=None, region=None, tag_variables=None):
    """
    Builds a string from a template string.

    Args:
        tags_str: (str): write your description
        task: (array): write your description
        task_id: (str): write your description
        timezone: (todo): write your description
        account: (int): write your description
        region: (str): write your description
        tag_variables: (str): write your description
    """
    return build_tags_from_template("str=" + tags_str, task, task_id,
                                    timezone=timezone,
                                    account=account, region=region,
                                    tag_variables=tag_variables)["str"]


def clean_tag_set(tags_to_clean):
    """
    Convert tags to clean tags.

    Args:
        tags_to_clean: (str): write your description
    """
    for t in tags_to_clean:
        if tags_to_clean[t] == TAG_DELETE:
            continue
        tags_to_clean[t] = re.sub(RESTRICTED_TAG_VALUE_SET_CHARACTERS, " ", tags_to_clean[t])
        tags_to_clean[t] = tags_to_clean[t].replace("\n", " ")


def tag_key_value_list(tags_dict):
    """
   Builds list of tag structures to be passed as parameter to the tag APIs
   :param tags_dict: dictionary of tags
   :return: list of tags
   """
    if tags_dict is None:
        return []

    valid_tags = {tag_key: tags_dict[tag_key] for tag_key in tags_dict if
                  not (tag_key.startswith("aws:")
                       and not tag_key.startswith("cloudformation:")
                       and not tag_key.startswith("rds:"))
                  }
    return [{"Key": t, "Value": tags_dict[t]} for t in valid_tags] if valid_tags is not None else []


def split_task_list(task_list):
    """
    Splits string with list of tasks into list
    :param task_list:
    :return:
    """
    if task_list is None:
        return []

    # separators are ",", " " and "/"
    return [t.strip() for t in task_list.replace(" ", ",").replace("/", ",").split(",") if t.strip() != ""]


def set_ec2_tags(ec2_client, resource_ids, tags, can_delete=True, logger=None):
    """
    Set the ec2 ec2 ec2 tags

    Args:
        ec2_client: (todo): write your description
        resource_ids: (str): write your description
        tags: (str): write your description
        can_delete: (todo): write your description
        logger: (todo): write your description
    """
    def create_tags(client, resources, created_tags):
        """
        Create an array.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            created_tags: (str): write your description
        """
        client.create_tags_with_retries(Resources=resources, Tags=created_tags)

    def delete_tags(client, resources, tags_to_delete):
        """
        Delete all tags from a set of resources.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            tags_to_delete: (str): write your description
        """
        client.delete_tags_with_retries(Resources=resources, Tags=[{"Key": t} for t in tags_to_delete])

    _set_resource_tags(client=ec2_client, resources=resource_ids,
                       tags=tags, create_func=create_tags,
                       delete_func=delete_tags,
                       logger=logger,
                       can_delete=can_delete)


def set_dynamodb_tags(ddb_client, resource_arns, tags, can_delete=True, logger=None):
    """
    Sets the dynamb resource tags.

    Args:
        ddb_client: (todo): write your description
        resource_arns: (str): write your description
        tags: (todo): write your description
        can_delete: (todo): write your description
        logger: (todo): write your description
    """
    def create_tags(client, resources, created_tags):
        """
        Creates a list of tags.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            created_tags: (str): write your description
        """
        for arn in resources:
            client.tag_resource_with_retries(ResourceArn=arn, Tags=created_tags)

    def delete_tags(client, resources, deleted_tags):
        """
        Delete tags for the specified tags.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            deleted_tags: (todo): write your description
        """
        for arn in resources:
            client.untag_resource_with_retries(ResourceArn=arn, TagKeys=deleted_tags)

    _set_resource_tags(client=ddb_client,
                       resources=resource_arns,
                       tags=tags,
                       create_func=create_tags,
                       delete_func=delete_tags,
                       logger=logger,
                       can_delete=can_delete)


def set_rds_tags(rds_client, resource_arns, tags, can_delete=True, logger=None):
    """
    Set rdf tags for a resource.

    Args:
        rds_client: (todo): write your description
        resource_arns: (str): write your description
        tags: (todo): write your description
        can_delete: (todo): write your description
        logger: (todo): write your description
    """
    def create_tags(client, resources, created_tags):
        """
        Create a list of the given resource.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            created_tags: (str): write your description
        """
        for arn in resources:
            client.add_tags_to_resource_with_retries(ResourceName=arn, Tags=created_tags)

    def delete_tags(client, resources, deleted_tags):
        """
        Deletes tags from a list of resources.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            deleted_tags: (todo): write your description
        """
        for arn in resources:
            client.remove_tags_from_resource_with_retries(ResourceName=arn, TagKeys=deleted_tags)

    _set_resource_tags(client=rds_client,
                       resources=resource_arns,
                       tags=tags,
                       create_func=create_tags,
                       delete_func=delete_tags,
                       logger=logger,
                       can_delete=can_delete)


def set_storagegateway_tags(sgw_client, resource_arns, tags, can_delete=True, logger=None):
    """
    Set tags on a storage gateway.

    Args:
        sgw_client: (str): write your description
        resource_arns: (str): write your description
        tags: (str): write your description
        can_delete: (str): write your description
        logger: (todo): write your description
    """
    def create_tags(client, resources, created_tags):
        """
        Create a list of the given resource.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            created_tags: (str): write your description
        """
        for arn in resources:
            client.add_tags_to_resource_with_retries(ResourceARN=arn, Tags=created_tags)

    def delete_tags(client, resources, deleted_tags):
        """
        Deletes tags from the given resources.

        Args:
            client: (todo): write your description
            resources: (dict): write your description
            deleted_tags: (todo): write your description
        """
        for arn in resources:
            client.remove_tags_from_resource_with_retries(ResourceARN=arn, TagKeys=deleted_tags)

    _set_resource_tags(client=sgw_client,
                       resources=resource_arns,
                       tags=tags,
                       create_func=create_tags,
                       delete_func=delete_tags,
                       logger=logger,
                       can_delete=can_delete)


def _set_resource_tags(client, resources, tags, create_func, delete_func, can_delete=True, logger=None):
    """
    Set the tags for a resource.

    Args:
        client: (todo): write your description
        resources: (todo): write your description
        tags: (todo): write your description
        create_func: (todo): write your description
        delete_func: (todo): write your description
        can_delete: (str): write your description
        logger: (todo): write your description
    """
    tag_set = copy.deepcopy(tags)

    resource_list = resources if isinstance(resources, list) else [resources]

    tags_to_delete = [t for t in tags if tag_set[t] == TAG_DELETE]
    if len(tags_to_delete) > 0:
        if can_delete:
            for t in tags_to_delete:
                del tag_set[t]
            delete_func(client, resource_list, tags_to_delete)

        else:
            if logger is not None:
                logger.warning(WARN_TAGS_CANNOT_BE_DELETED, ",".join(tags_to_delete))

    if len(tag_set) > 0:
        create_func(client, resource_list, tag_key_value_list(tag_set))
