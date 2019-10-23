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
import decimal
import json
import os.path
import sys
from datetime import datetime
from math import trunc

import boto3


class CustomCfnJsonEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, set):
            return list(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return str(trunc(o))
        if isinstance(o, Exception):
            return str(o)
        return json.JSONEncoder.default(self, o)


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print(("Syntax is {} taskname [optional profile name]".format(os.path.basename(sys.argv[0]))))

    stack_name = "%stack%"
    table_name = "%config_table%"

    task_name = sys.argv[1]

    session = boto3.Session(profile_name=sys.argv[2]) if (len(sys.argv)) > 2 else boto3.Session()

    service_token = "arn:aws:lambda:%region%:%account%:function:%stack%-OpsAutomator-Standard"

    db = session.resource("dynamodb").Table(table_name)
    config_item = db.get_item(
        TableName=table_name,
        Key={
            "Name": task_name
        }).get("Item")

    if config_item is None:
        print(("Task {} not found in table {}".format(task_name, table_name)))
        exit(1)

    config_item.update({"Name": task_name, "ServiceToken": service_token})
    if "StackId" in config_item:
        del config_item["StackId"]

    for p in list(config_item.keys()):
        if config_item[p] is None:
            del config_item[p]

    for p in list(config_item.get("Parameters",{}).keys()):
        if config_item["Parameters"][p] is None:
            del config_item["Parameters"][p]

    custom_resource = {
        "Type": "Custom::TaskConfig",
        "Properties": config_item
    }

    result = json.dumps(custom_resource, cls=CustomCfnJsonEncoder, indent=3, sort_keys=True)
    result = result.replace(': true', ': "True"')
    result = result.replace(': false', ': "False"')

    print(result)
