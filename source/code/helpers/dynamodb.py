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
from datetime import datetime
from decimal import Decimal


def unpack_record(record):
    def get_data(item):
        data_type = item.keys()[0]
        data = item[data_type]
        if data_type == "M":
            return {i: get_data(data[i]) for i in data}
        if data_type == "L":
            return [get_data(i) for i in data]
        return item[data_type]

    result = {i: get_data(record[i]) for i in record}
    return result


def build_record(item):
    def build_typed_item(o, dict_as_map=True):
        if isinstance(o, datetime):
            return {"S": o.isoformat()}
        if isinstance(o, bool):
            return {"BOOL": o}
        if isinstance(o, int) or isinstance(o, float) or isinstance(o, Decimal):
            return {"N": str(o)}
        if isinstance(o, dict):
            return {"M": {i: build_typed_item(o[i]) for i in o if o[i] not in [None, ""]}} if dict_as_map else o
        if isinstance(o, list):
            return {"L": [build_typed_item(i) for i in o if i not in [None, ""]]}
        return {"S": str(o)}

    return {attr: build_typed_item(item[attr]) for attr in item if item[attr] not in [None, ""]}


def as_dynamo_safe_types(data):
    def check_attributes(d):
        for attr in d.keys():
            if isinstance(d[attr], datetime):
                d[attr] = d[attr].isoformat()
                continue

            if isinstance(d[attr], basestring) and d[attr].strip() == "":
                del d[attr]
                continue

            if isinstance(d[attr], dict):
                d[attr] = as_dynamo_safe_types(d[attr])
                continue

    if isinstance(data, list):
        for i in data:
            check_attributes(i)
    else:
        check_attributes(data)

    return data
