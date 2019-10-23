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
import collections
import decimal
import json
import sys
import traceback
import types
from datetime import datetime


def pascal_to_snake_case(s):
    return s[0].lower() + "".join(
        [i if i.islower() or i.isdigit() or i == "_" else "_" + i.lower() for i in s[1:]])


def pascal_to_dash_case(s):
    return s[0].lower() + "".join(
        [i if i.islower() or i.isdigit() or i == "-" else "-" + i.lower() for i in s[1:]])


def snake_to_pascal_case(s):
    pascal = s[0].upper()
    is_ = False
    for i in range(1, len(s)):
        if s[i] != "_":
            pascal += s[i] if not is_ else s[i].upper()
            is_ = False
        else:
            is_ = True
    return pascal


def safe_dict(o):
    """
    Returns dictionary that can be serialized safely
    :param o: input "un-safe" dictionary
    :return: safe output dictionary
    """
    return json.loads(safe_json(o))


def safe_json(d, indent=0):
    """
    Returns a json document, using a custom encoder that converts all data types not supported by json
    :param d: input dictionary
    :param indent: indent level for output document
    :return: json document for input dictionary
    """
    return json.dumps(d, cls=CustomEncoder, indent=indent)


def is_dict(o):
    return isinstance(o, type({}))


def is_array(o):
    return isinstance(o, type([]))


def tuple_name_func(name):
    result = "".join([c if c.isalnum() or c == '_' else "" for c in name.strip()])
    while result.startswith("_") or result[0].isdigit():
        result = result[1:]
    return result


def as_namedtuple(name, d, deep=True, name_func=None, excludes=None):
    name_func = name_func if name_func is not None else tuple_name_func

    if not isinstance(d, dict) or getattr(d, "keys") is None:
        return d

    if excludes is None:
        excludes = []

    dest = {}

    if deep:
        # deep copy to avoid modifications on input dictionaries
        for key in list(d.keys()):
            key_name = name_func(key)
            if is_dict(d[key]) and key not in excludes:
                dest[key_name] = as_namedtuple(key, d[key], deep=True, name_func=name_func, excludes=excludes)
            elif is_array(d[key]) and key not in excludes:
                dest[key_name] = [as_namedtuple(key, i, deep=True, name_func=name_func, excludes=excludes) for i in d[key]]
            else:
                dest[key_name] = d[key]
    else:
        dest = {name_func(key): d[key] for key in list(d.keys())}

    return collections.namedtuple(name_func(name), list(dest.keys()))(*list(dest.values()))


def full_stack():
    exc = sys.exc_info()[0]
    stack = traceback.extract_stack()[:-1]
    if exc is not None:  # i.e. if an exception is present
        del stack[-1]
    trace = "Traceback (most recent call last):\n"
    stack_str = trace + ''.join(traceback.format_list(stack))
    if exc is not None:
        stack_str += '  ' + traceback.format_exc().lstrip(trace)
    return stack_str


class CustomEncoder(json.JSONEncoder):
    """
    Internal class used for serialization of types not supported in json.
    """

    def default(self, o):
        if types.FunctionType == type(o):
            return o.__name__
        # sets become lists
        if isinstance(o, set):
            return list(o)
        # date times become strings
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, decimal.Decimal):
            return float(o)
        if isinstance(o, type):
            return str(o)
        if isinstance(o, Exception):
            return str(o)
        if isinstance(o, set):
            return str(o, 'utf-8')
        if isinstance(o, bytes):
            return str(o, 'utf-8')
        
        return json.JSONEncoder.default(self, o)
