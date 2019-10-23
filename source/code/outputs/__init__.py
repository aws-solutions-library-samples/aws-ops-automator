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

import inspect
import sys


def get_error_constant_name(scope, message, prefix):
    for g in [n for n in scope if n.startswith(prefix)]:
        if isinstance(scope[g], str) and scope[g] == message:
            return g
    return None


def get_extended_info(error_message, prefix):
    # noinspection PyProtectedMember
    caller_stack_frame = sys._getframe(2)
    caller = caller_stack_frame.f_code.co_name
    line = caller_stack_frame.f_lineno
    module = inspect.getmodule(caller_stack_frame)
    error_code = get_error_constant_name(module.__dict__, error_message, prefix)
    if error_code is None:
        error_code = get_error_constant_name(caller_stack_frame.f_globals, error_message, prefix)

    result = {
        "Caller": caller,
        "Module": module.__name__,
        "Line": line
    }
    if error_code is not None:
        result["Code"] = error_code

    return result


def raise_value_error(msg, *args):
    s = msg if len(args) == 0 else msg.format(*args)
    ext_error_info = get_extended_info(msg, "ERR")
    code = ext_error_info.get("Code", None)
    if code is not None:
        s = "{} : {}".format(code, s)
    raise ValueError(s)


def raise_exception(msg, *args):
    s = msg if len(args) == 0 else msg.format(*args)
    ext_error_info = get_extended_info(msg, "ERR")
    code = ext_error_info.get("Code", None)
    if code is not None:
        s = "{} : {}".format(code, s)
    raise Exception(s)
