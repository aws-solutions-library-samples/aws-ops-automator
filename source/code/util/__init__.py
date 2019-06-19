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
import json

from util.custom_encoder import CustomEncoder


ENV_METRICS_URL = "METRICS_URL"
# Solution ID
ENV_SOLUTION_ID = "SOLUTION_ID"
# Switch for sending anonymous metrics
ENV_SEND_METRICS = "SEND_METRICS"


def pascal_to_snake_case(s):
    return s[0].lower() + "".join(
        [i if i.islower() or i.isdigit() or i == "_" else "_" + i.lower() for i in s[1:]])


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
