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
import os
import uuid
from datetime import datetime

import requests

import metrics
from helpers import safe_json

INF_METRICS_DATA = "Sending anonymous metrics data\n {}"
INF_METRICS_DATA_SENT = "Metrics data send, status code is {}, message is {}"
INF_SENDING_METRICS_FAILED = "Failed send metrics data ({})"
WARN_ENV_METRICS_URL_NOT_SET = "Environment variable {} is not set, metrics dat is not sent"
WARN_SOLUTION_ID_NOT_SET = "Solution id is not set, metrics are not sent"


def allow_send_metrics():
    """
    Tests if anonymous metrics can be send
    :return: True if metrics can be send
    """
    return str(os.getenv(metrics.ENV_SEND_METRICS, "false")).lower() == "true"


def send_metrics_data(metrics_data, logger):
    url = os.getenv(metrics.ENV_METRICS_URL, None)
    if url is None:
        logger.warning(WARN_ENV_METRICS_URL_NOT_SET, metrics.ENV_METRICS_URL)
        return

    solution_id = os.getenv(metrics.ENV_SOLUTION_ID, None)
    if solution_id is None:
        logger.warning(WARN_SOLUTION_ID_NOT_SET)
        return

    data_dict = {
        "TimeStamp": str(datetime.utcnow().isoformat()),
        "UUID": str(uuid.uuid4()),
        "Data": metrics_data,
        "Solution": solution_id,
    }

    data_json = safe_json(data_dict, indent=3)
    logger.info(INF_METRICS_DATA, data_json)

    headers = {
        'content-type': 'application/json',
        "content-length": str(len(data_json))
    }

    try:
        response = requests.post(url, data=data_json, headers=headers)
        response.raise_for_status()
        logger.info(INF_METRICS_DATA_SENT, response.status_code, response.text)
    except Exception as exc:
        logger.info(INF_SENDING_METRICS_FAILED, str(exc))
