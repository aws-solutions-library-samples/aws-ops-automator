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
from datetime import datetime

from boto_retry import get_client_with_retries

ENV_REPORT_BUCKET = "REPORTING_BUCKET"


def create_output_writer(context=None, logger=None):
    """
    Create a context output_writer : param logger : : return : : return :

    Args:
        context: (todo): write your description
        logger: (todo): write your description
    """
    return ReportOutputWriter(context=context, logger=logger)


class ReportOutputWriter(object):

    def __init__(self, **kwargs):
        """
        Initialize the context.

        Args:
            self: (todo): write your description
        """
        self._context = kwargs.get("context")
        self._logger = kwargs.get("logger")

    def write(self, data, key):
        """
        Write an object to the given key.

        Args:
            self: (todo): write your description
            data: (todo): write your description
            key: (str): write your description
        """
        s3_client = get_client_with_retries("s3", ["put_object"], context=self._context, logger=self._logger)
        s3_client.put_object_with_retries(Bucket=os.getenv(ENV_REPORT_BUCKET), Key=key, Body=data)


def report_key_name(action, account=None, region=None, subject=None, with_task_id=True, ext="csv"):
    """
    Generate the key name for a key.

    Args:
        action: (dict): write your description
        account: (todo): write your description
        region: (str): write your description
        subject: (str): write your description
        with_task_id: (str): write your description
        ext: (str): write your description
    """
    return "{}/{}/{}-{}{}-{}{}{}".format(action.__class__.__name__[0:-len("Action")],
                                         action.get("task"),
                                         account if account is not None else action.get("account"),
                                         region if region is not None else action.get("region"),
                                         "-" + subject if subject is not None else "",
                                         datetime.now().strftime("%Y%m%d%H%M"),
                                         ("-" + action.get("task_id")) if with_task_id is not None else "",
                                         (("." if ext.startswith(".") else "") + ext) if ext not in ["", None] else "")

