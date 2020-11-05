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
import time

import boto3

import actions
import handlers

DEFAULT_TIMEOUT = 300


class Context(object):
    def __init__(self, timeout_seconds=DEFAULT_TIMEOUT):
        """
        Initialize the environment.

        Args:
            self: (todo): write your description
            timeout_seconds: (int): write your description
            DEFAULT_TIMEOUT: (int): write your description
        """
        self._started = time.time()
        self._timeout = timeout_seconds

        self.run_local = True
        lambda_function = "{}-{}-{}".format(os.getenv(handlers.ENV_STACK_NAME), os.getenv(handlers.ENV_LAMBDA_NAME),
                                            actions.ACTION_SIZE_STANDARD)

        self.log_group_name = "/aws/lambda/" + lambda_function

        self.invoked_function_arn = "arn:aws:lambda:{}:{}:function:{}".format(boto3.Session().region_name,
                                                                         os.getenv(handlers.ENV_OPS_AUTOMATOR_ACCOUNT),
                                                                         lambda_function)

    def get_remaining_time_in_millis(self):
        """
        Returns the number of remaining remaining remaining remaining in the future.

        Args:
            self: (todo): write your description
        """
        return max(self._timeout - (time.time() - self._started), 0) * 1000
