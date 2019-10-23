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
from boto_retry.aws_service_retry import AwsApiServiceRetry


class CloudWatchLogsServiceRetry(AwsApiServiceRetry):

    # noinspection PyUnusedLocal
    def __init__(self, logger=None, context=None, timeout=15, wait_strategy=None, lambda_time_out_margin=10):
        """
        Initializes retry logic
        :param wait_strategy: Wait strategy that returns retry wait periods
        :param context: Lambda context that is used to calculate remaining execution time
        :param timeout: Timeout for method call. This time can not exceed the remaining time if a method is called
        within the context of a lambda function.
        :param lambda_time_out_margin: If called within the context of a Lambda function this time should at least be 
        remaining before making a retry. This is to allow possible cleanup and logging actions in the remaining time
        """
        AwsApiServiceRetry.__init__(
            self,
            call_retry_strategies=None,
            wait_strategy=wait_strategy,
            context=context,
            timeout=timeout,
            logger=None,
            lambda_time_out_margin=lambda_time_out_margin)




