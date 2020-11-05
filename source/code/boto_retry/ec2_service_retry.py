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
from botocore.exceptions import ClientError, ParamValidationError

from boto_retry.aws_service_retry import AwsApiServiceRetry


class Ec2ServiceRetry(AwsApiServiceRetry):
    """
        Class that extends retry logic with Ec2 specific logic
    """

    def __init__(self, context=None, logger=None, timeout=None, wait_strategy=None, lambda_time_out_margin=10):
        """
        Initialize volume.

        Args:
            self: (todo): write your description
            context: (str): write your description
            logger: (todo): write your description
            timeout: (int): write your description
            wait_strategy: (todo): write your description
            lambda_time_out_margin: (float): write your description
        """
        AwsApiServiceRetry.__init__(
            self,
            call_retry_strategies=None,
            wait_strategy=wait_strategy,
            context=context,
            timeout=timeout,
            logger=logger,
            lambda_time_out_margin=lambda_time_out_margin)

        self._call_retry_strategies += [
            self.snapshot_creation_per_volume_throttles,
            self.resource_limit_exceeded,
            self.request_limit_exceeded
        ]

    @classmethod
    def snapshot_creation_per_volume_throttles(cls, ex):
        """
        Retries in case the snapshot creation rate is exceeded for a volume
        :param ex: Exception to test
        :return: 
        """
        return type(ex) == ClientError and \
               ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) == 400 and \
               "SnapshotCreationPerVolumeRateExceeded" == ex.response.get("Error", {}).get("Code", "")

    @classmethod
    def resource_limit_exceeded(cls, ex):
        """
        Retries in case resource limits are exceeded. 
        :param ex: 
        :return: 
        """
        return type(ex) == ClientError and \
               ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) == 400 and \
               "ResourceLimitExceeded" == ex.response.get("Error", {}).get("Code", "")

    @classmethod
    def request_limit_exceeded(cls, ex):
        """
        Retries in case requests limits are exceeded.
        :param ex: 
        :return: 
        """
        return type(ex) == ClientError and \
               ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) == 503 and \
               "RequestLimitExceeded" == ex.response.get("Error", {}).get("Code", "")

    def can_retry(self, ex):
        """
           Tests if a retry can be done based on the exception of an earlier call
           :param ex: Execution raise by earlier call of the boto3 method
           :return: True if any of the call_retry_strategy returns True, else False
           """
        if type(ex) == ParamValidationError:
            return False
        return AwsApiServiceRetry.can_retry(self, ex)
