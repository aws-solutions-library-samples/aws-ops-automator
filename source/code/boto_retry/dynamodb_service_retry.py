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
from boto_retry.aws_service_retry import AwsApiServiceRetry


class DynamoDbServiceRetry(AwsApiServiceRetry):
    """
    Class that extends retry logic with DynamoDB specific logic
    """

    def __init__(self, context=None, logger=None, timeout=None, wait_strategy=None, lambda_time_out_margin=10):
        AwsApiServiceRetry.__init__(
            self,
            call_retry_strategies=None,
            wait_strategy=wait_strategy,
            context=context,
            timeout=timeout,
            logger=logger,
            lambda_time_out_margin=lambda_time_out_margin)

        self._call_retry_strategies += [
            self.dynamo_throughput_exceeded,
            self.dynamo_resource_in_use,
            self.dynamo_connection_reset_by_peer
        ]

    @classmethod
    def dynamo_throughput_exceeded(cls, ex):
        """
        Adds retry logic on top of the retry logic already done by boto3 if max throughput is exceeded for a table or index
        :param ex: Exception to test
        :return: 
        """
        return type(ex).__name__ == "ProvisionedThroughputExceededException"

    @classmethod
    def dynamo_resource_in_use(cls, ex):
        return type(ex).__name__ == "ResourceInUseException"

    @classmethod
    def dynamo_connection_reset_by_peer(cls, ex):
        return "Connection reset by peer" in str(ex)
