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
import boto3


class Sts(object):

    def __init__(self, session=None):
        self._sts_client = None
        self._session = session

    @property
    def sts_client(self):
        if self._sts_client is None:
            if self._session is None:
                self._session = boto3.Session()
            self._sts_client = self._session.client("sts")
        return self._sts_client

    @property
    def user_id(self):
        return self.sts_client.get_caller_identity().get("UserId")

    @property
    def account(self):
        return self.sts_client.get_caller_identity().get("Account")

    @property
    def identity_arn(self):
        return self.sts_client.get_caller_identity().get("Arn")
