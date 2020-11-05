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

import services.kms_service


class Kms(object):

    def __init__(self, region=None, session=None):
        """
        Initialize a boto3 client.

        Args:
            self: (todo): write your description
            region: (str): write your description
            session: (todo): write your description
        """
        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.kms_client = self.session.client("kms", region_name=self.region)
        self.kms_service = services.create_service("kms", session=self.session)

    def get_kms_key(self, keyid):
        """
        Get the kms for a key.

        Args:
            self: (todo): write your description
            keyid: (str): write your description
        """
        try:
            key = self.kms_service.get(services.kms_service.KEY,
                                       region=self.region,
                                       KeyId=keyid)
            return key
        except Exception as ex:
            if getattr(ex, "response", {}).get("Error", {}).get("Code") == "NotFoundException":
                if not keyid.startswith("arn") and not keyid.startswith("alias/"):
                    return self.get_kms_key("alias/" + keyid)
                return None
            else:
                raise ex
