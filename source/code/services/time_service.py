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
import datetime

import pytz
import services
from helpers import as_namedtuple
from services.aws_service import AwsService

RESOURCE_NAMES = []


class TimeService(AwsService):
    """
    This is a pseudo service class to let the scheduler use the current UTC time as a resource
    """

    def __init__(self, role_arn=None, session=None, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        """
        AwsService.__init__(self, service_name='time',
                            resource_names=[],
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=False,
                            as_named_tuple=as_named_tuple,
                            service_retry_strategy=service_retry_strategy)

    def describe(self, as_tuple=None, **kwargs):
        """
        This method is to retrieve a pseudo UTC time resource, method parameters are only used signature compatibility
        :param as_tuple: Set to true to return results as immutable named dictionaries instead of dictionaries
        :return: Pseudo time resource
        """

        def use_tuple():
            return (as_tuple is not None and as_tuple) or (as_tuple is None and self._as_tuple)

        region = kwargs.get("region")
        result = {
            "Time": datetime.datetime.now(pytz.timezone("UTC")),
            "AwsAccount": self.aws_account,
            "Region": region if region else services.get_session().region_name
        }

        return [as_namedtuple("Time", result)] if use_tuple() else [result]

    def service_regions(self):
        """
        Regions that can be used for this service, return all AWS regions (assuming they all support EC2)
        :return: Service regions
        """
        return services.get_session().get_available_regions(service_name="ec2")

    def get(self, region=None, as_tuple=None, **kwargs):
        """
        Returns a pseudo time resource containing the current UTC time
        :param region: Not used, copied to resource
        :param as_tuple: Set to true to return results as immutable named dictionaries instead of dictionaries
        :return: Service resource of the specified resource type for the service, None if the resource was not available.
        """
        return self.describe(
            region=region, as_tuple=as_tuple, **kwargs)[0]
