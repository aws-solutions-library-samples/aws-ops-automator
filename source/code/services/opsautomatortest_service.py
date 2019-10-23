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
import json
import random
import time
from datetime import datetime

import actions.ops_automator_test_action
import services
from scheduling.setbuilder import SetBuilder
from services.aws_service import AwsService


# Test service used for generating test resources used by the Ops Automator test action
class OpsautomatortestService(AwsService):
    """
    This is a pseudo service class to let the scheduler use the current UTC time as a resource
    """

    def __init__(self, role_arn=None, session=None, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        """
        AwsService.__init__(self, service_name='TestService',
                            resource_names=actions.ops_automator_test_action.TEST_RESOURCE_NAMES,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=False,
                            resources_with_tags=actions.ops_automator_test_action.TEST_RESOURCE_NAMES,
                            as_named_tuple=as_named_tuple,
                            service_retry_strategy=service_retry_strategy)
        self._args = None
        self._test_data = None
        self._region = None
        self._tags = None

    @property
    def _number_of_resources(self):

        def get_number_of_resources(spec):
            assert (spec is not None)
            # dict as json, key is region, value is spec for region
            if isinstance(spec, str):
                # noinspection PyBroadException,PyPep8
                try:
                    spec = json.loads(spec)
                    return get_number_of_resources(spec)
                except:
                    return SetBuilder(min_value=0, max_value=actions.ops_automator_test_action.TEST_MAX_RESOURCES).build(spec)

            # number of resources
            if isinstance(spec, int):
                return set([i + 1 for i in range(0, spec)])

            elif isinstance(spec, dict):
                # native dict, key is region, value is spec for region
                if self.region in spec:
                    return get_number_of_resources(spec[self.region])
                if "*" in spec:
                    return get_number_of_resources(spec["*"])
            return set()

        return get_number_of_resources(self._args.get(actions.ops_automator_test_action.PARAM_TEST_RESOURCES, 0))

    @property
    def region(self):
        return self._args["region"] if "region" in self._args else services.get_session().region_name

    @property
    def tags(self):
        if self._tags is None:
            lastkey = None

            tags = {}
            tag_str = self._args.get(actions.ops_automator_test_action.PARAM_TEST_SELECT_TAGS, "")

            if isinstance(tag_str, str):
                for t in tag_str.split(","):
                    t = t.strip()
                    if "=" in t:
                        t = t.partition("=")
                        key = t[0].strip()
                        tags[key] = t[2].strip()
                        lastkey = key
                    elif lastkey is not None:
                        tags[lastkey] = ",".join([tags[lastkey], t])
            self._tags = tags

        return self._tags

    @staticmethod
    def resource_id(i):
        return actions.ops_automator_test_action.RESOURCE_ID_FORMAT.format(i)

    def describe(self, as_tuple=None, **kwargs):
        """
        This method is to retrieve test resources, method parameters are only used signature compatibility
        :param as_tuple: Set to true to return results as immutable named dictionaries instead of dictionaries
        :return: Test resource
        """

        def create_resource(r):
            return {
                actions.ops_automator_test_action.TEST_RESOURCE_ID: OpsautomatortestService.resource_id(r),
                "AwsAccount": self.aws_account,
                "Region": kwargs["region"] if "region" in kwargs else self.region,
                "Service": self.service_name,
                "ResourceTypeName": actions.ops_automator_test_action.TEST_RESOURCE_NAMES[0],
                "Tags": self.tags
            }

        start = datetime.now()

        self._args = kwargs
        result = [create_resource(i) for i in sorted(self._number_of_resources)]

        if self._args.get(actions.ops_automator_test_action.PARAM_TEST_SELECT_FAILING, False) in ["True", True]:
            raise Exception("Selection of resources fails")

        select_time = int(self._args.get(actions.ops_automator_test_action.PARAM_TEST_SELECT_DURATION, 0))

        if select_time != 0:
            variance = float(self._args.get(actions.ops_automator_test_action.PARAM_TEST_SELECT_DURATION_VARIANCE, 0))
            if variance != 0:
                select_time += (random.uniform(variance * -1, variance) * select_time)
            time_spend = (datetime.now() - start).total_seconds()
            if time_spend < select_time:
                time.sleep(select_time - time_spend)

        return result

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
