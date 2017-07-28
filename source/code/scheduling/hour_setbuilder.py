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
from scheduling.setbuilder import SetBuilder


class HourSetBuilder(SetBuilder):
    """
    Class for building set of hour values 0-23 and am/pm
    """

    def __init__(self):
        SetBuilder.__init__(self, min_value=0, max_value=23, wrap=False)

    def _get_value_by_name(self, name_str):
        # allow usage of am and pm in value strings
        hour = SetBuilder._get_value_by_name(self, name_str)
        if hour is None:
            return self._get_hour_am_pm(name_str)

    def _get_hour_am_pm(self, hour_am_pm_str):
        # process times with am and pm
        if 2 < len(hour_am_pm_str) <= 4:
            s = hour_am_pm_str.lower()
            if s[-2:].lower() in ["am", "pm"]:
                hour = self._get_value_by_str(s[0:len(hour_am_pm_str) - 2])
                if hour is not None:
                    ampm = s[-2:]
                    if ampm == "pm":
                        # 12pm = 12:00
                        if hour != 12:
                            hour += 12
                        # invalid to use pm if hour > 12
                        if hour > 23:
                            raise ValueError("hour {} is not valid".format(hour_am_pm_str))
                    # 12am = 00
                    elif ampm == "am" and hour == 12:
                        hour = 0
                return hour
        return None
