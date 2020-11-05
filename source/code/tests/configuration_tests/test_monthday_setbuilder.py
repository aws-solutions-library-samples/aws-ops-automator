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
import calendar
import unittest

from scheduling.monthday_setbuilder import MonthdaySetBuilder


class TestMonthdaySetBuilder(unittest.TestCase):
    def test_name(self):
        """
        Return the name of the calendar.

        Args:
            self: (todo): write your description
        """
        years = [2016, 2017]  # leap and normal year

        for year in years:
            for month in range(1, 13):
                _, days = calendar.monthrange(year, month)

                for day in range(1, days):
                    self.assertEqual(MonthdaySetBuilder(year, month).build(str(day)), {day})

    def test_L_wildcard(self):
        """
        Test for each day.

        Args:
            self: (todo): write your description
        """
        years = [2016, 2017]  # leap and normal year

        for year in years:
            for month in range(1, 13):
                _, days = calendar.monthrange(year, month)
                self.assertEqual(MonthdaySetBuilder(year, month).build("L"), {days})

    def test_W_wildcard(self):
        """
        Test if the given day.

        Args:
            self: (todo): write your description
        """
        years = [2016, 2017]  # leap and normal year

        for year in years:
            for month in range(1, 13):
                _, days = calendar.monthrange(year, month)

                for day in range(1, days):
                    weekday = calendar.weekday(year, month, day)
                    result = day
                    if weekday == 5:
                        result = day - 1 if day > 1 else day + 2
                    elif weekday == 6:
                        result = day + 1 if day < days else day - 2

                    self.assertEqual(MonthdaySetBuilder(year, month).build(str(day) + "W"), {result})

    def test_exceptions(self):
        """
        Test for every day of every day.

        Args:
            self: (todo): write your description
        """
        for h in range(13, 25):
            self.assertRaises(ValueError, MonthdaySetBuilder(2016, 1).build, "W")
            self.assertRaises(ValueError, MonthdaySetBuilder(2016, 1).build, "32W")
