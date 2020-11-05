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

from scheduling.weekday_setbuilder import WeekdaySetBuilder


class TestMonthdaySetBuilder(unittest.TestCase):
    def test_name(self):
        """
        Return the name of the calendar.

        Args:
            self: (todo): write your description
        """
        for i, day_name in enumerate(calendar.day_abbr):
            self.assertEqual(WeekdaySetBuilder().build(day_name), {i})

        for i, day_name in enumerate(calendar.day_name):
            self.assertEqual(WeekdaySetBuilder().build(day_name), {i})

    def test_value(self):
        """
        Set the test value.

        Args:
            self: (todo): write your description
        """
        for i in range(0, len(calendar.day_abbr) - 1):
            self.assertEqual(WeekdaySetBuilder().build(str(i)), {i})

    def test_L_wildcard(self):
        """
        Build a l - day of - related year.

        Args:
            self: (todo): write your description
        """

        for year in [2016, 2017]:
            for month in range(1, 13):
                weekday, days_in_month = calendar.monthrange(year, month)
                for tested_on_day in range(1, days_in_month + 1):
                    builder = WeekdaySetBuilder(year=year, month=month, day=tested_on_day)

                    # test by name of weekday
                    day_num_l = calendar.day_abbr[weekday] + "L"
                    tested_by_name = builder.build(day_num_l)
                    # test by number of weekday
                    day_value_l = str(weekday) + "L"
                    tested_by_value = builder.build(day_value_l)

                    # everything before last week should be empty set
                    if tested_on_day <= (days_in_month - 7):
                        self.assertEqual(tested_by_name, set())
                        self.assertEqual(tested_by_value, set())
                    else:
                        # in last week the set should contain the day
                        self.assertEqual(tested_by_name, {weekday})
                        self.assertEqual(tested_by_value, {weekday})

                        # test if other weekdays on that day return empty set
                        for d in range(0, 6):
                            if d != weekday:
                                day_num_l = calendar.day_abbr[d] + "L"
                                day_value_l = str(d) + "L"
                                self.assertEqual(builder.build(day_num_l), set())
                                self.assertEqual(builder.build(day_value_l), set())

                    weekday = (weekday + 1) % 7

    def test_weekday_numbered(self):
        """
        Calculate the weekday of the given year.

        Args:
            self: (todo): write your description
        """

        for year in [2016, 2017]:
            for month in range(1, 13):
                weekday, days_in_month = calendar.monthrange(year, month)

                for day in range(1, days_in_month + 1):
                    num = int((day - 1) / 7) + 1
                    builder = WeekdaySetBuilder(year=year, month=month, day=day)

                    tested_by_name = builder.build(calendar.day_abbr[weekday] + "#" + str(num))
                    self.assertEqual(tested_by_name, {weekday})

                    tested_by_value = builder.build(str(weekday) + "#" + str(num))
                    self.assertEqual(tested_by_value, {weekday})

                    for other_weekday in range(0, 7):
                        if other_weekday != weekday:
                            tested_by_name = builder.build(calendar.day_abbr[other_weekday] + "#" + str(num))
                            self.assertEqual(tested_by_name, set())
                            tested_by_value = builder.build(str(other_weekday) + "#" + str(num))
                            self.assertEqual(tested_by_value, set())

                    for other_num in range(1, 6):
                        if num != other_num:
                            tested_by_name = builder.build(calendar.day_abbr[weekday] + "#" + str(other_num))
                            self.assertEqual(tested_by_name, set())
                            tested_by_value = builder.build(str(weekday) + "#" + str(other_num))
                            self.assertEqual(tested_by_value, set())

                    weekday = (weekday + 1) % 7

    def test_exceptions(self):
        """
        Set the day of the day for each day.

        Args:
            self: (todo): write your description
        """
        # L needs year, month and days params
        self.assertRaises(ValueError, WeekdaySetBuilder().build, "1L")
        self.assertRaises(ValueError, WeekdaySetBuilder(year=2016, month=10, day=4).build, "0#6")
        self.assertRaises(ValueError, WeekdaySetBuilder(year=2016, month=10, day=4).build, "0#0")
