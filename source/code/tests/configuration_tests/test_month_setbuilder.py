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

from scheduling.month_setbuilder import MonthSetBuilder


class TestMonthSetBuilder(unittest.TestCase):
    def test_name(self):
        # abbreviations
        for i, name in enumerate(calendar.month_abbr[1:]):
            self.assertEqual(MonthSetBuilder().build(name), {i + 1})
            self.assertEqual(MonthSetBuilder().build(name.lower()), {i + 1})
            self.assertEqual(MonthSetBuilder().build(name.upper()), {i + 1})

        # full names
        for i, name in enumerate(calendar.month_name[1:]):
            self.assertEqual(MonthSetBuilder().build(name), {i + 1})
            self.assertEqual(MonthSetBuilder().build(name.lower()), {i + 1})
            self.assertEqual(MonthSetBuilder().build(name.upper()), {i + 1})

    def test_value(self):
        for i in range(1, 12):
            self.assertEqual(MonthSetBuilder().build(str(i)), {i})
