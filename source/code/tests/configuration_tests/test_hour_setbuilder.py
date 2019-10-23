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
import unittest

from scheduling.hour_setbuilder import HourSetBuilder


class TestHourSetBuilder(unittest.TestCase):
    def test_name(self):
        for i in range(0, 24):
            self.assertEqual(HourSetBuilder().build(str(i)), {i})

        for i in range(1, 11):
            self.assertEqual(HourSetBuilder().build(str(i) + "am"), {i})
            self.assertEqual(HourSetBuilder().build(str(i) + "AM"), {i})

        for i in range(1, 11):
            self.assertEqual(HourSetBuilder().build(str(i) + "pm"), {i + 12})
            self.assertEqual(HourSetBuilder().build(str(i) + "PM"), {i + 12})

        self.assertEqual(HourSetBuilder().build("12am"), {0})
        self.assertEqual(HourSetBuilder().build("12pm"), {12})

    def test_exceptions(self):
        for h in range(13, 25):
            self.assertRaises(ValueError, HourSetBuilder().build, (str(h) + "PM"))
            self.assertRaises(ValueError, HourSetBuilder().build, "PM")

        self.assertRaises(ValueError, HourSetBuilder().build, "24")
        self.assertRaises(ValueError, HourSetBuilder().build, "-1")
