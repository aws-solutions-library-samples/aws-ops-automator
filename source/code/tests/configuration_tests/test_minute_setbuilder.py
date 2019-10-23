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

from scheduling.minute_setbuilder import MinuteSetBuilder


class TestMinuteSetBuilder(unittest.TestCase):
    def test_name(self):
        for i in range(0, 59):
            self.assertEqual(MinuteSetBuilder().build(str(i)), {i})

    def test_exceptions(self):
        self.assertRaises(ValueError, MinuteSetBuilder().build, "60")
