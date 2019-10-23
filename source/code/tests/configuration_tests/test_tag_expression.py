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
import copy
import unittest

from tagging.tag_filter_expression import TagFilterExpression


class TestTagFilterExpression(unittest.TestCase):

    def test_is_match(self):
        tags1 = {"A": "B"}
        tags2 = {"CD": "EFG"}
        tags3 = copy.copy(tags1)
        tags3.update(copy.copy(tags2))

        self.assertTrue(TagFilterExpression("A=B").is_match(tags1))
        self.assertTrue(TagFilterExpression("A=B").is_match(tags3))
        self.assertFalse(TagFilterExpression("A=B").is_match(tags2))
        self.assertTrue(TagFilterExpression("A=B&CD=EFG").is_match(tags3))
        self.assertTrue(TagFilterExpression("A=!B|CD=EFG").is_match(tags3))
        self.assertFalse(TagFilterExpression("A=!B|CD=EFG").is_match(tags1))

        self.assertFalse(TagFilterExpression("A=!B|CD=!EFG").is_match(tags3))
        self.assertFalse(TagFilterExpression("A=!B&CD=!EFG").is_match(tags3))
        self.assertFalse(TagFilterExpression("!A&CD=EFG").is_match(tags3))
        self.assertTrue(TagFilterExpression("!A=B|CD=EFG").is_match(tags3))
        self.assertFalse(TagFilterExpression("A=B&!CD=EFG").is_match(tags3))
        self.assertFalse(TagFilterExpression("!A=B|!CD=E*").is_match(tags3))
        self.assertFalse(TagFilterExpression("!A=B|!CD=E*").is_match(tags1))
        self.assertTrue(TagFilterExpression("(A=X|A=B").is_match(tags1))
        self.assertFalse(TagFilterExpression("(A=X|A=B").is_match(tags2))
        self.assertTrue(TagFilterExpression("(A=X|A=Y|CD=*").is_match(tags3))
        self.assertTrue(TagFilterExpression("(A=B&CD=!XYZ").is_match(tags3))
        self.assertTrue(TagFilterExpression("(A=B&CD=XYZ)|(A=Z|CD=EFG)").is_match(tags3))

        self.assertFalse(TagFilterExpression("A=B&!CD=E*").is_match(tags3))
        self.assertFalse(TagFilterExpression("A=B&!CD=!E*").is_match(tags3))
        self.assertTrue(TagFilterExpression("A=B|!CD=!E*").is_match(tags3))
        self.assertTrue(TagFilterExpression("A=1,2,3").is_match({"A": "1,2,3"}))
        self.assertTrue(TagFilterExpression("A=*").is_match({"A": "1,2,3"}))

    def test_helper_functions(self):
        self.assertEqual(TagFilterExpression("A=B&CD=!XYZ").get_filters(), ["A=B", "CD=!XYZ"])
        self.assertEqual(TagFilterExpression("(A=B&CD=!XYZ)|(A=Z|CD=EFG)").get_filters(), ["A=B", "CD=!XYZ", "A=Z", "CD=EFG"])
        self.assertEqual(TagFilterExpression("A&*=!XYZ").get_filters(), ["A", "*=!XYZ"])

        self.assertEqual(TagFilterExpression("A=B&CD=!XYZ").get_filter_keys(), {"A", "CD"})
        self.assertEqual(TagFilterExpression("(A=B&CD=!XYZ)|(A=Z|CD=EFG)").get_filter_keys(), {"A", "CD"})
        self.assertEqual(TagFilterExpression("A&*=!XYZ").get_filter_keys(), {"A", "*"})
