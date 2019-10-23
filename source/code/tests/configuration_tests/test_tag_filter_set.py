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

from tagging.tag_filter_set import TagFilterSet


class TestTagFilterSet(unittest.TestCase):
    tags1 = {"A": "B"}
    tags2 = {"CD": "EFG"}
    tags3 = copy.deepcopy(tags1)
    tags3.update(copy.deepcopy(tags2))

    def test_equal_single(self):
        self.assertEqual(TagFilterSet("A=B").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=B").pairs_matching_any_filter(self.tags3), self.tags1)
        self.assertEqual(TagFilterSet("*").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("*=*").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A").pairs_matching_any_filter(self.tags3), self.tags1)
        self.assertEqual(TagFilterSet("CD=EFG").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("C*=").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("C*=EFG").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("*=EFG").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("CD=*G").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("CD=E*").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("CD=*F*").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("*").pairs_matching_any_filter(self.tags3), self.tags3)
        self.assertEqual(TagFilterSet("*=*").pairs_matching_any_filter(self.tags3), self.tags3)
        self.assertEqual(TagFilterSet("*").pairs_matching_any_filter({}), {})
        self.assertEqual(TagFilterSet("A=B").pairs_matching_any_filter({}), {})

        self.assertEqual(TagFilterSet("A=\\B").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=\\.").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=\.{1,}").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=\.{1,}").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("\\C.=EFG").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("\\C.=\^EFG$").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("\\C.=\.{1,3}").pairs_matching_any_filter(self.tags3), self.tags2)

        self.assertEqual(TagFilterSet("").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("!").pairs_matching_any_filter(self.tags1), self.tags1)

    def test_not_equal_single(self):
        self.assertEqual(TagFilterSet("X=Y").pairs_matching_any_filter(self.tags1), {})
        self.assertNotEqual(TagFilterSet("X=Y").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("X=*").pairs_matching_any_filter(self.tags3), {})
        self.assertEqual(TagFilterSet("*=Y").pairs_matching_any_filter(self.tags3), {})
        self.assertEqual(TagFilterSet("C*=Y").pairs_matching_any_filter(self.tags3), {})
        self.assertEqual(TagFilterSet("*=*Y*").pairs_matching_any_filter(self.tags3), {})

        self.assertEqual(TagFilterSet("A=\\C").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("A=\\d").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("A=\.{2,}").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("\\d=").pairs_matching_any_filter(self.tags3), {})
        self.assertEqual(TagFilterSet("\.{4,}").pairs_matching_any_filter(self.tags3), {})

    def test_multiple(self):
        self.assertEqual(TagFilterSet("A=B,CD=EFG").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=B,X=Y").pairs_matching_any_filter(self.tags3), self.tags1)
        self.assertEqual(TagFilterSet("CD=EFG,X=Y").pairs_matching_any_filter(self.tags3), self.tags2)
        self.assertEqual(TagFilterSet("A=B,CD=EFG").pairs_matching_any_filter(self.tags3), self.tags3)
        self.assertEqual(TagFilterSet("X=Y,Z=Z").pairs_matching_any_filter(self.tags3), {})
        self.assertEqual(TagFilterSet("A=B,CD=EFG").pairs_matching_any_filter({}), {})

    def test_not_operator(self):
        self.assertEqual(TagFilterSet("!A=B").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("!A").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("!Z").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("!Z=B").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("!Z,A").pairs_matching_any_filter(self.tags3), self.tags3)
        self.assertEqual(TagFilterSet("!Z,!A").pairs_matching_any_filter(self.tags3), self.tags3)

        self.assertEqual(TagFilterSet("!\d=B").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("!A=!\w").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("!A=!\d").pairs_matching_any_filter(self.tags1), {})

    def test_not_equal_operator(self):
        self.assertEqual(TagFilterSet("A=!B").pairs_matching_any_filter(self.tags1), {})
        self.assertNotEqual(TagFilterSet("A=!B").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("*=!B").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("*=!B").pairs_matching_any_filter(self.tags3), self.tags2)

        self.assertEqual(TagFilterSet("\A=!Z").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=!\Z").pairs_matching_any_filter(self.tags1), self.tags1)
        self.assertEqual(TagFilterSet("A=!\.").pairs_matching_any_filter(self.tags1), {})

    def test_not_equal_and_not_operator(self):
        self.assertEqual(TagFilterSet("!Z=!B").pairs_matching_any_filter(self.tags1), {})
        self.assertEqual(TagFilterSet("!Z=!Y").pairs_matching_any_filter(self.tags1), self.tags1)

    def test_exceptions(self):
        bad_filter = TagFilterSet("\\.(")
        self.assertRaises(ValueError, bad_filter.pairs_matching_any_filter, self.tags1)

    def other_tests(self):
        self.assertTrue(TagFilterSet("!A=B").has_not_operator())
        self.assertFalse(TagFilterSet("A=B").has_not_operator())
        self.assertTrue(TagFilterSet("A=B,!C=D").has_not_operator())
        self.assertTrue(TagFilterSet("!A=B,!C=D").has_not_operator())
        self.assertFalse(TagFilterSet("A=!B").has_not_operator())

        self.assertEqual(TagFilterSet("A=B").tag_names(), ["A"])
        self.assertEqual(TagFilterSet("!A=B").tag_names(), ["A"])
        self.assertEqual(TagFilterSet("A=B,C,D=E").tag_names(), ["A", "C", "D"])
        self.assertEqual(TagFilterSet("A=B,!C,D=E").tag_names(), ["A", "C", "D"])
