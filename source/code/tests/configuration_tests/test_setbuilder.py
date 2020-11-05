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
import string
import unittest

from scheduling.setbuilder import SetBuilder

characters = string.ascii_lowercase
names = [c * 3 for c in characters]
names_cased = [n.capitalize() for n in names]
all_items = set([index for index in range(0, len(names))])


# noinspection PyTypeChecker
class TestSetBuilder(unittest.TestCase):
    def test_name(self):
        """
        Build a name of the test.

        Args:
            self: (todo): write your description
        """
        # names 1 char
        for i, name in enumerate(names):
            self.assertEqual(SetBuilder(names=names).build(name), {i})
        # names 1 char with offset
        for i, name in enumerate(names):
            self.assertEqual(SetBuilder(names=names, offset=1).build(name), {i + 1})

        # names 1 char ignore case
        for i, name in enumerate(names):
            self.assertEqual(SetBuilder(names=names, ignore_case=True).build(name.upper()), {i})

        # names 3
        for i, name in enumerate(names_cased):
            self.assertEqual(SetBuilder(names=names_cased).build(name), {i})

        # names 3, ignore case
        for i, name in enumerate(names):
            self.assertEqual(SetBuilder(names=names_cased, ignore_case=True).build(name), {i})

        # names 3, 1 significant character
        for i, name in enumerate(names):
            self.assertEqual(SetBuilder(names=names_cased, significant_name_characters=1).build(name.upper()), {i})

        # names 3, 1 significant character, ignore case
        for i, name in enumerate(names):
            self.assertEqual(SetBuilder(names=names_cased, significant_name_characters=3).build(name + name), {i})

        # all items passed in as list of strings
        self.assertEqual(SetBuilder(names=names).build(names), all_items)

    def test_value(self):
        """
        Set the test value

        Args:
            self: (todo): write your description
        """
        # all by value
        for value in range(0, len(names)):
            self.assertEqual(SetBuilder(names=names).build(str(value)), {value})

        # all by value with offset
        for value in range(1, len(names) + 1):
            self.assertEqual(SetBuilder(names=names, offset=1).build(str(value)), {value})

    def test_min_max(self):
        """
        Set the min / max value.

        Args:
            self: (todo): write your description
        """
        # builder initialized by min and max values
        for i in range(0, 5):
            self.assertEqual(SetBuilder(min_value=0, max_value=4).build(str(i)), {i})

    def test_wildcards(self):
        """
        Test for all wildcards.

        Args:
            self: (todo): write your description
        """
        # all items using standard and custom wildcard
        self.assertEqual(SetBuilder(names).build("*"), all_items)
        self.assertEqual(SetBuilder(names).build("?"), all_items)
        self.assertEqual(SetBuilder(names, all_items_wildcards="!").build("!"), all_items)

        # first item using standard and custom wildcard
        self.assertEqual(SetBuilder(names).build("^"), {0})
        self.assertEqual(SetBuilder(names, first_item_wildcard="!").build("!"), {0})
        self.assertEqual(SetBuilder(names, offset=1).build("^"), {1})

        # last item using standard and custom wildcard
        self.assertEqual(SetBuilder(names).build("$"), {len(names) - 1})
        self.assertEqual(SetBuilder(names, last_item_wildcard="!").build("!"), {len(names) - 1})
        self.assertEqual(SetBuilder(names, offset=1).build("$"), {len(names)})

        # combined first and last wildcard
        self.assertEqual(SetBuilder(names).build("^,$"), {0, len(names) - 1})
        self.assertEqual(SetBuilder(names).build("^-$"), all_items)

    def test_multiple(self):
        """
        Generate all the test sets of all elements in - place

        Args:
            self: (todo): write your description
        """
        # comma separated list of names
        self.assertEqual(SetBuilder(names).build(",".join(names)), all_items)
        # comma separated list of values
        self.assertEqual(SetBuilder(names).build(",".join([str(i) for i in range(0, len(names))])), all_items)

    def test_ranges(self):
        """
        Create a set of ranges.

        Args:
            self: (todo): write your description
        """
        # name range
        self.assertEqual(SetBuilder(names).build(names[0] + "-" + names[2]), {0, 1, 2})
        # name ranges no overlap
        self.assertEqual(SetBuilder(names).build(names[0] + "-" + names[2] + "," + names[4] + "-" + names[6]), {0, 1, 2, 4, 5, 6})
        # name ranges with overlap
        self.assertEqual(SetBuilder(names).build(names[2] + "-" + names[6] + "," + names[4] + "-" + names[8]),
                         {2, 3, 4, 5, 6, 7, 8})
        # name range with wrap
        self.assertEqual(SetBuilder(names, wrap=True).build(names[-2] + "-" + names[2]), {0, 1, 2, len(names) - 2, len(names) - 1})

        # value range
        self.assertEqual(SetBuilder(names).build("0-2"), {0, 1, 2})
        # value ranges
        self.assertEqual(SetBuilder(names).build("0-3, 9-12"), {0, 1, 2, 3, 9, 10, 11, 12})
        # value ranges with overlap
        self.assertEqual(SetBuilder(names).build("0-8, 6-12"), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        # value range with wrap
        self.assertEqual(SetBuilder(names, wrap=True).build(str(len(names) - 2) + "-2"), {0, 1, 2, len(names) - 2, len(names) - 1})

        self.assertRaises(ValueError, SetBuilder(names, wrap=False).build, str(names[1]) + "-" + names[0])
        self.assertRaises(ValueError, SetBuilder(names, wrap=False).build, "2-1")

    def test_increments(self):
        """
        Create the number of the test sets.

        Args:
            self: (todo): write your description
        """
        # increments on start name and value
        self.assertEqual(SetBuilder(names).build(names[0] + "/5"), {i for i in list(range(0, len(names), 5))})
        self.assertEqual(SetBuilder(names).build("0/3"), {i for i in list(range(0, len(names), 3))})

        # increment on ranges
        self.assertEqual(SetBuilder(names).build(names[0] + "-" + names[10] + "/2"), {0, 2, 4, 6, 8, 10})
        self.assertEqual(SetBuilder(names).build("0-10/3"), {0, 3, 6, 9})
        self.assertEqual(SetBuilder(names, wrap=True).build("10-5/5"), {10, 15, 20, 25, 4})

        # invalid increment numbers
        self.assertRaises(ValueError, SetBuilder(names).build, "0/0")
        self.assertRaises(ValueError, SetBuilder(names).build, "0/!")

        # wrap is false for range
        self.assertRaises(ValueError, SetBuilder(names, wrap=False).build, 10 - 5 / 5)

    def test_unknown_values(self):
        """
        Returns a set of unknown values in this set.

        Args:
            self: (todo): write your description
        """
        # unknown name raises error
        with self.assertRaises(ValueError):
            SetBuilder(names).build("##")

        # unknown value raises error
        with self.assertRaises(ValueError):
            SetBuilder(min_value=0, max_value=1).build("-1")

        # this class has a handler for handling unknown items
        class SetBuilderWithHandler(SetBuilder):
            def _parse_unknown(self, s):
                """
                Parse a unknown type.

                Args:
                    self: (todo): write your description
                    s: (todo): write your description
                """
                return [] if s == "" else None

        self.assertEqual(SetBuilderWithHandler(names).build(""), set())
        self.assertRaises(ValueError, SetBuilderWithHandler(names).build, "unknown")

    def test_custom_parsers(self):
        """
        Initialize custom custom custom custom parsers.

        Args:
            self: (todo): write your description
        """
        class SetBuilderWithCustomPreParser(SetBuilder):
            def __init__(self, value_names):
                """
                Initialize custom custom settings.

                Args:
                    self: (todo): write your description
                    value_names: (str): write your description
                """
                SetBuilder.__init__(self, names=value_names)
                self._pre_custom_parsers = [self._pre_parser]

            # noinspection PyMethodMayBeStatic
            def _pre_parser(self, s):
                """
                Pre_pre_parser

                Args:
                    self: (todo): write your description
                    s: (todo): write your description
                """
                if s == "###":
                    return [0]

        self.assertEqual(SetBuilderWithCustomPreParser("").build("###"), {0})

        class SetBuilderWithCustomPostParser(SetBuilder):

            def __init__(self, nm):
                """
                Do some setup after initialisation.

                Args:
                    self: (todo): write your description
                    nm: (int): write your description
                """
                SetBuilder.__init__(self, names=nm)
                self._post_custom_parsers = [self._post_parser]

            # noinspection PyMethodMayBeStatic
            def _post_parser(self, s):
                """
                Parse the parser.

                Args:
                    self: (todo): write your description
                    s: (todo): write your description
                """
                if s == "!!!":
                    return [1]

        self.assertEqual(SetBuilderWithCustomPostParser("").build("!!!"), {1})

        class SetBuilderWithCustomParsers(SetBuilder):
            def __init__(self, nm):
                """
                Initialize the parser.

                Args:
                    self: (todo): write your description
                    nm: (int): write your description
                """
                SetBuilder.__init__(self, names=nm)
                self._post_custom_parsers = [self._pre_parser, self._post_parser]

            # noinspection PyMethodMayBeStatic
            def _pre_parser(self, s):
                """
                Parse a parser.

                Args:
                    self: (todo): write your description
                    s: (todo): write your description
                """
                if s == "###":
                    return [99]

            # noinspection PyMethodMayBeStatic
            def _post_parser(self, s):
                """
                Parse a parser.

                Args:
                    self: (todo): write your description
                    s: (todo): write your description
                """
                if s == "!!!":
                    return [100]

        self.assertEqual(SetBuilderWithCustomParsers(names).build("###,!!!," + names[0]), {0, 99, 100})

    def test_set_str(self):
        """
        Generate a string representation of set.

        Args:
            self: (todo): write your description
        """
        sep_item = ", "
        sep_range = "-"
        sb = SetBuilder(names)
        # single item
        self.assertEqual(sb.str({0}), names[0])
        # two items
        self.assertEqual(sb.str({0, 3}), names[0] + sep_item + names[3])
        # range
        self.assertEqual(sb.str({0, 1, 2, 3, 4}), names[0] + sep_range + names[4])
        # range and item
        self.assertEqual(sb.str({0, 1, 2, 4}), names[0] + sep_range + names[2] + sep_item + names[4])
        # two ranges
        self.assertEqual(sb.str({0, 1, 3, 4}), names[0] + sep_range + names[1] + sep_item + names[3] + sep_range + names[4])

    def test_exceptions(self):
        """
        Return a list of all the possible possible.

        Args:
            self: (todo): write your description
        """
        # build with invalid param types, must be string or string list
        self.assertRaises(ValueError, SetBuilder(names=names).build, None)
        self.assertRaises(ValueError, SetBuilder(names=names).build, 1)

        # names and max_value combination not allowed
        with self.assertRaises(ValueError):
            SetBuilder(names=names, max_value=1)

        # names and min_value combination not allowed
        with self.assertRaises(ValueError):
            SetBuilder(names=names, min_value=0)

        # both min_value and max_value must be used
        with self.assertRaises(ValueError):
            SetBuilder(min_value=0)

        # both min_value and max_value must be used
        with self.assertRaises(ValueError):
            SetBuilder(max_value=1)

        # max_value must be equal or greater than min_value
        with self.assertRaises(ValueError):
            SetBuilder(min_value=99, max_value=1)

        # offset must be the same if specified with min_value
        with self.assertRaises(ValueError):
            SetBuilder(min_value=0, max_value=1, offset=1)
