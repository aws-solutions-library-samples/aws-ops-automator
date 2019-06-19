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

import re

WILDCARD_CHAR = "*"
REGEX_CHAR = "\\"
FILTER_SEP = ","
NAME_VAL_SEP = "="
NOT_OPERATOR = "!"


class TagFilterSet(object):
    """
    Class for matching string and value pairs against a set of filters
    Filters are specified in a comma separated list of one or more filter.
    Filters can start, end or start and end with a * wildcard character for partial matching
    if the filter starts with a \ character it is a regular expression.
    For filtering key-value pairs set in dictionaries the format of the filters is <namefilter>=<valuefilter> where
    both items can start/end with a wildcard or with a \ for regular expressions. If the item has the
    format <namefilter> then the pairs will match this filter if the just keyname matches the expression.
    """

    def __init__(self, filters, name_val_sep=NAME_VAL_SEP, filter_sep=FILTER_SEP, regex_char=REGEX_CHAR,
                 wildcard_char=WILDCARD_CHAR):
        if filters.startswith("\\") or "=\\" in filters or "!=\\" in filters:
            self._filters = [filters.strip()]
        else:
            self._filters = [f.strip() for f in filters.split(filter_sep)]

        self._name_val_sep = name_val_sep
        self._regex_char = regex_char
        self._wildcard_char = wildcard_char

    # matches a single string against a single filter
    def match_string(self, filter_string, tested_string):
        """
        Matches a single string against a single filter
        :param filter_string: string containing the filer to match against. The string can start or end with a wildcard
        character, contain
        just the wildcard character of a regular expression starting with a \ character.
        :param tested_string: The string to test
        :return: True id the string matches, False if not
        """

        # empty or none matches empty or none
        if filter_string == "" or filter_string is None:
            return tested_string == "" or tested_string is None

        # filter is regex
        try:
            if filter_string.startswith(self._regex_char) and len(filter_string) > 1:
                return re.match(filter_string[1:], tested_string) is not None
        except re.error as ex:
            raise ValueError("\"{}\" is not a valid regular expression ({})".format(filter_string[1:], ex))

        # just "*" matches any value
        if filter_string == self._wildcard_char:
            return True

        if filter_string.startswith(self._wildcard_char):
            if filter_string.endswith(self._wildcard_char):
                # *contained*
                return filter_string[1:-1] in tested_string
            else:
                # *endswith
                return tested_string.endswith(filter_string[1:])

        if filter_string.endswith(self._wildcard_char):
            # startswith*
            return tested_string.startswith(filter_string[:-1])
        else:
            # exact match
            return filter_string == tested_string

    def tag_names(self):
        names = [f.split(self._name_val_sep)[0] for f in self._filters]
        return [n[1:] if n.startswith(NOT_OPERATOR) else n for n in names]

    def matches_name_value_pair(self, filter_str, pair_key, pair_value):
        """
        Matches a filter against a name value pair
        :param filter_str: filter in the format [!]<key>[!]=<value> or [!]<key> where both key and value can contain
        wildcards or a regex.
        :param pair_key: keyname to test
        :param pair_value: value to test
        :return: True if the filter matched the key pair, False if not
        """

        tag_name, tag_value, not_equal, not_tag = self._split_filter(filter_str)
        if self.match_string(tag_name, pair_key) != not_tag:
            return len(tag_value) == 0 or (self.match_string(tag_value, pair_value) != not_equal)
        return False

    def pairs_matching_any_filter(self, key_pairs):
        """
        Selects key value pairs match any of a set of name value filters
        :param key_pairs: Dictionary containing the pairs to test
        :return: Dictionary containing all keypairs that match at least one filter
        """
        result = {}
        for key_name in key_pairs:
            if any([self.matches_name_value_pair(f, key_name, key_pairs[key_name]) for f in self._filters]):
                result[key_name] = key_pairs[key_name]
        return result

    def all_pairs_matching_filter(self, key_pairs):
        for key_name in key_pairs:
            if not any([self.matches_name_value_pair(f, key_name, key_pairs[key_name]) for f in self._filters]):
                return False
        return True

    @classmethod
    def _split_filter(cls, filter_str):
        not_equal = False
        not_tag = False

        temp = filter_str.split(NAME_VAL_SEP, 1)
        tag_name = temp[0].strip()
        if tag_name.startswith(NOT_OPERATOR):
            not_tag = True
            tag_name = tag_name[1:].strip()

        if len(temp) > 1:
            tag_value = temp[1].strip()
            if len(tag_value) > 1:
                if tag_value[0] == NOT_OPERATOR:
                    not_equal = True
                    tag_value = tag_value[1:].strip()
        else:
            tag_value = ""
        return tag_name, tag_value, not_equal, not_tag

    def has_not_operator(self):
        for f in self._filters:
            if f.startswith(NOT_OPERATOR):
                return True
        return False
