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


class TagFilterSet:
    """
    Class for matching string and value pairs against a set of filters
    Filters are specified in a comma separated list of one or more filter.
    Filters can start, end or start and end with a * wildcard character for partial matching
    if the filter starts with a \ character it is a regular expression.
    For filtering key-value pairs set in dictionaries the format of the filters is <namefilter>=<valuefilter> where
    both items can start/end with a wildcard or with a \ for regular expressions. If the item has the
    format <namefilter> then the pairs will match this filter if the just keyname matches the expression.
    """

    def __init__(self, filters, name_val_sep="=", filter_sep=",", regex_char="\\", wildcard_char="*"):
        self._filters = filters.split(filter_sep)
        self._name_val_sep = name_val_sep
        self._regex_char = regex_char
        self._wildcard_char = wildcard_char

    # matches a single string against a single filter
    def match_string(self, filter_string, s):
        """
        Matches a single string against a single filter
        :param filter_string: string containing the filer to match against. The string can start or end with a wildcard
        character, contain
        just the wildcard character of a regular expression starting with a \ character.
        :param s: The string to test
        :return: True id the string matches, False if not
        """

        # empty or none matches empty or none
        if filter_string == "" or filter_string is None:
            return s == "" or s is None

        # filter is regex
        try:
            if filter_string.startswith(self._regex_char) and len(filter_string) > 1:
                return re.match(filter_string[1:], s) is not None
        except re.error as ex:
            print("\"{}\" is not a valid regular expression ({})".format(filter_string[1:], ex))
            return False

        # just "*" matches any value
        if filter_string == self._wildcard_char:
            return True

        if filter_string.startswith(self._wildcard_char):
            if filter_string.endswith(self._wildcard_char):
                # *contained*
                return filter_string[1:-1] in s
            else:
                # *endswith
                return s.endswith(filter_string[1:])

        if filter_string.endswith(self._wildcard_char):
            # startswith*
            return s.startswith(filter_string[:-1])
        else:
            # exact match
            return filter_string == s

    def matches_name_value_pair(self, filterstring, name, value):
        """
        Matches a filter against a name value pair
        :param filterstring: filter in the format <key>=<value> or <key> where both key and value can contain wildcards or a regex.
        :param name: keyname to test
        :param value: value to test
        :return: True if the filter matched the key pair, False if not
        """
        filter_parts = filterstring.split(self._name_val_sep, 1)
        if self.match_string(filter_parts[0], name):
            return len(filter_parts) == 1 or self.match_string(filter_parts[1], value)
        return False

    def string_matches_any_filter(self, s):
        """
        Tests if string matches any string filter in a set of filters(OR)
        :param s: string to test
        :return: True if the string matches any of the filters
        """
        for f in self._filters:
            if self.match_string(f, s):
                return True
        return False

    def string_matches_all_filters(self, s):
        """
        Test if string matches all string filters in a set of filters(AND)
        :param s: string to test
        :return: True if the string matches all of the filters
        """
        for f in self._filters:
            if not self.match_string(f, s):
                return False
        return True

    def strings_matching_any_filter(self, filter_list):
        """
        Tests which strings in a list of strings matches any of a set of string filters
        :param filter_list: List of strings to test against a set of string filters
        :return: list of strings matching any of the string filters
        """
        result = []
        for s in filter_list:
            if any([self.match_string(f, s) for f in self._filters]):
                result.append(s)
        return result

    def strings_matching_all_filters(self, filter_list):
        """
        Tests which strings in a list of strings matches any of a set of string filters
        :param filter_list: List of strings to test against a set of string filters
        :return: list of strings matching all of the string filters
        """
        result = []
        for s in filter_list:
            if all([self.match_string(f, s) for f in self._filters]):
                result.append(s)
        return result

    def pairs_matching_any_filter(self, pairs):
        """
        Selects key value pairs match any of a set of name value filters
        :param pairs: Dictionary containing the pairs to test
        :return: Dictionary containing all keypairs that match at least one filter
        """
        result = {}
        for name in pairs:
            if any([self.matches_name_value_pair(f, name, pairs[name]) for f in self._filters]):
                result[name] = pairs[name]
        return result

    # pairs that match all filters (AND)
    def pairs_matching_all_filters(self, pairs):
        """
        Selects key value pairs match all of a set of name value filters
        :param pairs: Dictionary containing the pairs to test
        :return: Dictionary containing all keypairs that match all filters
        """
        result = {}
        for name in pairs:
            if all([self.matches_name_value_pair(f, name, pairs[name]) for f in self._filters]):
                result[name] = pairs[name]
        return result
