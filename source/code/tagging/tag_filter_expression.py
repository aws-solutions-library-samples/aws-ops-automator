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

from tag_filter_set import TagFilterSet

EXPRESSION_OR = "|"
EXPRESSION_AND = "&"
EXPRESSION_ESCAPE_CHARACTER = '%'


class TagFilterExpression(object):
    def __init__(self, filter_expression):
        self.filter_expression = filter_expression
        self.not_matching = None

    @classmethod
    def split_expression(cls, s):

        def next_char():
            return s[i] if i < len(s) else None

        group_level = 0
        elements = []
        i = 0
        element = ""

        c = next_char()

        escape = c == EXPRESSION_ESCAPE_CHARACTER

        while c is not None:

            if not escape:
                if c == "(":
                    group_level += 1
                elif c == ")":
                    group_level -= 1

                if c in [EXPRESSION_AND, EXPRESSION_OR] and group_level == 0:
                    elements.append(element)
                    elements.append(s[i])
                    element = ""
                else:
                    element += c
            else:
                element += c
                escape = False

            i += 1
            c = next_char()
            escape = c == EXPRESSION_ESCAPE_CHARACTER and not escape
            if escape:
                i += 1
                c = next_char()

        if len(element) > 0:
            elements.append(element)
        return elements

    def is_match(self, tags_dict):

        elements = self.split_expression(self.filter_expression)
        temp = elements.pop(0)
        if temp[0] == "(":
            a = TagFilterExpression(temp[1:-1].strip()).is_match(tags_dict=tags_dict)
        else:
            element_filter = TagFilterSet(temp, filter_sep="|")
            if not element_filter.has_not_operator():
                a = len(element_filter.pairs_matching_any_filter(tags_dict)) > 0
            else:
                a = element_filter.all_pairs_matching_filter(tags_dict)

        while len(elements) > 1:
            op = elements.pop(0)
            b_expression = elements.pop(0)
            b = TagFilterExpression(b_expression).is_match(tags_dict)
            if not b:
                self.not_matching = b_expression
            a = a and b if op == EXPRESSION_AND else a or b

        return a

    def get_filters(self):
        result = []

        elements = self.split_expression(self.filter_expression)
        temp = elements.pop(0)
        if temp[0] == "(":
            a = TagFilterExpression(temp[1:-1].strip()).get_filters()
            result += a
        else:
            result.append(temp)

        while len(elements) > 1:
            elements.pop(0)
            b = TagFilterExpression(elements.pop(0)).get_filters()
            result += b

        return result

    def get_filter_keys(self):
        return set([f.split("=")[0] for f in self.get_filters()])
