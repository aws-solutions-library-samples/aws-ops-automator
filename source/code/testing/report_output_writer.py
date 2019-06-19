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

import StringIO
import csv


def create_output_writer(_=None, __=None):
    return ReportOutputWriter(_)


def csv_to_dict_list(s):

    if s is None:
        return None

    result = []
    cols = None
    try:
        reader = csv.reader(StringIO.StringIO(s))
        cols = reader.next()
        row = reader.next()

        while True:
            result.append({cols[i]: row[i] for i in range(0, len(cols))})
            row = reader.next()

    except StopIteration:
        if cols is None:
            return None
        else:
            return result


# noinspection PyMethodMayBeStatic
class ReportOutputWriter(object):

    def __init__(self, _):
        self._data_ = None

    def write(self, data, _):
        self._data_ = data

    @property
    def data(self):
        return self._data_

    def reset(self):
        self._data_ = None
