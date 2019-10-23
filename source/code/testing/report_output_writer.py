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
import io
import csv


def create_output_writer(_=None, __=None):
    return ReportOutputWriter(_)


def csv_to_dict_list(s):

    if s is None:
        return None

    result = []
    cols = None
    try:
        reader = csv.reader(io.StringIO(s))
        cols = next(reader)
        row = next(reader)

        while True:
            result.append({cols[i]: row[i] for i in list(range(0, len(cols)))})
            row = next(reader)

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
