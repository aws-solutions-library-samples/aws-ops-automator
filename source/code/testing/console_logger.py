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
from actions import date_time_provider

LOG_FORMAT = "{:0>4d}-{:0>2d}-{:0>2d} - {:0>2d}:{:0>2d}:{:0>2d}.{:0>3s} - {:7s} : {}"

LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_ERROR = "ERROR"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_TEST = "TEST"


# noinspection PyMethodMayBeStatic
class ConsoleLogger(object):

    def __init__(self, debug=False):
        self._debug = debug

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()

    def _emit(self, level, msg, *args):
        s = msg if len(args) == 0 else msg.format(*args)
        dt = date_time_provider().now()
        s = LOG_FORMAT.format(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                              dt.second, str(dt.microsecond)[0:3], level, s)
        print(s)

    @property
    def debug_enabled(self):
        return self._debug

    @debug_enabled.setter
    def debug_enabled(self, value):
        self._debug = value

    def info(self, msg, *args):
        self._emit(LOG_LEVEL_INFO, msg, *args)

    def error(self, msg, *args):
        self._emit(LOG_LEVEL_ERROR, msg, *args)

    def warning(self, msg, *args):
        self._emit(LOG_LEVEL_WARNING, msg, *args)

    def test(self, msg, *args):
        self._emit(LOG_LEVEL_TEST, msg, *args)

    def debug(self, msg, *args):
        if self._debug:
            self._emit(LOG_LEVEL_DEBUG, msg, *args)

    def flush(self):
        pass
