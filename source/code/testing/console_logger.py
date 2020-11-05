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
        """
        Initialize the debug.

        Args:
            self: (todo): write your description
            debug: (bool): write your description
        """
        self._debug = debug

    def __enter__(self):
        """
        Decor function.

        Args:
            self: (todo): write your description
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the given exc is raised.

        Args:
            self: (todo): write your description
            exc_type: (todo): write your description
            exc_val: (todo): write your description
            exc_tb: (todo): write your description
        """
        self.flush()

    def _emit(self, level, msg, *args):
        """
        Emit a message.

        Args:
            self: (todo): write your description
            level: (int): write your description
            msg: (str): write your description
        """
        s = msg if len(args) == 0 else msg.format(*args)
        dt = date_time_provider().now()
        s = LOG_FORMAT.format(dt.year, dt.month, dt.day, dt.hour, dt.minute,
                              dt.second, str(dt.microsecond)[0:3], level, s)
        print(s)

    @property
    def debug_enabled(self):
        """
        Return the debug status is enabled.

        Args:
            self: (todo): write your description
        """
        return self._debug

    @debug_enabled.setter
    def debug_enabled(self, value):
        """
        Sets the debug flag.

        Args:
            self: (todo): write your description
            value: (str): write your description
        """
        self._debug = value

    def info(self, msg, *args):
        """
        Log an info.

        Args:
            self: (todo): write your description
            msg: (str): write your description
        """
        self._emit(LOG_LEVEL_INFO, msg, *args)

    def error(self, msg, *args):
        """
        Emit an error.

        Args:
            self: (todo): write your description
            msg: (str): write your description
        """
        self._emit(LOG_LEVEL_ERROR, msg, *args)

    def warning(self, msg, *args):
        """
        Log msg with warning.

        Args:
            self: (todo): write your description
            msg: (str): write your description
        """
        self._emit(LOG_LEVEL_WARNING, msg, *args)

    def test(self, msg, *args):
        """
        Emit a test.

        Args:
            self: (todo): write your description
            msg: (str): write your description
        """
        self._emit(LOG_LEVEL_TEST, msg, *args)

    def debug(self, msg, *args):
        """
        Log a debug message.

        Args:
            self: (todo): write your description
            msg: (str): write your description
        """
        if self._debug:
            self._emit(LOG_LEVEL_DEBUG, msg, *args)

    def flush(self):
        """
        Flush the cache entries.

        Args:
            self: (todo): write your description
        """
        pass
