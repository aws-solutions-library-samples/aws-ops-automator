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
import threading


class Timer(object):
    def __init__(self, timeout_seconds, start=True):
        """
        Start a new timer.

        Args:
            self: (todo): write your description
            timeout_seconds: (int): write your description
            start: (int): write your description
        """
        self.timeout = False
        self._timer = threading.Timer(interval=timeout_seconds, function=self.fn)
        if timeout_seconds > 0 and start:
            self.start()

    def fn(self):
        """
        Cancel a task.

        Args:
            self: (todo): write your description
        """
        self._timer.cancel()
        self.timeout = True

    def start(self):
        """
        Start the timer.

        Args:
            self: (todo): write your description
        """
        self.timeout = False
        self._timer.start()

    def stop(self):
        """
        Stop the task.

        Args:
            self: (todo): write your description
        """
        self._timer.cancel()

    def __enter__(self):
        """
        Decor function.

        Args:
            self: (todo): write your description
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the given exception.

        Args:
            self: (todo): write your description
            exc_type: (todo): write your description
            exc_val: (todo): write your description
            exc_tb: (todo): write your description
        """
        self._timer.cancel()
