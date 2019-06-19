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
import threading


class Timer(object):
    def __init__(self, timeout_seconds, start=True):
        self.timeout = False
        self._timer = threading.Timer(interval=timeout_seconds, function=self.fn)
        if timeout_seconds > 0 and start:
            self.start()

    def fn(self):
        self._timer.cancel()
        self.timeout = True

    def start(self):
        self.timeout = False
        self._timer.start()

    def stop(self):
        self._timer.cancel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._timer.cancel()
