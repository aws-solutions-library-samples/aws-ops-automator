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
from datetime import datetime, timedelta

_delta_ = timedelta()


def set_datetime_delta(delta):
    """
    Set delta delta delta.

    Args:
        delta: (todo): write your description
    """
    global _delta_
    _delta_ = delta


# noinspection SpellCheckingInspection
class DatetimeProvider(datetime):
    _delta_ = None

    @classmethod
    def now(cls, tz=None):
        """
        Return a datetime object representing the current timezone.

        Args:
            cls: (todo): write your description
            tz: (todo): write your description
        """
        dt = datetime.now(tz)
        return dt + _delta_

    @classmethod
    def utcnow(cls, tz=None):
        """
        Returns a datetime.

        Args:
            cls: (todo): write your description
            tz: (todo): write your description
        """
        dt = datetime.utcnow()
        return dt + _delta_
