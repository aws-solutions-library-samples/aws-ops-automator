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

from datetime import datetime, timedelta

_delta_ = timedelta()


def set_datetime_delta(delta):
    global _delta_
    _delta_ = delta


# noinspection SpellCheckingInspection
class DatetimeProvider(datetime):
    _delta_ = None

    @classmethod
    def now(cls, tz=None):
        dt = datetime.now(tz)
        return dt + _delta_

    @classmethod
    def utcnow(cls, tz=None):
        dt = datetime.utcnow()
        return dt + _delta_
