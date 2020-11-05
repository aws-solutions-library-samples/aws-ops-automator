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
from scheduling.setbuilder import SetBuilder


class MinuteSetBuilder(SetBuilder):
    """
    Class for building builds set of minute values (00-59)
    """

    def __init__(self):
        """
        Initialize the internal state.

        Args:
            self: (todo): write your description
        """
        SetBuilder.__init__(self, min_value=0, max_value=59, wrap=False)
