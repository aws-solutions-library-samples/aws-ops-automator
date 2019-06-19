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
import sys

from builders import build_events_forward_template

if __name__ == "__main__":
    print(build_events_forward_template(template_filename=sys.argv[1],
                                        script_filename=sys.argv[2],
                                        ops_automator_topic_arn="arn:topic",
                                        event_role_arn=sys.argv[3],
                                        version=sys.argv[4]))
