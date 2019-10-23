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
import json
import os

import boto3

FORWARDED_EVENTS = {
    "aws.ec2": [
        "EC2 Instance State-change Notification",
        "EBS Snapshot Notification"
    ],
    "aws.tag": [
        "Tag Change on Resource"
    ],
    "aws.rds": [
        "AWS API Call via CloudTrail"
    ]
}

INF_FORWARDED = "Event from source \"{}\", type \"{}\" forwarded to region {}, account {}, topic {}\n{}"
INF_EVENT_ALREADY_IN_REGION = "Event from source \"{}\", type \"{}\" already in forward region {} or is a non-forwarded event"
ERR_FAILED_FORWARD = "Failed to forward event {},  {}"


def lambda_handler(event, _):
    print("Ops Automator Events Forwarder (version %version%)")
    destination_region = os.getenv("OPS_AUTOMATOR_REGION", "")
    destination_account = os.getenv("OPS_AUTOMATOR_ACCOUNT")
    source = event.get("source", "")
    detail_type = event.get("detail-type", "")
    if ((event.get("region", "") != destination_region) or (event.get("account", "") != destination_account)) and \
            detail_type in FORWARDED_EVENTS.get(source, []):

        destination_region_sns_client = boto3.client("sns", region_name=destination_region)

        try:
            topic = os.getenv("OPS_AUTOMATOR_TOPIC_ARN")
            destination_region_sns_client.publish(TopicArn=topic, Message=json.dumps(event))
            print((INF_FORWARDED.format(source, detail_type, destination_region, destination_account, topic, str(event))))
            return "OK"
        except Exception as ex:
            raise Exception(ERR_FAILED_FORWARD, str(event), ex)

    else:
        print((INF_EVENT_ALREADY_IN_REGION.format(source, detail_type, destination_region)))
