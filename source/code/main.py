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


import traceback
from datetime import datetime

import handlers
from util import safe_dict, safe_json
from util.logger import Logger

MSG_REQUEST_HANLED = "Request handler {} completed in {:>.3f} seconds"
MSG_ERR_HANDLING_REQUEST = "Error handling request {} by handler {}: ({})\n{}"
MSG_NO_REQUEST_HANDLER = "Request was not handled, no handler was able to handle this type of request {}"

LOG_STREAM = "{}-{:0>4d}{:0>2d}{:0>2d}"


def lambda_handler(event, context):
    dt = datetime.utcnow()
    logstream = LOG_STREAM.format("OpsAutomatorMain", dt.year, dt.month, dt.day)

    with Logger(logstream=logstream, context=context, buffersize=20) as logger:

        logger.info("Ops Automator, version %version%")

        for handler_name in handlers.all_handlers():

            if handlers.get_class_for_handler(handler_name).is_handling_request(event):
                handler = handlers.create_handler(handler_name, event, context)
                logger.info("Handler is {}", handler_name)
                try:
                    result = handler.handle_request()
                    logger.info(MSG_REQUEST_HANLED, handler_name, (datetime.utcnow() - dt).total_seconds())
                    return safe_dict(result)
                except Exception as e:
                    logger.error(MSG_ERR_HANDLING_REQUEST, safe_json(event, indent=2), handler_name, e, traceback.format_exc())

                return

        logger.error(MSG_NO_REQUEST_HANDLER, safe_json(event, indent=2))

