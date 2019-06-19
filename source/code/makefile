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

# version number
version=`cat version.txt`

# bucket name to deploy to
ifndef bucket
bucket = ops-automator-deploy
endif

# prefix of deployed artifacts in bucket
ifndef prefix
prefix = ops-automator/$(version)/
endif

# s3 bucket to deploy to
s3bucket=s3://$(bucket)/

# directories containing deployed python code
pydirs= actions boto_retry builders configuration  handlers helpers metrics outputs scheduling services tagging
py=$(foreach dir, $(pydirs), $(wildcard $(dir)/*.py))

# source sub dir of ecs related files
ecsdir=../ecs
ecs=$(wildcard $(ecsdir)/*)

# destination directory to deploy to
deployment=../../deployment
zip = $(deployment)/ops-automator-$(version).zip
cw_zip = $(deployment)/cloudwatch-handler-$(version).zip

# build targets
build: lambda cloudwatchhandler cfn docker

######################################################################################################################
# lambda code                                                                                                        #
######################################################################################################################
lambda:$(py) main.py version.txt
	mkdir -p $(deployment)

	# delete old zip files
	find $(deployment) -maxdepth 1 -type f -name 'ops-automator-*.zip' -delete

	# build zip
	zip  $(zip) version.txt
	mv main.py main.py.org
	sed s/%version%/$(version)/g main.py.org > main.py
	zip  $(zip) main.py config.py
	rm main.py
	mv main.py.org main.py
	zip $(zip) $(pydirs)
	zip -r $(zip)  $(py) version.txt
	zip -r -q $(zip) pytz
	zip $(zip) forward-events.py
	zip $(zip) build_task_custom_resource.py
	zip $(zip) ../cloudformation/AccountForwardEvents.template
	zip $(zip) ../cloudformation/scenarios/*.template
	zip $(zip) builders/actions.html

######################################################################################################################
# cloudwatch handler code                                                                                            #
######################################################################################################################
cloudwatchhandler: cloudwatch_queue_handler_lambda.py version.txt
	mkdir -p $(deployment)

	# delete old zip files
	find $(deployment) -maxdepth 1 -type f -name 'cloudwatch-handler-*.zip' -delete

	# build zip
	zip  $(cw_zip) version.txt
	mv cloudwatch_queue_handler_lambda.py cloudwatch_queue_handler_lambda.py.org
	sed s/%version%/$(version)/g cloudwatch_queue_handler_lambda.py.org > cloudwatch_queue_handler_lambda.py
	zip  $(cw_zip) cloudwatch_queue_handler_lambda.py
	rm cloudwatch_queue_handler_lambda.py
	mv cloudwatch_queue_handler_lambda.py.org cloudwatch_queue_handler_lambda.py


######################################################################################################################
# cloudformation templates                                                                                           #
######################################################################################################################
    cfn:version.txt $(templates)
	mkdir -p $(deployment)

	# delete old templates
	find $(deployment) -maxdepth 1 -type f -name 'ops-automator-*.template' -delete

	# build main ops automator template
	python build-ops-automator-template.py ../cloudformation/ops-automator.template $(version) $(bucket) $(prefix) > $(deployment)/ops-automator-$(version).template
	cp $(deployment)/ops-automator-$(version).template  $(deployment)/ops-automator-latest.template


######################################################################################################################
# docker / ECS                                                                                                       #
######################################################################################################################
docker: $(ecs)
	mkdir -p $(deployment)
	mkdir -p $(deployment)/ecs

	# delete old files
	find $(deployment)/ecs -maxdepth 1 -type f -name '*' -delete

	# set version of ops automator runner script
	sed s/%version%/$(version)/g $(ecsdir)/ops-automator-ecs-runner.py > $(deployment)/ecs/ops-automator-ecs-runner.py

	# docker and readme file
	cp $(ecsdir)/Dockerfile $(deployment)/ecs/Dockerfile
	cp $(ecsdir)/README.md $(deployment)/ecs/README.md

	# version ecs cluster template
	sed s/%version%/$(version)/g ../cloudformation/ops-automator-ecs-cluster.template > $(deployment)/ops-automator-ecs-cluster.template

	# build shell script for creating and pushing docker image
	python build-docker-script.py $(ecsdir)/build-and-deploy-image.sh $(bucket) $(version) $(prefix)  > $(deployment)/ecs/build-and-deploy-image.sh


######################################################################################################################
# Deploy to S3                                                                                                       #
######################################################################################################################
deploy: build
	# ops automator main template latest
	aws s3 cp $(deployment)/ops-automator-latest.template $(s3bucket)$(prefix) --acl public-read
	# ops automator versioned copy
	aws s3 cp $(deployment)/ops-automator-$(version).template $(s3bucket)$(prefix)  --acl public-read
	# ops automator lambda code zip file
	aws s3 cp $(deployment)/ops-automator-$(version).zip $(s3bucket)$(prefix)  --acl public-read
	# cloudwatch queue handler
	aws s3 cp $(deployment)/cloudwatch-handler-$(version).zip $(s3bucket)$(prefix)  --acl public-read

	# ecs related files
	aws s3 cp --recursive $(deployment)/ecs $(s3bucket)$(prefix)ecs

	# extra copy of script to build docker image
	aws s3 cp $(deployment)/ops-automator-latest.template $(s3bucket) --acl public-read
	# extra copy of script to build docker image
	aws s3 cp $(deployment)/ecs/build-and-deploy-image.sh $(s3bucket) --acl public-read
	# extra copy of ecs-cluster template
	aws s3 cp $(deployment)/ops-automator-ecs-cluster.template $(s3bucket) --acl public-read

	# update build number after deployment
	# python update-build-number.py version.txt



