################################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.     # 
#                                                                              # 
#  Licensed under the Apache License Version 2.0 (the "License"). You may not  #
#  use this file except in compliance with the License. A copy of the License  #
#  is located at                                                               # 
#                                                                              # 
#      http://www.apache.org/licenses/                                         # 
#                                                                              # 
#  or in the "license" file accompanying this file. This file is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express  #
#  or implied. See the License for the specific language governing permissions # 
#  and limitations under the License.                                          # 
################################################################################

s3bucket=s3://$(bucket)/

# directories containing deployed python code
pydirs= actions boto_retry builders configuration  handlers helpers metrics outputs scheduling services tagging
py=$(foreach dir, $(pydirs), $(wildcard $(dir)/*.py))

# source sub dir of ecs related files
ecsdir=../ecs
ecs=$(wildcard $(ecsdir)/*)

# destination directory to deploy to
deployment=../..
# deployment=../../deployment

global_assets_dir=$(deployment)/global-s3-assets
regional_assets_dir=$(deployment)/regional-s3-assets

zip = $(regional_assets_dir)/ops-automator.zip 
cw_zip = $(regional_assets_dir)/cloudwatch-handler.zip
templates=$(wildcard ../../*.template)
# build targets
build: lambda cloudwatchhandler cfn docker

#######################################################################
# lambda code                                                         #
#######################################################################
lambda:$(py) main.py version.txt
	#==================================================================
	# make lambda
	#==================================================================

	mkdir -p $(deployment)
	mkdir -p $(regional_assets_dir)

	# delete old zip files
	find $(deployment) -maxdepth 1 -type f -name 'ops-automator.zip' -delete

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
	zip -r -q $(zip) requests
	zip -r -q $(zip) urllib3
	zip -r -q $(zip) chardet
	zip -r -q $(zip) idna
	zip -r -q $(zip) certifi
	zip $(zip) forward-events.py
	zip $(zip) build_task_custom_resource.py
	zip $(zip) ../cloudformation/AccountForwardEvents.template
	zip $(zip) ../cloudformation/scenarios/*.template
	zip $(zip) builders/actions.html

#######################################################################
# cloudwatch handler code                                             #
#######################################################################
cloudwatchhandler: cloudwatch_queue_handler_lambda.py version.txt
	#==================================================================
	# make cloudwatchhandler
	#==================================================================

	mkdir -p $(deployment)

	# delete old zip files
	find $(deployment) -maxdepth 1 -type f -name 'cloudwatch-handler.zip' -delete

	# build zip
	zip  $(cw_zip) version.txt
	mv cloudwatch_queue_handler_lambda.py cloudwatch_queue_handler_lambda.py.org
	sed s/%version%/$(version)/g cloudwatch_queue_handler_lambda.py.org > cloudwatch_queue_handler_lambda.py
	zip  $(cw_zip) cloudwatch_queue_handler_lambda.py
	rm cloudwatch_queue_handler_lambda.py
	mv cloudwatch_queue_handler_lambda.py.org cloudwatch_queue_handler_lambda.py


#######################################################################
# cloudformation templates                                            #
#######################################################################
cfn: version.txt $(templates)
	#==================================================================
	# make cfn
	#==================================================================

	mkdir -p $(deployment)
	mkdir -p $(global_assets_dir)

	# delete old templates
	find $(global_assets_dir) -maxdepth 1 -type f -name 'ops-automator.template' -delete

	# build main ops automator template
	python ./build-ops-automator-template.py ../cloudformation/ops-automator.template  $(bucket) $(solution) $(version) > $(global_assets_dir)/ops-automator.template

#######################################################################
# docker / ECS                                                        #
#######################################################################
docker: $(ecs)
	#==================================================================
	# make docker
	#==================================================================

	mkdir -p $(deployment)
	mkdir -p $(deployment)/ecs

	# delete old files
	find $(deployment)/ecs -maxdepth 1 -type f -name '*' -delete

	# set version of ops automator runner script
	sed s/%version%/$(version)/g $(ecsdir)/ops-automator-ecs-runner.py > $(deployment)/ecs/ops-automator-ecs-runner.py

	# docker and readme file
	sed s/%version%/$(version)/g $(ecsdir)/Dockerfile > $(deployment)/ecs/Dockerfile.orig
	cp $(deployment)/dist/code/requirements.txt $(deployment)/ecs

	# build shell script for creating and pushing docker image
	python build-docker-script.py $(ecsdir)/build-and-deploy-image.sh $(bucket) $(version) $(prefix)  > $(deployment)/ecs/build-and-deploy-image.sh

	chmod +x $(deployment)/ecs/*.sh
	cp -R $(deployment)/ecs $(global_assets_dir)
	