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

#!/usr/bin/env bash

echo "Build and deploy docker image for Ops Automator, version %version%"

while [[ $# -gt 1 ]]
do

key="$1"

case ${key} in
    -r|--region)
    region="$2"
    shift # past argument
    ;;
    -s|--stack-name)
    stack="$2"
    shift # past argument
    ;;
    *)
    echo "${key} is not a valid parameter, valid parameters are --region (-r) and --stack-name (-s)"
    exit 1
    ;;
esac
shift # past argument or value
done

if [ "${region}" == "" ]
then
   echo "Error: No region specified, use -r or --region to specify the region."
   exit 1
fi

if [ "${stack}" == "" ]
then
   echo "Error: No stack name  specified, use -s or --stack-name to specify the name of the stack."
   exit 1
fi


# If running Docker requires sudo to run on this system then also run this script with sudo

repository=`aws cloudformation describe-stacks --region ${region} --stack-name ${stack} --query "Stacks[0].Outputs[?OutputKey=='Repository']|[0].OutputValue" --output text`
if [ "${repository}" == "" ]
then
   echo "No repository in output of stack $(stack)"
   exit 1
fi

image=ops-automator

echo
echo "Image is      : " ${image}
echo "Repository is : " ${repository}
echo "Region is     : " ${region}
echo

echo "=== Downloading Ops Automator runner =="
curl '%ecs_runner_script%' > ops-automator-ecs-runner.py
echo

echo "=== Downloading Dockerfile =="
curl '%docker_file%' > Dockerfile
echo

# Pulling latest AWS Linux image. Note that this repo/region must match FROM value in Docker file
echo "=== Pulling latest AWS Linux image   ==="
sudo `aws ecr get-login --region us-west-2 --registry-ids 137112412989 --no-include-email`
docker pull 137112412989.dkr.ecr.us-west-2.amazonaws.com/amazonlinux:latest

sudo `aws ecr get-login --region ${region} --no-include-email`

echo
echo "=== Building docker image ==="
docker build -t ${image}  .


echo
echo "=== Tagging and pushing image $image to $repository ==="
docker tag ${image}:latest ${repository}:latest
docker push ${repository}:latest
