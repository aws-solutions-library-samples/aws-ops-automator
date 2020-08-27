#!/usr/bin/env bash
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
# If running Docker requires sudo to run on this system then also run this script with sudo
function usage {
  echo "usage: $0 [--stack | -s] stackname [--region | -r] awsregion"
}

function do_cmd {
  cmd=$*
  $cmd
  if [ $? -gt 0 ]; 
  then
    echo Command failed: ${cmd}
    exit 1
  fi
}

do_replace() {
  replace="s|$2|$3|g"
  file=$1
  sed -i -e $replace $file
}

#------------------------------------------------------------------------------------------------

echo "Build and deploy docker image for Ops Automator, version %version%"
# Before running this script:
# - you have created an ECS Docker cluster
# - you have updated the OA stack with the name of the cluster
# - Cloudformation has created the ECR repo
# - you have the name of the stack

while [[ $# -gt 1 ]]
do

  key="$1"

  case ${key} in
      -r|--region)
      region="$2"
      shift # past argument
      ;;
      -s|--stack)
      stack="$2"
      shift # past argument
      ;;
      *)
      usage
      exit 1
      ;;
  esac
  shift # past argument or value
done

if [ "${region}" == "" ]
then
  echo "Error: No region specified, use -r or --region to specify the region."
  usage
  exit 1
fi

if [ "${stack}" == "" ]
then
  echo "Error: No stack name specified, use -s or --stack to specify the name of the Ops Automator stack."
  usage
  exit 1
fi

# Get repository from the stack parameters
repository=`aws cloudformation describe-stacks --region ${region} --stack-name ${stack} --query "Stacks[0].Outputs[?OutputKey=='Repository']|[0].OutputValue" --output text`
if [ "${repository}" == "" ]
then
   echo "No repository in output of stack $(stack)"
   exit 1
fi

# Get account id
accountid=`aws sts get-caller-identity --region ${region} --output text | sed 's/\t.*//'`

image=ops-automator

echo
echo "Image is      : " ${image}
echo "repository is : " ${repository}
echo "Region is     : " ${region}
echo

echo "=== Creating Dockerfile =="
cp Dockerfile.orig Dockerfile
do_replace Dockerfile '<AWS_ECR_REPO>' ${repository}
echo

# Pulling latest AWS Linux image. Note that this repo/region must match FROM value in Docker file
echo "=== Pulling latest AWS Linux image from DockerHub ==="
do_cmd docker pull amazonlinux

echo
echo "=== Building docker image ==="
do_cmd docker build -t ${image}  .

echo
echo "=== Tagging and pushing image $image to $repository ==="
do_cmd docker tag ${image}:latest ${repository}:latest

login=`aws ecr get-login --region ${region} --no-include-email`
do_cmd $login
do_cmd docker push ${repository}:latest
