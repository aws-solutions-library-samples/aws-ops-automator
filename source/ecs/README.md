# AWS Ops Automator ECS Setup

## Description

**Ops Automator is a developer framework** for running actions to manage AWS environments with explicit support for multiple accounts and regions.

The framework is running as a set of micro services in as AWS Lambda. In cases where parts of tasks take longer than the 5 minute limit of Lambda functions, 
the framework can be configures to run these steps as ECS tasks. 

This readme file describes how to setup the use of an ECS cluster that is used by the Ops Automator framework for running these ECS tasks.


## Setup

* Optionally create an ECS cluster using the ecs-cluster.template. This template creates an ECS cluster, in an existing or new VPC. The instances of for the cluster are
part of an EC2 autoscaling group that scales in and out based on the memory usage of the tasks running in the cluster. The name of the cluster is in the output of the 
cloudformation stack.
* Use the name of the ECS cluster created in the step above, or the name of an existing ECS cluster, and use this as the value for the ECS Cluster name parameter.
Create or update the Ops Automator stack after setting this parameter. In the stack created or updated by the template, an ECR repository to store the Ops Automator Docker image 
and task definition for running tasks using that image, are created.
* Verify that Docker is installed on the workstation.
* download the build-and-deploy-image.sh image from the Ops Automator deployment url `TBD`
* In the ecs directory run the following command `sudo ./build-and-deploy-image.sh -s <name-of-ops-automator-stack> -r <region-of-ops-automator-stack>`
This step pulls down the required files to build docker image based on the AWS Linux Docker optimized AMI. It installs the ops-automator-ecs-runner.py script on the image. 
The image is then pushed to the repository created by the Ops Automator template.
* The ECS option is now available, additionally to the Lambda memory sizes, in the task configuration for actions that can use this option.

_**Warning**_ : Do not clear the ECS cluster parameter of an Ops Automator stack that has tasks configured to use a cluster. Doing so will cause the execution of these tasks to fail.



***

Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
