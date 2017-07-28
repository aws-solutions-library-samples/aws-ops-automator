# AWS Ops Automator

## Description

**Ops Automator is a developer framework** for running actions to manage AWS environments with explicit support for multiple accounts and regions.

Ops Automator's primary function is to run tasks. A task is an action with a set of parameters that runs at scheduled times or events and operated on a select set of AWS resources. Events are triggered by changes in your environments and resources are selected through the resource discovery and tagging mechanisms built into Ops Automator.  

Ops Automator comes with a number of actions. These are ready to use in your AWS environment and can be used as an example/starting point for developing your own actions.

Examples of actions included are creating backups, setting capacity, cleaning up and security reporting.

Ops Automator helps you to develop your own operation automations tasks in a consistent way with the framework handling all the heavy lifting.

### The Ops Automator framework handles the following functionality:
* Operations across multiple accounts and regions
* Task audit trails
* Logging
* Resource selection
* Scaling
* AWS API retries
* Completion handling for long running tasks
* Concurrency handling for relevant processes like snapshot copying

Ops Automator lets you focus on implementing the actual logic of the action. Actions are developed in Python and can be added easily to the Ops Automator solution. Ops Automator has the ability to generate CloudFormation scripts for configuring tasks, based on metadata of the action that are part of the deployment.

Development of actions is described in the Ops Automator Action Developers guide.

### Examples of actions customers could build:
* Cross-Account Trusted Advisor reporting
* Automated AMI creation and retention
* Automated vertical instance scaling based on CloudWatch and/or Trusted Advisor Data
* Automated EC2 SSM Command deployments
* S3 Bucket Synchronization
* Responding to change events in your infrastructure with automated runbooks
* Integration with an IoT environment to perform actions on real world events   

***

Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
