AWS Ops Automator
=================

**Ops Automator is a developer framework** for running actions to manage AWS environments with explicit support for multiple accounts and regions.

Ops Automator's primary function is to run tasks. A task is an action with a set of parameters that runs at scheduled times or events and operated on a select set of AWS resources. Events are triggered by changes in your environments and resources are selected through the resource discovery and tagging mechanisms built into Ops Automator.

Ops Automator comes with a number of actions. These are ready to use in your AWS environment and can be used as an example/starting point for developing your own actions.

Examples of actions included are creating backups, setting capacity, cleaning up and security reporting.

Ops Automator helps you to develop your own operation automations tasks in a consistent way with the framework handling all the heavy lifting.

The Ops Automator framework handles the following functionality:
----------------------------------------------------------------

-   Operations across multiple accounts and regions
-   Task audit trails
-   Logging
-   Resource selection
-   Scaling
-   AWS API retries
-   Completion handling for long running tasks
-   Concurrency handling via queue throttling

Ops Automator lets you focus on implementing the actual logic of the action. Actions are developed in Python and can be added easily to the Ops Automator solution. Ops Automator has the ability to generate CloudFormation scripts for configuring tasks, based on metadata of the action that are part of the deployment.

Development of actions is described in the Ops Automator Action Implementation Guide.

Documentation
-------------

[Ops Automator full documentation](https://docs.aws.amazon.com/solutions/latest/ops-automator/welcome.html) is available on the AWS web site.

Platform Support
----------------

Ops Automator v2.2.0 and later supports AWS Lambda, AWS ECS, AWS Fargate for the execution platform. Choose ECSFargate = Yes in the CloudFormation template to use ECS or Fargate, or leave it set to No to use Lambda. Note that with ECS/Fargate you may choose to use Lambda or containers at the Task level. To implement ECS/Fargate see the instructions later in this README.

Building from GitHub
--------------------

### Overview of the Process

Building from GitHub source will allow you to modify the solution, such as adding custom actions or upgrading to a new release. The process consists of downloading the source from GitHub, creating buckets to be used for deployment, building the solution, and uploading the artifacts needed for deployment.

### You will need:

-   a Linux client with the AWS CLI installed and python 3.6+
-   source code downloaded from GitHub
-   two S3 buckets (minimum): 1 global and 1 for each region where you will
    deploy

### Download from GitHub

Clone or download the repository to a local directory on your linux client. Note: if you intend to modify Ops Automator you may wish to create your own fork of the GitHub repo and work from that. This allows you to check in any changes you make to your private copy of the solution.

**Git Clone example:**

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/awslabs/aws-ops-automator.git
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Download Zip example:**

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
wget https://github.com/awslabs/aws-ops-automator/archive/master.zip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Customize to your needs

Some customers have implementations of older versions of Ops Automator that include deprecated or custom actions. In order to upgrade to the latest release you will need to bring these actions forward to the latest build. See details later in this file.

[See Ops Automator documentation for more details.](https://docs.aws.amazon.com/solutions/latest/ops-automator/welcome.html)
### Build for Distribution

AWS Solutions use two types of buckets: a bucket for global access to templates, which is accessed via HTTP, and regional buckets for access to assets within the region, such as Lambda code. You will need:

-   One global bucket that is access via the http end point. AWS CloudFormation
    templates are stored here. Ex. "mybucket"
-   One regional bucket for each region where you plan to deploy using the name
    of the global bucket as the root, and suffixed with the region name. Ex.
    "mybucket-us-east-1"
-   Your buckets should be encrypted and disallow public access

From the *deployment* folder in your cloned repo, run build-s3-dist.sh

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
chmod +x build-s3-dist.sh
build-s3-dist.sh <bucketname> ops-automator {version}
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**\<bucketname\>**: name of the "global bucket" - *mybucket* in the example above

**ops-automator**: name of the solution. This is used to form the first level prefix in the regional s3 bucket

**version**: Optionally, you can override the version (from version.txt). You will want to do this when doing incremental updates within the same version, as this causes CloudFormation to update the infrastructure, particularly Lambdas when the source code has changed. We recommend using a build suffix to the semver version. Ex. for version 2.2.0 suffix it with ".001" and increment for each subsequent build. 2.2.0.001, 2.2.0.002, and so on. This value is used as the second part of the prefix for artifacts in the S3 buckets. The default version is the value in **version.txt.**

Automatically upload files using the **deployment/upload-s3-dist.sh** script. You must have configured the AWS CLI and have access to the S3 buckets. The upload script automatically receives the values you provided the the build script. You may run **upload-s3-dist.sh** for each region where you plan to deploy the solution.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
chmod +x upload-s3-dist.sh
upload-s3-dist.sh <region>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Upgrading from a 2.0.0 Release
------------------------------

Version 2.1.0 and later include 7 supported actions: \* DynamoDbSetCapacity \* Ec2CopySnapshot \* Ec2CreateSnapshot \* Ec2DeleteSnapshot \* Ec2ReplaceInstance \* Ec2ResizeInstance \* Ec2TagCpuInstance

Many customers have older versions of Ops Automator that include custom actions. It is possible to add these actions to Ops Automator 2.1 and later. You will need a copy of the source for your current implementation. You can find a zip file containing your current deployment as follows:

1.  Open CloudFormation and locate your Ops Automator stack
2.  Open the stack and view the template (**Tenplate** tab)
3.  Find the Resource definition for OpsAutomatorLambdaFunctionStandard. Note
    the values for S3Bucket and S3Key.
4.  Interpret these values to get the bucket name and prefix.
5.  Derive the url: https://\<bucketname\>-\<region\>/\<S3Key\>
6.  Get the file
7.  Extract the file to a convenient location

**Example**

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
OpsAutomatorLambdaFunctionStandard": {
            "Type": "AWS::Lambda::Function", 
            "Properties": {
                "Code": {
                    "S3Bucket": {
                        "Fn::Join": [
                            "-", 
                            [
                                "ops-automator-deploy", 
                                {
                                    "Ref": "AWS::Region"
                                }
                            ]
                        ]
                    }, 
                    "S3Key": "ops-automator/latest/ops-automator-2.2.0.61.zip"
                }, 
                "FunctionName": {
                    "Fn::Join": [
                        "-", 
                        [
                            {
                                "Ref": "AWS::StackName"
                            }, 
                            {
                                "Fn::FindInMap": [
                                    "Settings", 
                                    "Names", 
                                    "OpsAutomatorLambdaFunction"
                                ]
                            }, 
                            "Standard"
                        ]
                    ]
                }, 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**S3Bucket:** ops-automator-deploy

**S3Key:** ops-automator/latest/ops-automator-2.2.0.61.zip

**url for us-east-1:**
https://ops-automator-deploy-us-east-1.s3.amazonaws.com/ops-automator/latest/ops-automator-2.2.0.61.zip

**Get the source:**

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
wget https://ops-automator-deploy-us-east-1.s3.amazonaws.com/ops-automator/latest/ops-automator-2.2.0.61.zip
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Create Build Area

Follow the instructions above to create a development copy of Ops Automator from GitHub. Go to the root of that copy. You should see:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 oa-220-customer]$ ll
total 28
-rw-rw-r-- 1 ec2-user ec2-user   324 Jan  6 14:29 CHANGELOG.md
drwxrwxr-x 2 ec2-user ec2-user   122 Jan  6 14:29 deployment
-rwxrwxr-x 1 ec2-user ec2-user 10577 Jan  6 14:29 LICENSE.txt
-rwxrwxr-x 1 ec2-user ec2-user   822 Jan  6 14:29 NOTICE.txt
-rwxrwxr-x 1 ec2-user ec2-user  3837 Jan  6 14:29 README.md
drwxrwxr-x 5 ec2-user ec2-user    51 Jan  6 14:29 source
-rw-rw-r-- 1 ec2-user ec2-user     5 Jan  6 14:29 version.txt
(python3) [ec2-user@ip-10-0-20-184 oa-220-customer]$
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Initial Build

To verify that all is well, do a base build from this source. You will need the base name of the global bucket you created earlier.

Ex. "mybucket" will use "mybucket" for the templates and "mybucket-us-east-1" for a deployment in us-east-1.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
cd deployment
chmod +x *.sh
./build-s3-dist.sh <bucket> ops-automator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After your build completes without error copy the output files to your S3 buckets using the upload-s3-dist.sh script to send the files to the desire region:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
./upload-s3-dist.sh <region>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This will create the prefix ops-automator/\<*version*\> in both buckets, one containing the templates and the other a zip of the Lambda source code. This is your baseline, box-stock OA build.

### Upgrading Actions

#### Overview

1.  Get a list of Actions to be migrated
2.  Copy action source to source/code/actions
-   Audit for prereqs
-   Audit for Python 3 compatibility
1.  Run build-s3-dist.sh
2.  Run upload-s3-dist.sh
3.  Update the stack using the S3 url for the updated new template after all
    actions imported

#### Get a list of Actions

Use the DynamoDB Console to query the Ops Automator ConfigurationTable for unique values in the Action column. For any action not in the above list you will need to find the source code in your current release, **source/code/actions.** Repeat the following steps for each Action.

#### Update each Action

1.  Check dependencies

Locate the Action file in your current deployment. For example, we'll work with DynamodbCreateBackup, which was a supported action as recently as 2.0.0.213, removed in a later 2.0 build.

Copy the file to source/code/actions in the new release source.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 actions]$ ll
total 312
-rw-rw-r-- 1 ec2-user ec2-user  7911 Jan  6 15:17 action_base.py
-rw-rw-r-- 1 ec2-user ec2-user  9365 Jan  6 15:17 action_ec2_events_base.py
-rw-rw-r-- 1 ec2-user ec2-user  9688 Jan  6 16:37 dynamodb_create_backup_action.py
-rw-rw-r-- 1 ec2-user ec2-user 12694 Jan  6 15:17 dynamodb_set_capacity_action.py
-rw-rw-r-- 1 ec2-user ec2-user 49681 Jan  6 15:17 ec2_copy_snapshot_action.py
-rwxrwxr-x 1 ec2-user ec2-user 38045 Jan  6 15:17 ec2_create_snapshot_action.py
-rwxrwxr-x 1 ec2-user ec2-user 16840 Jan  6 15:17 ec2_delete_snapshot_action.py
-rwxrwxr-x 1 ec2-user ec2-user 55337 Jan  6 15:17 ec2_replace_instance_action.py
-rwxrwxr-x 1 ec2-user ec2-user 34373 Jan  6 15:17 ec2_resize_instance_action.py
-rwxrwxr-x 1 ec2-user ec2-user 15825 Jan  6 15:17 ec2_tag_cpu_instance_action.py
-rw-rw-r-- 1 ec2-user ec2-user 14559 Jan  6 15:17 __init__.py
-rwxrwxr-x 1 ec2-user ec2-user  6199 Jan  6 15:17 scheduler_config_backup_action.py
-rwxrwxr-x 1 ec2-user ec2-user  8092 Jan  6 15:17 scheduler_task_cleanup_action.py
-rwxrwxr-x 1 ec2-user ec2-user  9132 Jan  6 15:17 scheduler_task_export_action.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Open the file in an editor and observe the imports:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import services.dynamodb_service
import tagging
from actions import *
from actions.action_base import ActionBase
from boto_retry import get_client_with_retries, get_default_retry_strategy
from helpers import safe_json
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Verify that dynamodb_service.py exists in the source/code/services:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 deployment]$ cd ../services
(python3) [ec2-user@ip-10-0-20-184 services]$ ll
total 212
-rwxrwxr-x 1 ec2-user ec2-user 29299 Jan  6 15:17 aws_service.py
-rwxrwxr-x 1 ec2-user ec2-user  4882 Jan  6 15:17 cloudformation_service.py
-rwxrwxr-x 1 ec2-user ec2-user  4871 Jan  6 15:17 cloudwatchlogs_service.py
-rwxrwxr-x 1 ec2-user ec2-user  5390 Jan  6 15:17 dynamodb_service.py
-rwxrwxr-x 1 ec2-user ec2-user 12657 Jan  6 15:17 ec2_service.py
-rwxrwxr-x 1 ec2-user ec2-user  4987 Jan  6 15:17 ecs_service.py
-rwxrwxr-x 1 ec2-user ec2-user  6861 Jan  6 15:17 elasticache_service.py
-rwxrwxr-x 1 ec2-user ec2-user  5214 Jan  6 15:17 elb_service.py
-rwxrwxr-x 1 ec2-user ec2-user  5369 Jan  6 15:17 elbv2_service.py
-rwxrwxr-x 1 ec2-user ec2-user  6125 Jan  6 15:17 iam_service.py
-rwxrwxr-x 1 ec2-user ec2-user  8193 Jan  6 15:17 __init__.py
-rwxrwxr-x 1 ec2-user ec2-user  5341 Jan  6 15:17 kms_service.py
-rwxrwxr-x 1 ec2-user ec2-user  5291 Jan  6 15:17 lambda_service.py
-rwxrwxr-x 1 ec2-user ec2-user  7558 Jan  6 15:17 opsautomatortest_service.py
-rwxrwxr-x 1 ec2-user ec2-user 11413 Jan  6 15:17 rds_service.py
-rw-rw-r-- 1 ec2-user ec2-user 13363 Jan  6 15:17 route53_service.py
-rwxrwxr-x 1 ec2-user ec2-user  9749 Jan  6 15:17 s3_service.py
-rwxrwxr-x 1 ec2-user ec2-user  6725 Jan  6 15:17 servicecatalog_service.py
-rwxrwxr-x 1 ec2-user ec2-user  5769 Jan  6 15:17 storagegateway_service.py
-rwxrwxr-x 1 ec2-user ec2-user  3441 Jan  6 15:17 tagging_service.py
-rwxrwxr-x 1 ec2-user ec2-user  4086 Jan  6 15:17 time_service.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Note that this action uses ActionBase, which is already in the actions folder (see above listing).

1.  Verify the code / compatibility

Do a quick scan to make sure there are no Python 3 compatibility issues.

**TIP**: use a linter. This code looks clean with regards to Python 3 issues.

1.  Repeat for all actions to be added from the old release

#### Build it as a new version

Open the directory for your new version and go to the **deployment** folder. Find out the current base semver version:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 customer]$ more version.txt
2.2.0
(python3) [ec2-user@ip-10-0-20-184 customer]$ 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Append a build number. Start with 001. We will use version 2.2.0.001 for our first build. This is important as it will allow us to update the install. Do not change the semver version, as this allows AWS to match your installation back to the original.

Run build_s3_dist:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 deployment]$ ./build-s3-dist.sh mybucket ops-automator v2.2.0.001
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Upon successful completion, upload to S3:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 deployment]$ ./upload-s3-dist.sh us-east-1
==========================================================================
Deploying ops-automator version v2.2.0 to bucket mybucket-us-east-1
==========================================================================
Templates: mybucket/ops-automator/v2.2.0/
Lambda code: mybucket-us-east-1/ops-automator/v2.2.0/
---
Press [Enter] key to start upload to us-east-1
upload: global-s3-assets/ops-automator-ecs-cluster.template to s3://mybucket/ops-automator/v2.2.0/ops-automator-ecs-cluster.template
upload: global-s3-assets/ops-automator.template to s3://mybucket/ops-automator/v2.2.0/ops-automator.template
upload: regional-s3-assets/cloudwatch-handler.zip to s3://mybucket-us-east-1/ops-automator/v2.2.0/cloudwatch-handler.zip
upload: regional-s3-assets/ops-automator.zip to s3://mybucket-us-east-1/ops-automator/v2.2.0/ops-automator.zip
Completed uploading distribution. You may now install from the templates in mybucket/ops-automator/v2.2.0/
(python3) [ec2-user@ip-10-0-20-184 deployment]$ 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#### Update the Stack or Deploy as New

We generally recommend that you deploy a new stack with the new version and then migrate your actions from old to new. You may optionally update the stack in place. We have tested upgrade-in-place from v2.0.0 to v2.2.0 successfully, following the instructions above very carefully.

**To Update**

Replace the template with the one from the new version.

Ex.
https://mybucket.s3-us-west-2.amazonaws.com/ops-automator/v2.2.0.001/ops-automator.template.

There is no need to change any parameters.

**Validate the change in Lambda**

Open the Lambda console. Find all of your Lambdas by filtering by stack name. All should show an update at the time you updated the stack. Open onf of the OpsAutomator-\<size\> Lambdas - ex. *OA-220-customer-OpsAutomator-Large*. View the Function code. Expand the actions folder. You should see the new action, dynamodb_create_backup_action.py.

Verify that the action template was uploaded to the S3 configuration bucket:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
(python3) [ec2-user@ip-10-0-20-184 deployment]$ aws s3 ls s3://oa-220-customer-configuration-1wg089n4zjpt4/TaskConfiguration/
                           PRE ScenarioTemplates/
                           PRE Scripts/
2020-01-06 17:13:52       6324 ActionsConfiguration.html
2020-01-06 17:13:46      25492 DynamodbCreateBackup.template
2020-01-06 17:13:45      33083 DynamodbSetCapacity.template
2020-01-06 17:13:50      37284 Ec2CopySnapshot.template
2020-01-06 17:13:49      34782 Ec2CreateSnapshot.template
2020-01-06 17:13:49      27938 Ec2DeleteSnapshot.template
2020-01-06 17:13:48      39649 Ec2ReplaceInstance.template
2020-01-06 17:13:46      38499 Ec2ResizeInstance.template
2020-01-06 17:13:51      26854 Ec2TagCpuInstance.template
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Check the Logs**

Examine both the Lambda logs and the Ops Automator logs for errors.

ECS/Fargate Implementation
--------------------------

This section describes how to setup the use of an AWS Fargate cluster that is used by the Ops Automator framework for long-running tasks. Starting with Ops Automator 2.2.0 you can deploy with AWS ECS / Fargate or add it later. With ECS/Fargate enabled you can choose which tasks to run on containers, and which to run on Lambda - they are not mutually exclusive. ECS/Fargate may be a desirable option for customers with tasks that run longer than 15 minutes.

## Setup
This assumes that you have downloaded the Ops Automator source from Github, built, and deployed the solution from source in your account. If you installed from the AWS Solutions template a simpler deployment is described in the AWS Ops Automator Implementation Guide, Appendix K.

### Overview

1. Deploy/update the AWS Ops Automator stack to use the ECS option
2. Build and deploy the Docker container
3. Update/deploy tasks using ECS/Fargate

### Deploy Ops Automator with ECS

See above procedure to build and deploy the solution from source. Select the ECS/Fargate option. You must do this first, as this option will create the ECS Container Registry needed in the last step.

ECS can deploy a cluster in an existing VPC. You will need to provide the VPC ID and subnet IDs for at least two subnets. 

Fargate is automatically selected if you do not provide a VPC ID. It deploys a new VPC and public subnets for the Fargate cluster. 

### Build and Deploy the Docker Container

From the */deployment/ecs* folder where you built the solution the ecs files, run the following command 

```./build-and-deploy-image.sh -s <stack-name> -r <region>```

This step pulls down the required files to build docker image based on the AWS Linux Docker optimized AMI. It installs the ops-automator-ecs-runner.py script on the image. 

The image is then pushed to the **ops-automator** repository, created by the Ops Automator template

### Deploy Ops Automator Actions using ECS

The ECS option is now available for Actions. You can now deploy additional tasks using the ECS option or modify existing tasks to use ECS. Note: if you deployed tasks prior to selecting ECS in the main AWS Ops Automator stack you will need to update their template from the S3 Ops Automator configuration bucket. ECS will now be an option for Resource Selection Memory and Execution Memory.

***

Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License Version 2.0 (the "License"). You may not use
this file except in compliance with the License. A copy of the License is
located at

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
http://www.apache.org/licenses/
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

or in the "license" file accompanying this file. This file is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied.
See the License for the specific language governing permissions and limitations
under the License.

<a name="collection-of-operational-metrics"></a>
# Collection of operational metrics

This solution collects anonymous operational metrics to help AWS improve the
quality of features of the solution. For more information, including how to disable
this capability, please see the
[Implementation Guide](https://docs.aws.amazon.com/solutions/latest/ops-automator/appendix-k.html)
