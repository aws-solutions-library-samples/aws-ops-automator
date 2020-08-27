# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.0] - 2020-08-27
### Added
- (installed from source) Instructions for using ECS/Fargate rather than Lambda for Automation. See GitHub https://github.com/awslabs/aws-ops-automator/tree/master/source/ecs/README.md
- S3 access logging to aws-opsautomator-s3-access-logs-\<account\>-\<region\>

### Changed
- README.md now contains instructions on upgrading Ops Automator 2.x to the latest release.
- ECS/Fargate option updated to use Python3
- ECS/Fargate option now uses OpsAutomatorLambdaRole (previously had no role assigned)
- Updated all Lambda runtimes to Python 3.8
- Encryption is now enabled by default in Mappings->Settings->Resources->EncryptResourceData. All SNS topics, SQS queue, and DynamoDB tables are encrypted by this setting.
- S3 buckets are now encrypted using SSE AES256

### Known Issues
- ECS can be used for the Resource Selection process, but may fail when used for the Execution of actions. Customers should test use of ECS for Execution and use Lambda if unsuccessful.

## [2.1.0] - 2019-10-06
### Added
- upgraded the solution to Python 3.7