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
import time

import boto3

import services.dynamodb_service
from helpers.timer import Timer


class DynamoDB(object):

    def __init__(self, region=None, session=None):
        """
        Initialize the service

        Args:
            self: (todo): write your description
            region: (str): write your description
            session: (todo): write your description
        """

        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.ddb_client = self.session.client("dynamodb", region_name=self.region)
        self.ddb_service = services.dynamodb_service.DynamodbService(session=self.session)

    def wait_until_table_backups_available(self, table, timeout_seconds=30 * 60):
        """
        Waits for a table to appear in a table.

        Args:
            self: (todo): write your description
            table: (str): write your description
            timeout_seconds: (int): write your description
        """
        with Timer(timeout_seconds=timeout_seconds, start=True) as t:

            while not t.timeout:
                try:
                    # the only way to find out if backups are available for newly created table is to try to create a backup
                    resp = self.ddb_client.create_backup(TableName=table, BackupName="is-backup-available")
                    arn = resp.get("BackupDetails", {}).get("BackupArn", None)
                    self.ddb_client.delete_backup(BackupArn=arn)
                    return True
                except Exception as ex:
                    if type(ex).__name__ != "ContinuousBackupsUnavailableException":
                        return False

                time.sleep(30)

        return False

    def get_table(self, table_name):
        """
        Returns the dynamodb table.

        Args:
            self: (todo): write your description
            table_name: (str): write your description
        """
        return self.ddb_service.get(services.dynamodb_service.TABLE,
                                    TableName=table_name,
                                    region=self.region,
                                    tags=True)

    def create_backup(self, table_name, backup_name):
        """
        Create a backup object

        Args:
            self: (todo): write your description
            table_name: (str): write your description
            backup_name: (str): write your description
        """
        return self.ddb_client.create_backup(TableName=table_name, BackupName=backup_name).get("BackupDetails")

    def delete_backup(self, backup_arn, exception_if_not_exists=False):
        """
        Delete backup.

        Args:
            self: (todo): write your description
            backup_arn: (todo): write your description
            exception_if_not_exists: (todo): write your description
        """
        try:
            self.ddb_client.delete_backup(BackupArn=backup_arn)
        except Exception as e:
            if e.__class__.__name__ == "BackupNotFoundException":
                if exception_if_not_exists:
                    raise e
                return
            raise e

    def delete_table_backups(self, table_name):
        """
        Delete the backups

        Args:
            self: (todo): write your description
            table_name: (str): write your description
        """
        for backup_arn in [s["BackupArn"] for s in self.get_table_backups(table_name) if s["BackupStatus"] == "AVAILABLE"]:
            self.delete_backup(backup_arn=backup_arn)

    def get_table_backups(self, table_name):
        """
        Returns the table backups.

        Args:
            self: (todo): write your description
            table_name: (str): write your description
        """
        return self.ddb_service.describe(services.dynamodb_service.BACKUPS, TableName=table_name)

    def create_tags(self, table_name, tags):
        """
        Add tags to aws account

        Args:
            self: (todo): write your description
            table_name: (str): write your description
            tags: (str): write your description
        """
        arn = "arn:aws:dynamodb:{}:{}:table/{}".format(self.region, services.get_aws_account(), table_name)
        self.ddb_client.tag_resource(ResourceArn=arn, Tags=[{"Key": t, "Value": tags[t]} for t in tags])
