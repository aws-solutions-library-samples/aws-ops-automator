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
import time
import uuid

import boto3
import boto3.exceptions
import pymysql

import services.rds_service
import tagging
from helpers.timer import Timer
from tagging import tag_key_value_list
from tagging.tag_filter_expression import TagFilterExpression


class Rds(object):

    def __init__(self, region=None, session=None):
        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.rds_client = self.session.client("rds", region_name=self.region)
        self.rds_service = services.create_service("rds", session=self.session)

        self._account = None

    @property
    def account(self):
        if self._account is None:
            self._account = services.get_aws_account()
        return self._account

    def get_instance(self, instance_id, tags=True):
        try:
            return self.rds_service.get(services.rds_service.DB_INSTANCES,
                                        DBInstanceIdentifier=instance_id,
                                        region=self.region,
                                        tags=tags)
        except Exception as ex:
            if self.db_instance_not_found(ex):
                return None
            else:
                raise ex

    def get_cluster(self, cluster_id, tags=True):
        try:
            return self.rds_service.get(services.rds_service.DB_CLUSTERS,
                                        DBClusterIdentifier=cluster_id,
                                        region=self.region,
                                        tags=tags)
        except Exception as ex:
            if self.db_cluster_not_found(ex):
                return None
            else:
                raise ex

    @classmethod
    def is_exception_with_code(cls, ex, code):
        return getattr(ex, "response", {}).get("Error", {}).get("Code", "") == code

    @classmethod
    def db_instance_not_found(cls, ex):
        return cls.is_exception_with_code(ex, "DBInstanceNotFound")

    @classmethod
    def db_cluster_not_found(cls, ex):
        return cls.is_exception_with_code(ex, "DBClusterNotFoundFault")

    @classmethod
    def connect(cls, host_endpoint, user, password, db):
        return pymysql.connect(host=host_endpoint, user=user, password=password, db=db)

    def create_tags(self, resource_arn, tags):
        self.rds_client.add_tags_to_resource(ResourceName=resource_arn, Tags=[{"Key": t, "Value": tags[t]} for t in tags])

    def create_db_tags(self, db, tags):
        arn = "arn:aws:rds:{}:{}:db:{}".format(self.region, services.get_aws_account(), db)
        self.create_tags(arn, tags)

    def create_cluster_tags(self, cluster, tags):
        arn = "arn:aws:rds:{}:{}:cluster:{}".format(self.region, services.get_aws_account(), cluster)
        self.create_tags(arn, tags)

    def create_instance_snapshot_tags(self, snapshot, tags):
        arn = "arn:aws:rds:{}:{}:snapshot:{}".format(self.region, services.get_aws_account(), snapshot)
        self.create_tags(arn, tags)

    def get_instance_snapshot(self, snapshot_identifier):
        try:
            return self.rds_service.get(services.rds_service.DB_SNAPSHOTS,
                                        DBSnapshotIdentifier=snapshot_identifier,
                                        region=self.region,
                                        tags=True)
        except Exception as ex:
            if "DBSnapshotNotFound" in ex.message:
                return None
            raise ex

    def get_cluster_snapshot(self, snapshot_identifier):
        try:
            return self.rds_service.get(services.rds_service.DB_CLUSTER_SNAPSHOTS,
                                        DBClusterSnapshotIdentifier=snapshot_identifier,
                                        region=self.region,
                                        tags=True)
        except Exception as ex:
            if "DBClusterSnapshotNotFound" in ex.message:
                return None
            raise ex

    def create_instance_snapshot(self, instance_identifier, snapshot_identifier, tags):
        try:
            self.rds_client.create_db_snapshot(DBSnapshotIdentifier=snapshot_identifier,
                                               DBInstanceIdentifier=instance_identifier,
                                               Tags=[{"Key": t, "Value": tags[t]} for t in tags])
            with Timer(timeout_seconds=600) as t:
                while True:
                    snapshot = self.get_instance_snapshot(snapshot_identifier=snapshot_identifier)
                    if snapshot is not None and snapshot.get("Status") == "available":
                        return snapshot
                    if t.timeout:
                        return None
                    time.sleep(20)
        except Exception as ex:
            print(ex)
            return None

    def create_cluster_snapshot(self, cluster_identifier, snapshot_identifier, tags):
        try:
            self.rds_client.create_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot_identifier,
                                                       DBClusterIdentifier=cluster_identifier,
                                                       Tags=[{"Key": t, "Value": tags[t]} for t in tags])
            with Timer(timeout_seconds=1200) as t:
                while True:
                    snapshot = self.get_cluster_snapshot(snapshot_identifier=snapshot_identifier)
                    if snapshot is not None and snapshot.get("Status") == "available":
                        return snapshot
                    if t.timeout:
                        return None
                    time.sleep(20)
        except Exception as ex:
            print(ex)
            return None

    def delete_instance_snapshots(self, snapshot_ids):
        for db_id in snapshot_ids if isinstance(snapshot_ids, list) else [snapshot_ids]:
            if self.get_instance_snapshot(db_id) is not None:
                while True:
                    with Timer(timeout_seconds=300) as t:
                        try:
                            self.rds_client.delete_db_snapshot(DBSnapshotIdentifier=db_id)
                        except Exception as ex:
                            if "InvalidDBSnapshotState" in ex.message:
                                if t.timeout:
                                    raise ex
                                time.sleep(10)
                                continue

                            if "DBSnapshotNotFound" in ex.message:
                                return
                            raise ex

    def delete_cluster_snapshots(self, snapshot_ids):
        for db_id in snapshot_ids if isinstance(snapshot_ids, list) else [snapshot_ids]:
            if self.get_cluster_snapshot(db_id) is not None:
                while True:
                    with Timer(timeout_seconds=300) as t:
                        try:
                            self.rds_client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=db_id)
                        except Exception as ex:
                            if "InvalidDBClusterSnapshotState" in ex.message:
                                if t.timeout:
                                    raise ex
                                time.sleep(10)
                                continue

                            if "DBClusterSnapshotNotFound" in ex.message:
                                break
                            raise ex

    def delete_instance_snapshots_by_tags(self, tag_filter_expression):
        delete_filter = TagFilterExpression(tag_filter_expression)
        snapshots = []
        for s in self.rds_service.describe(services.rds_service.DB_SNAPSHOTS, region=self.region, tags=True):
            if delete_filter.is_match(s.get("Tags")):
                snapshots.append(s["DBSnapshotIdentifier"])
        self.delete_instance_snapshots(snapshots)

    def delete_cluster_snapshots_by_tags(self, tag_filter_expression):
        delete_filter = TagFilterExpression(tag_filter_expression)
        snapshots = []
        for s in self.rds_service.describe(services.rds_service.DB_CLUSTER_SNAPSHOTS, region=self.region, tags=True):
            if delete_filter.is_match(s.get("Tags")):
                snapshots.append(s["DBSClusterSnapshotIdentifier"])
        self.delete_cluster_snapshots(snapshots)

    def delete_db_instance_snapshots(self, instance_identifier):
        db_snapshots = self.get_db_instance_snapshots(instance_identifier)
        for snapshot in db_snapshots:
            self.delete_instance_snapshots(snapshot["DBSnapshotIdentifier"])

    def delete_db_cluster_snapshots(self, cluster_identifier):
        db_snapshots = self.get_db_cluster_snapshots(cluster_identifier)
        for snapshot in db_snapshots:
            self.delete_cluster_snapshots(snapshot["DBClusterSnapshotIdentifier"])

    def get_db_instance_snapshots(self, instance_identifier):
        db_snapshots = self.rds_service.describe(services.rds_service.DB_SNAPSHOTS, DBInstanceIdentifier=instance_identifier)
        return db_snapshots

    def get_db_cluster_snapshots(self, cluster_identifier, snapshot_type="manual"):
        db_snapshots = self.rds_service.describe(services.rds_service.DB_CLUSTER_SNAPSHOTS,
                                                 DBClusterIdentifier=cluster_identifier,
                                                 SnapshotType=snapshot_type)
        return db_snapshots

    def get_resource_tags(self, arn, default=None):
        # noinspection PyBroadException
        try:
            tag_list = self.rds_client.list_tags_for_resource(ResourceName=arn).get("TagList", {})
            return {t["Key"]: t["Value"] for t in tag_list}
        except Exception:
            return default

    def get_instance_tags(self, instance_id):
        arn = "arn:aws:rds:{}:{}:db:{}".format(self.session.region_name, self.account, instance_id)
        return self.get_resource_tags(arn)

    def get_cluster_tags(self, cluster_id):
        arn = "arn:aws:rds:{}:{}:cluster:{}".format(self.session.region_name, self.account, cluster_id)
        return self.get_resource_tags(arn)

    def get_instance_snapshot_tags(self, snapshot_id):
        arn = "arn:aws:rds:{}:{}:snapshot:{}".format(self.session.region_name, self.account, snapshot_id)
        return self.get_resource_tags(arn)

    def get_cluster_snapshot_tags(self, snapshot_id):
        arn = "arn:aws:rds:{}:{}:cluster-snapshot:{}".format(self.session.region_name, self.account, snapshot_id)
        return self.get_resource_tags(arn)

    def restore_resource_tags(self, resource_id, resource_type, tags):
        arn = "arn:aws:rds:{}:{}:{}:{}".format(self.session.region_name, self.account, resource_type, resource_id)
        existing_tags = self.get_resource_tags(arn, [])
        self.rds_client.remove_tags_from_resource(ResourceName=arn,
                                                  TagKeys=[t for t in existing_tags if t not in tags and not t.startswith("aws:")])
        self.rds_client.add_tags_to_resource(ResourceName=arn, Tags=tag_key_value_list(tags))

    def get_instance_snapshot_shared_accounts(self, instance_snapshot_id):
        resp = self.rds_client.describe_db_snapshot_attributes(DBSnapshotIdentifier=instance_snapshot_id)
        accounts = [a["AttributeValues"] for a in resp.get("DBSnapshotAttributesResult", {}).get("DBSnapshotAttributes", []) if
                    a["AttributeName"] == "restore"]
        if len(accounts) > 0:
            return accounts[0]
        else:
            return []

    def get_cluster_snapshot_shared_accounts(self, cluster_snapshot_id):
        resp = self.rds_client.describe_db_cluster_snapshot_attributes(DBClusterSnapshotIdentifier=cluster_snapshot_id)
        accounts = [a["AttributeValues"] for a in
                    resp.get("DBClusterSnapshotAttributesResult", {}).get("DBClusterSnapshotAttributes", []) if
                    a["AttributeName"] == "restore"]
        if len(accounts) > 0:
            return accounts[0]
        else:
            return []

    def create_instance(self,
                        db_instance_identifier,
                        db_instance_class,
                        db_name=None,
                        vpc_security_group_ids=None,
                        engine="mysql",
                        allocated_storage=8,
                        backup_retention_period=0,
                        master_username="Administrator",
                        master_user_password=None,
                        db_cluster_identifier=None,
                        tags=None,
                        publicly_accessible=False,
                        timeout=30 * 60):

        args = {
            "DBInstanceIdentifier": db_instance_identifier,
            "DBInstanceClass": db_instance_class,
            "Engine": engine,
            "AllocatedStorage": allocated_storage,
            "BackupRetentionPeriod": backup_retention_period,
            "MasterUsername": master_username,
            "MasterUserPassword": master_user_password if master_user_password is not None else str(uuid.uuid4()),
            "Tags": tagging.tag_key_value_list(tags if tags is not None else {}),
            "PubliclyAccessible": publicly_accessible
        }

        if vpc_security_group_ids is not None:
            args["VpcSecurityGroupIds"] = vpc_security_group_ids if isinstance(vpc_security_group_ids, list) else [
                vpc_security_group_ids]

        if db_cluster_identifier is not None:
            args["DBClusterIdentifier"] = db_cluster_identifier
            del args["AllocatedStorage"]
            del args["MasterUsername"]
            del args["MasterUserPassword"]
            del args["BackupRetentionPeriod"]

        if db_name is not None:
            args["DBName"] = db_name

        # noinspection PyBroadException
        try:
            db_instance = self.rds_client.create_db_instance(**args).get("DBInstance", {})
            db_instance_id = db_instance.get("DBInstanceIdentifier")
            if db_instance_id is None:
                return None

            if db_instance.get("DBInstanceStatus") != "creating":
                return False

            return db_instance if self.wait_until_instance_status(db_instance_id=db_instance_id, status="available",
                                                                  timeout=timeout) else None

        except Exception as ex:
            print(ex)
            return None

    def create_cluster(self,
                       db_cluster_identifier,
                       master_username="Administrator",
                       db_name=None,
                       engine="aurora",
                       backup_retention_period=1,
                       master_user_password=None,
                       tags=None,
                       timeout=30 * 60):

        args = {
            "DBClusterIdentifier": db_cluster_identifier,
            "Engine": engine,
            "MasterUsername": master_username,
            "MasterUserPassword": master_user_password if master_user_password is not None else str(uuid.uuid4()),
            "BackupRetentionPeriod": backup_retention_period,
            "Tags": tagging.tag_key_value_list(tags if tags is not None else {}),
            "SourceRegion": self.region
        }

        if db_name is not None:
            args["DBName"] = db_name

        # noinspection PyBroadException,PyPep8
        try:
            db_cluster = self.rds_client.create_db_cluster(**args).get("DBCluster", {})
            db_cluster_id = db_cluster.get("DBClusterIdentifier")
            if db_cluster_id is None:
                return None

            if db_cluster.get("Status") != "creating":
                return False

            if not self.wait_until_cluster_status(db_cluster_id=db_cluster_id,
                                                  status="available",
                                                  timeout=timeout):
                return None

            if not self.wait_cluster_members_in_state(db_cluster_id=db_cluster_id, state="available"):
                return None

            return db_cluster

        except:
            return None

    def wait_cluster_members_in_state(self, db_cluster_id, state, timeout=30 * 60):

        with Timer(timeout_seconds=timeout) as t:
            while True:

                if t.timeout:
                    return False

                members = self.rds_service.describe(services.rds_service.DB_INSTANCES,
                                                    region=self.region,
                                                    Filters=[{"Name": "db-cluster-id", "Values": [db_cluster_id]}])

                if all([m["DBInstanceStatus"] == state for m in members]):
                    return True

                time.sleep(15)

    def delete_instance(self, db_instance_id, timeout=60 * 15):

        inst = self.get_instance(instance_id=db_instance_id, tags=False)
        if inst is None:
            return True

        if inst["DBInstanceStatus"] == "deleting":
            while True:
                with Timer(timeout_seconds=timeout) as t:
                    if t.timeout:
                        return False
                inst = self.get_instance(instance_id=db_instance_id, tags=False)
                if inst is None:
                    return True
                time.sleep(15)

        self.rds_client.delete_db_instance(DBInstanceIdentifier=db_instance_id, SkipFinalSnapshot=True)

        while True:
            with Timer(timeout_seconds=timeout) as t:
                if t.timeout:
                    return False

            instance = self.get_instance(instance_id=db_instance_id, tags=False)
            if instance is None:
                return
            time.sleep(15)

    def delete_cluster(self, db_cluster_id, timeout=60 * 15):

        cluster = self.get_cluster(cluster_id=db_cluster_id, tags=False)
        if cluster is None:
            return True

        if cluster["Status"] == "deleting":
            while True:
                with Timer(timeout_seconds=timeout) as t:
                    if t.timeout:
                        return False
                cluster = self.get_instance(instance_id=db_cluster_id, tags=False)
                if cluster is None:
                    return True
                time.sleep(15)

        self.start_cluster(db_cluster_id=db_cluster_id, timeout=timeout)

        for member_id in [m["DBInstanceIdentifier"] for m in cluster.get("DBClusterMembers", [])]:
            self.delete_instance(member_id)

        self.rds_client.delete_db_cluster(DBClusterIdentifier=db_cluster_id, SkipFinalSnapshot=True)

        while True:
            with Timer(timeout_seconds=timeout) as t:
                if t.timeout:
                    return False

            instance = self.get_cluster(cluster_id=db_cluster_id, tags=False)
            if instance is None:
                return True
            time.sleep(15)

    def stop_instance(self, db_instance_id, timeout=30 * 60):
        inst = self.get_instance(instance_id=db_instance_id, tags=False)
        if inst is None:
            return False

        status = inst.get("DBInstanceStatus")

        if status == "stopped":
            return True

        if status == "starting":
            if not self.wait_until_instance_status(db_instance_id=db_instance_id, status="available", timeout=timeout / 2):
                return False

        if status == "stopping":
            return self.wait_until_instance_status(db_instance_id=db_instance_id, status="stopping", timeout=timeout)

        self.rds_client.stop_db_instance(DBInstanceIdentifier=db_instance_id)

        return self.wait_until_instance_status(db_instance_id=db_instance_id, status="stopped", timeout=timeout)

    def stop_cluster(self, db_cluster_id, timeout=15 * 60):
        cluster = self.get_cluster(cluster_id=db_cluster_id, tags=False)
        if cluster is None:
            return False

        status = cluster.get("Status")

        if status == "stopped":
            return True

        if status == "starting":
            if not self.wait_until_cluster_status(db_cluster_id=db_cluster_id, status="available", timeout=timeout / 2):
                return False

        if status == "stopping":
            return self.wait_until_cluster_status(db_cluster_id=db_cluster_id, status="stopping", timeout=timeout)

        self.rds_client.stop_db_cluster(DBClusterIdentifier=db_cluster_id)

        if not self.wait_until_cluster_status(db_cluster_id=db_cluster_id, status="stopped", timeout=timeout):
            return False

        return self.wait_cluster_members_in_state(db_cluster_id=db_cluster_id, state="stopped")

    def start_instance(self, db_instance_id, timeout=15 * 60):

        inst = self.get_instance(instance_id=db_instance_id, tags=False)
        if inst is None:
            return False

        status = inst.get("DBInstanceStatus")

        if status == "available":
            return True

        if status == "stopping":
            if not self.wait_until_instance_status(db_instance_id=db_instance_id, status="stopped", timeout=timeout / 2):
                return False

        if status == "starting":
            return self.wait_until_instance_status(db_instance_id=db_instance_id, status="available", timeout=timeout)

        self.rds_client.start_db_instance(DBInstanceIdentifier=db_instance_id)
        return self.wait_until_instance_status(db_instance_id=db_instance_id, status="available", timeout=timeout)

    def start_cluster(self, db_cluster_id, timeout=15 * 60):

        cluster = self.get_cluster(cluster_id=db_cluster_id, tags=False)
        if cluster is None:
            return False

        status = cluster.get("Status")

        if status == "available":
            return True

        if status == "stopping":
            if not self.wait_until_cluster_status(db_cluster_id=db_cluster_id, status="stopped", timeout=timeout / 2):
                return False

        if status == "starting":
            return self.wait_until_cluster_status(db_cluster_id=db_cluster_id, status="available", timeout=timeout)

        self.rds_client.start_db_cluster(DBClusterIdentifier=db_cluster_id)
        if not self.wait_until_cluster_status(db_cluster_id=db_cluster_id, status="available", timeout=timeout):
            return False

        return self.wait_cluster_members_in_state(db_cluster_id=db_cluster_id, state="available")

    def wait_until_instance_status(self, db_instance_id, status, timeout):

        if not isinstance(status, list):
            status = [status]

        while True:
            with Timer(timeout_seconds=timeout) as t:
                if t.timeout:
                    return False
            time.sleep(15)
            current_status = self.get_instance(instance_id=db_instance_id, tags=False).get("DBInstanceStatus")
            if current_status in status:
                return True

    def wait_until_cluster_status(self, db_cluster_id, status, timeout):

        if not isinstance(status, list):
            status = [status]

        while True:
            with Timer(timeout_seconds=timeout) as t:
                if t.timeout:
                    return False
            time.sleep(15)
            current_status = self.get_cluster(cluster_id=db_cluster_id, tags=False).get("Status")
            if current_status in status:
                return True

    def instance_exists(self, db_instance_id):
        return self.get_instance(instance_id=db_instance_id, tags=False) is not None
