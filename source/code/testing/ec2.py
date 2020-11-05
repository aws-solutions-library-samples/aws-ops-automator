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
import re as regex
import time
from functools import cmp_to_key

import boto3
import boto3.exceptions
import dateutil.parser

import services.ec2_service
import services.kms_service
from helpers.timer import Timer
from tagging.tag_filter_expression import TagFilterExpression


class Ec2(object):

    def __init__(self, region=None, session=None):
        """
        Initialize aws ec2 ec2 service.

        Args:
            self: (todo): write your description
            region: (str): write your description
            session: (todo): write your description
        """
        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.ec2_client = self.session.client("ec2", region_name=self.region)
        self.ec2_service = services.ec2_service.Ec2Service(session=self.session)

        self._latest_aws_image = None
        self._ebs_default_key_arn = None

    @property
    def latest_aws_linux_image(self):
        """
        Compute the aws aws aws aws aws image.

        Args:
            self: (todo): write your description
        """
        def compare_date(a,b):
            """
            Compares two datetime objects.

            Args:
                a: (todo): write your description
                b: (todo): write your description
            """
            return int(
                (dateutil.parser.parse(a["CreationDate"]) - dateutil.parser.parse(b["CreationDate"])).total_seconds()
                )
        if self._latest_aws_image is None:
            # noinspection PyPep8
            images = sorted(list(
                self.ec2_service.describe(services.ec2_service.IMAGES,
                                          region=self.region, Owners=["amazon"],
                                          Filters=[
                                              {
                                                  "Name": "owner-alias",
                                                  "Values": ["amazon"]
                                              },
                                              {
                                                  "Name": "state",
                                                  "Values": ["available"]
                                              },
                                              {
                                                  "Name": "name",
                                                  "Values": ["amzn-ami-hvm-????.??.?.????????-x86_64-gp2"]
                                              }
                                          ],
                                          ExecutableUsers=["all"])),
                key=cmp_to_key(compare_date),
                reverse=True)
            assert (len(images) > 0)
            self._latest_aws_image = images[0]
        return self._latest_aws_image

    @property
    def latest_aws_windows_core_image(self):
        """
        Latest image image image

        Args:
            self: (todo): write your description
        """
        def compare_date(a,b):
            """
            Compares two datetime objects.

            Args:
                a: (todo): write your description
                b: (todo): write your description
            """
            return int(
                (dateutil.parser.parse(a["CreationDate"]) - dateutil.parser.parse(b["CreationDate"])).total_seconds()
                )
        if self._latest_aws_image is None:
            # noinspection PyPep8
            images = sorted(list(
                self.ec2_service.describe(services.ec2_service.IMAGES,
                                          region=self.region, Owners=["amazon"],
                                          Filters=[
                                              {
                                                  "Name": "owner-alias",
                                                  "Values": ["amazon"]
                                              },
                                              {
                                                  "Name": "state",
                                                  "Values": ["available"]
                                              },
                                              {
                                                  "Name": "name",
                                                  "Values": ["Windows_Server-2016-English-Full-Base-????.??.??"]
                                              }
                                          ],
                                          ExecutableUsers=["all"])),
                key=cmp_to_key(compare_date),
                reverse=True)
            assert (len(images) > 0)
            self._latest_aws_image = images[0]
        return self._latest_aws_image

    def get_default_vpc(self):
        """
        Returns the default vpc if it.

        Args:
            self: (todo): write your description
        """
        vpcs = [v for v in self.ec2_service.describe(services.ec2_service.VPCS) if v["IsDefault"]]
        if len(vpcs) != 0:
            return vpcs[0]
        return None

    def get_instance(self, instance_id):
        """
        Returns an ec2 instance.

        Args:
            self: (str): write your description
            instance_id: (str): write your description
        """
        # noinspection PyBroadException,PyPep8
        try:
            return self.ec2_service.get(services.ec2_service.INSTANCES,
                                        InstanceIds=[instance_id],
                                        region=self.region)
        except:
            return None

    def get_instance_tags(self, instance_id):
        """
        Retrieves an instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """
        return self.get_resource_tags("instance", instance_id)

    def get_instance_status(self, instance_id):
        """
        Return the status of an instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """
        instance = self.get_instance(instance_id)
        assert (instance is not None)
        return instance["State"]["Name"]

    def restore_instance_tags(self, instance_id, tags):
        """
        Restore an instance of an instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            tags: (list): write your description
        """
        self.restore_resource_tags(resource_id=instance_id, resource_type="instance", tags=tags)

    def get_root_volume(self, instance_id):
        """
        Returns the root volume.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """
        instance = self.get_instance(instance_id)
        root_device = instance["RootDeviceName"]
        root_volume = None
        for m in instance["BlockDeviceMappings"]:
            if m["DeviceName"] == root_device:
                root_volume = m["Ebs"]["VolumeId"]
                break

        assert (root_volume is not None)
        return root_volume

    def get_resource_tags(self, resource_type, resource_id):
        """
        Get all tags of the specified resource type.

        Args:
            self: (todo): write your description
            resource_type: (str): write your description
            resource_id: (str): write your description
        """
        response = self.ec2_client.describe_tags(Filters=[
            {
                "Name": "resource-type",
                "Values": [resource_type]
            },
            {
                "Name": "resource-id",
                "Values": [resource_id]
            }
        ])
        return {t["Key"]: t["Value"] for t in response.get("Tags", [])}

    @classmethod
    def is_exception_with_code(cls, ex, code):
        """
        Returns if the exception is an exception.

        Args:
            cls: (callable): write your description
            ex: (todo): write your description
            code: (str): write your description
        """
        return getattr(ex, "response", {}).get("Error", {}).get("Code", "") == code

    @classmethod
    def snapshot_not_found(cls, ex):
        """
        Returns a snapshot of the given exception.

        Args:
            cls: (todo): write your description
            ex: (str): write your description
        """
        return cls.is_exception_with_code(ex, "InvalidSnapshot.NotFound")

    @classmethod
    def snapshot_creation_per_volume_rate_exceeded(cls, ex):
        """
        Returns a snapshot snapshot of a snapshot that snapshot.

        Args:
            cls: (todo): write your description
            ex: (todo): write your description
        """
        return cls.is_exception_with_code(ex, "SnapshotCreationPerVolumeRateExceeded")

    @classmethod
    def snapshot_request_limit_exceeded(cls, ex):
        """
        Return a snapshot of a snapshot.

        Args:
            cls: (todo): write your description
            ex: (todo): write your description
        """
        return cls.is_exception_with_code(ex, "RequestLimitExceeded")

    @classmethod
    def image_not_found(cls, ex):
        """
        Return a : class : class is not found.

        Args:
            cls: (todo): write your description
            ex: (todo): write your description
        """
        return cls.is_exception_with_code(ex, "InvalidAMIID.NotFound")

    @classmethod
    def image_not_available(cls, ex):
        """
        Return a list of all available image class.

        Args:
            cls: (todo): write your description
            ex: (todo): write your description
        """
        return cls.is_exception_with_code(ex, "InvalidAMIID.Unavailable")

    def restore_volume_tags(self, image_id, tags):
        """
        Restore an image tags

        Args:
            self: (todo): write your description
            image_id: (str): write your description
            tags: (str): write your description
        """
        self.restore_resource_tags(resource_id=image_id, resource_type="volume", tags=tags)

    def get_snapshot(self, snapshot_id):
        """
        Returns the snapshot with the given snapshot

        Args:
            self: (todo): write your description
            snapshot_id: (str): write your description
        """
        try:
            return self.ec2_service.get(services.ec2_service.SNAPSHOTS, SnapshotIds=[snapshot_id], region=self.region)
        except Exception as ex:
            if self.snapshot_not_found(ex):
                return None
            raise ex

    def get_snapshot_status(self, snapshot_id):
        """
        Returns the status of a given snapshot

        Args:
            self: (todo): write your description
            snapshot_id: (str): write your description
        """
        snapshot = self.get_snapshot(snapshot_id)
        return snapshot.get("State") if snapshot is not None else None

    def delete_snapshots(self, snapshot_ids):
        """
        Deletes the specified snapshots

        Args:
            self: (todo): write your description
            snapshot_ids: (str): write your description
        """
        for snapshot_id in snapshot_ids:
            try:
                self.ec2_client.delete_snapshot(SnapshotId=snapshot_id)
            except Exception as ex:
                if not self.snapshot_not_found(ex):
                    raise ex

    def get_snapshot_tags(self, snapshot_id):
        """
        Gets the tags associated with the given snapshot.

        Args:
            self: (todo): write your description
            snapshot_id: (str): write your description
        """
        return self.get_resource_tags("snapshot", snapshot_id)

    def restore_snapshot_tags(self, image_id, tags):
        """
        Restore a snapshot of an image.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
            tags: (todo): write your description
        """
        self.restore_resource_tags(resource_id=image_id, resource_type="snapshot", tags=tags)

    def restore_resource_tags(self, resource_id, resource_type, tags):
        """
        Restore the tags from the specified resource.

        Args:
            self: (todo): write your description
            resource_id: (str): write your description
            resource_type: (str): write your description
            tags: (todo): write your description
        """
        existing_tags = self.get_resource_tags(resource_type, resource_id)
        self.ec2_client.delete_tags(Resources=[resource_id],
                                    Tags=[{"Key": t} for t in existing_tags if t not in tags and not t.startswith("aws:")])
        self.create_tags([resource_id], {t: tags[t] for t in tags if not t.startswith("aws:")})

    def get_snapshot_create_volume_permission_users(self, snapshot_id):
        """
        Returns a new ebs

        Args:
            self: (todo): write your description
            snapshot_id: (str): write your description
        """
        attributes = self.ec2_service.get(services.ec2_service.SNAPSHOT_ATTRIBUTE,
                                          SnapshotId=snapshot_id,
                                          Attribute="createVolumePermission",
                                          region=self.region)
        return [p["UserId"] for p in attributes.get("CreateVolumePermissions", []) if p.get("UserId") is not None]

    def get_volume_tags(self, volume_id):
        """
        Retrieves a list of a volume.

        Args:
            self: (todo): write your description
            volume_id: (str): write your description
        """
        return self.get_resource_tags("volume", volume_id)

    def create_tags(self, resource_ids, tags):
        """
        Create the tags to the given list of ids

        Args:
            self: (todo): write your description
            resource_ids: (str): write your description
            tags: (str): write your description
        """
        if tags is not None and len(tags) > 0:
            resources = resource_ids if isinstance(resource_ids, list) else [resource_ids]
            self.ec2_client.create_tags(Resources=resources, Tags=[{"Key": t, "Value": tags[t]} for t in tags])

    def create_snapshot(self, volume_id, tags=None, description=None, wait_to_complete=300):
        """
        Create a new snapshot

        Args:
            self: (todo): write your description
            volume_id: (str): write your description
            tags: (str): write your description
            description: (str): write your description
            wait_to_complete: (str): write your description
        """
        args = {
            "VolumeId": volume_id
        }
        if tags is not None:
            args["TagSpecifications"] = [{"ResourceType": "snapshot", "Tags": [{"Key": t, "Value": tags[t]} for t in tags]}]
        if description is not None:
            args["Description"] = description

        with Timer(timeout_seconds=wait_to_complete) as timer:
            while True:
                try:
                    if timer.timeout:
                        return None
                    snapshot_id = self.ec2_client.create_snapshot(**args)["SnapshotId"]
                    break
                except Exception as ex:
                    if self.snapshot_creation_per_volume_rate_exceeded(ex):
                        time.sleep(20)

            snapshot = self.get_snapshot(snapshot_id)
            if wait_to_complete == 0:
                return snapshot

            while True:
                if snapshot["State"] == "completed":
                    return snapshot
                if timer.timeout:
                    return None
                time.sleep(20)
                snapshot = self.get_snapshot(snapshot_id)
                if snapshot["State"] == "error":
                    return None

    def copy_snapshot(self, snapshot_id, destination_region, tags=None, description=None, wait_to_complete=300):
        """
        Copy a snapshot to another snapshot.

        Args:
            self: (todo): write your description
            snapshot_id: (str): write your description
            destination_region: (str): write your description
            tags: (list): write your description
            description: (str): write your description
            wait_to_complete: (str): write your description
        """

        ec2_destination = Ec2(region=destination_region)
        ec2_destination_client = boto3.client("ec2", region_name=destination_region)

        args = {
            "SourceSnapshotId": snapshot_id,
            "SourceRegion": self.region
        }

        if description is not None:
            args["Description"] = description

        with Timer(timeout_seconds=wait_to_complete) as timer:
            while True:
                try:
                    if timer.timeout:
                        return None
                    snapshot_copy_id = ec2_destination_client.copy_snapshot(**args)["SnapshotId"]
                    break
                except Exception as ex:
                    if self.snapshot_request_limit_exceeded(ex):
                        time.sleep(20)

            snapshot = ec2_destination.get_snapshot(snapshot_copy_id)
            if wait_to_complete == 0:
                if snapshot is not None:
                    ec2_destination.create_tags(resource_ids=[snapshot["SnapshotId"]], tags=tags)
                return snapshot

            while True:
                if snapshot["State"] == "completed":
                    ec2_destination.create_tags(resource_ids=[snapshot["SnapshotId"]], tags=tags)
                    return snapshot
                if timer.timeout:
                    return None
                time.sleep(20)
                snapshot = ec2_destination.get_snapshot(snapshot_copy_id)
                if snapshot["State"] == "error":
                    return None

    def get_snapshots_for_volume(self, volume_id, source_volume_tag=None):
        """
        Returns a list of snapshots associated with this volume

        Args:
            self: (todo): write your description
            volume_id: (str): write your description
            source_volume_tag: (str): write your description
        """

        def for_volume(snapshot):
            """
            Return true if the given volume is associated with the given snapshot

            Args:
                snapshot: (dict): write your description
            """
            if snapshot["VolumeId"] == volume_id:
                return True

            return source_volume_tag is not None and any(
                [t["Key"] == source_volume_tag and t.get("Value", "") == volume_id for t in snapshot.get("Tags", [])])

        return self.ec2_service.describe(services.ec2_service.SNAPSHOTS, filter_func=for_volume, OwnerIds=["self"])

    def delete_snapshots_by_tags(self, tag_filter_expression):
        """
        Deletes snapshots by tag_snapshots

        Args:
            self: (todo): write your description
            tag_filter_expression: (str): write your description
        """
        delete_filter = TagFilterExpression(tag_filter_expression)
        snapshots = []
        for s in self.ec2_service.describe(services.ec2_service.SNAPSHOTS, region=self.region, tags=True, OwnerIds=["self"]):
            if delete_filter.is_match(s.get("Tags")):
                snapshots.append(s["SnapshotId"])
        self.delete_snapshots(snapshots)

    @property
    def ebs_default_key_arn(self):
        """
        The default ebs key. gbs.

        Args:
            self: (todo): write your description
        """
        if self._ebs_default_key_arn is None:
            kms = services.kms_service.KmsService()
            aliases = [a for a in kms.describe(services.kms_service.ALIASES, region=self.region) if
                       a["AliasName"] == "alias/aws/ebs"]
            assert (len(aliases) == 1)
            self._ebs_default_key_arn = "arn:aws:kms:{}:{}:key/{}".format(self.region, aliases[0]["AwsAccount"],
                                                                          aliases[0]["TargetKeyId"])
        return self._ebs_default_key_arn

    def get_image(self, image_id):
        """
        Get an image by its id.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
        """
        try:
            return self.ec2_service.get(services.ec2_service.IMAGES,
                                        ImageIds=[image_id],
                                        region=self.region)
        except Exception as ex:
            if self.image_not_found(ex):
                return None
            raise ex

    def wait_for_image_available(self, image_id, timeout=300):
        """
        Waits for an image to appear

        Args:
            self: (todo): write your description
            image_id: (str): write your description
            timeout: (float): write your description
        """

        with Timer(timeout_seconds=timeout, start=True) as t:
            count = 0
            while not t.timeout:
                img = self.get_image(image_id)
                if img is not None and img.get("State") == "available":
                    count += 1
                    if count >= 3:
                        return True
                time.sleep(5)
        return False

    def wait_for_image_not_longer_available(self, image_id, timeout=300):
        """
        Waits for long running image to complete.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
            timeout: (int): write your description
        """

        with Timer(timeout_seconds=timeout, start=True) as t:
            count = 0
            while not t.timeout:
                img = self.get_image(image_id)
                if img is None:
                    count += 1
                    if count >= 3:
                        return True
                time.sleep(5)
        return False

    def get_images(self, image_ids):
        """
        : param image ids : param image_ids : list of image ids

        Args:
            self: (todo): write your description
            image_ids: (str): write your description
        """
        for img in image_ids:
            image = self.get_image(image_id=img)
            if image is not None:
                yield image

    def get_image_state(self, image_id):
        """
        Get the image state

        Args:
            self: (todo): write your description
            image_id: (str): write your description
        """
        image = self.get_image(image_id)
        if image is None:
            return None
        else:
            return image["State"]

    def deregister_image(self, image_id):
        """
        Deregisters an image.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
        """
        try:
            self.ec2_client.deregister_image(ImageId=image_id)
        except Exception as ex:
            if not self.image_not_available(ex):
                raise ex

    def get_image_snapshots(self, image_id):
        """
        Returns a snapshot of an image

        Args:
            self: (todo): write your description
            image_id: (str): write your description
        """
        image = self.get_image(image_id)
        if image is None:
            return None
        return [b["Ebs"]["SnapshotId"] for b in image["BlockDeviceMappings"] if "Ebs" in b]

    def delete_images(self, image_ids):
        """
        Deletes the image with given image.

        Args:
            self: (todo): write your description
            image_ids: (str): write your description
        """

        for image_id in image_ids:
            snapshots = self.get_image_snapshots(image_id)

            self.deregister_image(image_id)
            if snapshots is not None:
                self.delete_snapshots(snapshots)
            with Timer(timeout_seconds=300) as timer:
                image = self.get_image(image_id)
                if image is None:
                    return
                if timer.timeout:
                    raise "Timeout deleting image {}"
                time.sleep(10)

    def delete_images_by_tags(self, tag_filter_expression):
        """
        Delete all tags matching the given tag.

        Args:
            self: (todo): write your description
            tag_filter_expression: (str): write your description
        """
        delete_filter = TagFilterExpression(tag_filter_expression)
        images = []

        for s in self.ec2_service.describe(services.ec2_service.IMAGES, region=self.region, tags=True, Owners=["self"]):
            if delete_filter.is_match(s.get("Tags")):
                images.append(s["ImageId"])

        self.delete_images(images)

    def get_image_launch_permissions_users(self, image_id):
        """
        Returns a list of launch permissions for an image.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
        """
        attributes = self.ec2_service.get(services.ec2_service.IMAGE_ATTRIBUTE,
                                          ImageId=image_id,
                                          Attribute="launchPermission",
                                          region=self.region)
        return [p["UserId"] for p in attributes.get("LaunchPermissions", []) if p.get("UserId") is not None]

    def get_image_tags(self, image_id):
        """
        Get the list of an image.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
        """
        return self.get_resource_tags("image", image_id)

    def restore_image_tags(self, image_id, tags):
        """
        Restore the image tags from an image.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
            tags: (todo): write your description
        """
        self.restore_resource_tags(resource_id=image_id, resource_type="image", tags=tags)

    def create_image(self, instance_id, name, tags=None, description=None, no_reboot=True, wait_to_complete=600):
        """
        Create an instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            name: (str): write your description
            tags: (todo): write your description
            description: (str): write your description
            no_reboot: (str): write your description
            wait_to_complete: (str): write your description
        """

        args = {
            "InstanceId": instance_id,
            "Name": name,
            "NoReboot": no_reboot
        }

        if description is not None:
            args["Description"] = description

        with Timer(timeout_seconds=wait_to_complete) as timer:

            # noinspection PyBroadException
            try:
                image_id = self.ec2_client.create_image(**args)["ImageId"]

                image = self.get_image(image_id)
                if wait_to_complete == 0:
                    self.create_tags([image_id], tags=tags)
                    return self.get_image(image_id)

                while True:
                    if image["State"] == "available":
                        self.create_tags([image_id], tags=tags)

                        # there may be a tile lag between the image created and becoming visible in a new sessions.
                        while True:
                            img = Ec2(self.region, session=boto3.Session()).get_image(image["ImageId"])
                            if img is not None:
                                return img
                            if timer.timeout:
                                raise Exception("Image created but not returned by describe function")
                            time.sleep(10)

                    if timer.timeout:
                        return None
                    time.sleep(20)
                    image = self.get_image(image_id)
                    if image["State"] == "failed":
                        return None
            except Exception as ex:
                print(ex)
                return None

    def copy_image(self, image_id, destination_region, name, tags=None, description=None, wait_to_complete=300, encrypted=False):
        """
        Copy an image to an image to a new region.

        Args:
            self: (todo): write your description
            image_id: (str): write your description
            destination_region: (str): write your description
            name: (str): write your description
            tags: (str): write your description
            description: (str): write your description
            wait_to_complete: (str): write your description
            encrypted: (str): write your description
        """

        ec2_destination = Ec2(region=destination_region)
        ec2_destination_client = boto3.client("ec2", region_name=destination_region)

        args = {
            "SourceImageId": image_id,
            "SourceRegion": self.region,
            "Name": name,
            "Encrypted": encrypted
        }

        if description is not None:
            args["Description"] = description

        with Timer(timeout_seconds=wait_to_complete) as timer:

            # noinspection PyBroadException
            try:
                image_copy_id = ec2_destination_client.copy_image(**args)["ImageId"]
            except Exception:
                return None

            image_copy = ec2_destination.get_image(image_copy_id)
            if wait_to_complete == 0:
                if image_copy is not None:
                    ec2_destination.create_tags(resource_ids=[image_copy["ImageId"]], tags=tags)
                return image_copy

            while True:
                if image_copy is not None and image_copy["State"] == "available":
                    ec2_destination.create_tags(resource_ids=[image_copy["ImageId"]], tags=tags)
                    return image_copy
                if timer.timeout:
                    return None
                time.sleep(20)
                image_copy = ec2_destination.get_image(image_copy_id)
                if image_copy is not None and image_copy["State"] == "failed":
                    return None

    def get_volume(self, volume_id):
        """
        Get a ebs by its id

        Args:
            self: (todo): write your description
            volume_id: (str): write your description
        """
        # noinspection PyPep8,PyBroadException
        try:
            return self.ec2_service.get(services.ec2_service.VOLUMES, VolumeIds=[volume_id], tags=True, region=self.region)
        except:
            return None

    def start_instance(self, instance_id, timeout=600):
        """
        Starts the instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            timeout: (float): write your description
        """

        with Timer(timeout) as timer:

            while True:
                state = self.get_instance_status(instance_id)
                if state == "terminated":
                    raise Exception("Instance {} is terminated".format(instance_id))

                if state == "running":
                    return

                if state == "stopped":
                    self.ec2_client.start_instances(InstanceIds=[instance_id])

                elif timer.timeout:
                    raise Exception("Timeout starting instance {}, last status is {}".format(instance_id, state))

                time.sleep(10)

    def get_instance_volumes(self, instance_id):
        """
        Returns a list of an ebs

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """
        return self.ec2_service.describe(services.ec2_service.VOLUMES,
                                         region=self.region,
                                         tags=True,
                                         Filters=[
                                             {
                                                 "Name": "attachment.instance-id",
                                                 "Values": [instance_id]
                                             }])

    def stop_instance(self, instance_id, timeout=600):
        """
        Stops the instance gracefully.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            timeout: (float): write your description
        """
        with Timer(timeout) as timer:

            while True:
                state = self.get_instance_status(instance_id)
                if state == "terminated":
                    raise Exception("Instance {} is terminated".format(instance_id))

                if state == "stopped":
                    return

                if state == "running":
                    self.ec2_client.stop_instances(InstanceIds=[instance_id])

                elif timer.timeout:
                    raise Exception("Timeout starting instance {}, last status is {}".format(instance_id, state))

                time.sleep(10)

    def get_system_status(self, instance_id):
        """
        Returns system status of a system.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """
        status = self.ec2_service.get(services.ec2_service.INSTANCE_STATUS, region=self.region, InstanceIds=[instance_id])
        if status is None:
            return None
        return status["SystemStatus"]["Status"]

    def wait_for_system_ready(self, instance_id, timeout=600):
        """
        Waits until instance is ready.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            timeout: (float): write your description
        """

        with Timer(timeout_seconds=timeout, start=True) as t:
            while not t.timeout:
                status = self.get_system_status(instance_id=instance_id)
                if status is not None and status == "ok":
                    return True
                time.sleep(10)
        return False

    def resize_instance(self, instance_id, new_size):
        """
        Resize the instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            new_size: (int): write your description
        """

        start_after_resize = self.get_instance_status(instance_id) in ["running", "pending"]
        if self.get_instance(instance_id)["InstanceType"] != new_size:
            if start_after_resize:
                self.stop_instance(instance_id=instance_id)
            self.ec2_client.modify_instance_attribute(InstanceId=instance_id, InstanceType={"Value": new_size})
        if start_after_resize:
            self.start_instance(instance_id=instance_id)

    def wait_for_instance_stopped(self, instance_id, stop_if_running=False, timeout=600):
        """
        Wait for the instance is stopped.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            stop_if_running: (todo): write your description
            timeout: (float): write your description
        """
        with Timer(timeout) as timer:

            while True:
                state = self.get_instance_status(instance_id)
                if state == "terminated":
                    raise Exception("Instance {} is terminated".format(instance_id))

                if state == "stopped":
                    return

                if state == "running" and stop_if_running:
                    self.ec2_client.stop_instances(InstanceIds=[instance_id])

                elif timer.timeout:
                    raise Exception(
                        "Timeout waiting for instance to stop instance {}, last status is {}".format(instance_id, state))

                time.sleep(10)

    def set_all_volumes_to_delete_on_terminate(self, instance_id):
        """
        Set all ebs volume on an ec2

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """
        mappings = [
            {
                "DeviceName": m["DeviceName"],
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeId": m["Ebs"]["VolumeId"]
                }
            }
            for m in self.get_instance(instance_id)["BlockDeviceMappings"]]

        self.ec2_client.modify_instance_attribute(InstanceId=instance_id, BlockDeviceMappings=mappings)

    def create_instance(self, instance_type, key_pair=None, root_vol_size=None, role_name=None, image_id=None, tags=None,
                        wait_to_complete=300,
                        hibernation=False):
        """
        Create an ec2 instance.

        Args:
            self: (todo): write your description
            instance_type: (str): write your description
            key_pair: (str): write your description
            root_vol_size: (int): write your description
            role_name: (str): write your description
            image_id: (str): write your description
            tags: (list): write your description
            wait_to_complete: (str): write your description
            hibernation: (str): write your description
        """

        used_image = image_id if image_id is not None else self.latest_aws_linux_image["ImageId"]
        args = {
            "ImageId": used_image,

            "InstanceType": instance_type,

            "TagSpecifications": [] if tags is None else [
                {
                    "ResourceType": "instance", "Tags": [{"Key": t, "Value": tags[t]} for t in tags]
                }
            ],

            "NetworkInterfaces": [
                {
                    "AssociatePublicIpAddress": True,
                    "DeleteOnTermination": True,
                    "DeviceIndex": 0

                },
            ],
            "MinCount": 1,
            "MaxCount": 1,
            "HibernationOptions": {
                "Configured": hibernation
            }
        }

        if key_pair is not None:
            args["KeyName"] = key_pair
        if role_name is not None:
            args["IamInstanceProfile"] = {"Name": role_name}

        if root_vol_size is not None:
            image = self.ec2_service.get(services.ec2_service.IMAGES, ImageIds=[used_image])
            for m in image["BlockDeviceMappings"]:
                if m["DeviceName"] == image["RootDeviceName"]:
                    args["BlockDeviceMappings"] = {
                                                      "DeviceName": m["DeviceName"],
                                                      "Ebs": {
                                                          "VolumeSize": root_vol_size,

                                                      }
                                                  },
                    break

        resp = self.ec2_client.run_instances(**args)
        instance_id = resp["Instances"][0]["InstanceId"]
        time.sleep(15)

        with Timer(timeout_seconds=wait_to_complete) as timer:
            while True:
                status = self.get_instance_status(instance_id)
                if status == "running":
                    return self.get_instance(instance_id)
                if timer.timeout:
                    return None
                time.sleep(15)

    def terminate_instance(self, instance_id, wait_to_complete=300):
        """
        Terminate the instance

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            wait_to_complete: (str): write your description
        """

        status = self.get_instance_status(instance_id)
        if status in [None, "terminated"]:
            return True

        self.ec2_client.terminate_instances(InstanceIds=[instance_id])

        time.sleep(15)

        with Timer(timeout_seconds=wait_to_complete) as timer:
            while True:
                status = self.get_instance_status(instance_id)
                if status == "terminated":
                    return True
                if timer.timeout:
                    return False
                time.sleep(10)

    def allocate_address(self, instance_id=None):
        """
        Allocate an ec2 instance.

        Args:
            self: (todo): write your description
            instance_id: (int): write your description
        """
        address = self.ec2_client.allocate_address(
            Domain="vpc"
        )

        assert (address is not None)

        if instance_id is not None:
            self.ec2_client.associate_address(AllocationId=address["AllocationId"], InstanceId=instance_id)

        return address

    def wait_for_volume_state(self, volume_id, state, timeout=300):
        """
        Wait for an ebs volume to have a given volume id.

        Args:
            self: (todo): write your description
            volume_id: (str): write your description
            state: (str): write your description
            timeout: (float): write your description
        """

        with Timer(timeout_seconds=timeout, start=True) as t:
            count = 0
            while not t.timeout:
                img = self.get_volume(volume_id)
                if img is not None and img.get("State") == state:
                    count += 1
                    if count >= 3:
                        return True
                time.sleep(5)
        return False

    def add_instance_volume(self, instance_id=None, device=None, tags=None):
        """
        Add an ebs volume to an ec2 instance.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
            device: (todo): write your description
            tags: (list): write your description
        """
        instance = self.get_instance(instance_id=instance_id)
        assert instance is not None
        args = {
            "AvailabilityZone": instance["Placement"]["AvailabilityZone"],
            "Size": 1
        }

        if tags is not None:
            for t in tags:
                args["TagSpecifications"] = [
                    {
                        "ResourceType": "volume",
                        "Tags": [
                            {
                                "Key": t,
                                "Value": tags[t]
                            }]
                    }]

        vol = self.ec2_client.create_volume(**args)
        assert self.wait_for_volume_state(volume_id=vol['VolumeId'], state="available")

        if device is None:
            device = self.get_next_device_name(instance_id)

        self.ec2_client.attach_volume(Device=device, InstanceId=instance_id, VolumeId=vol["VolumeId"])

        return vol

    def delete_volume(self, volume_id, forced=False):
        """
        Delete a ebs volume

        Args:
            self: (todo): write your description
            volume_id: (str): write your description
            forced: (bool): write your description
        """
        vol = self.get_volume(volume_id)

        if vol is None:
            return

        if forced:
            attachments = vol.get("Attachments", [])
            for a in [i for i in attachments if i["State"] == "attached"]:
                self.ec2_client.detach_volume(
                    Device=a["Device"],
                    Force=True,
                    InstanceId=a["InstanceId"],
                    VolumeId=volume_id
                )

        assert self.wait_for_volume_state(volume_id=vol["VolumeId"], state="available")

        self.ec2_client.delete_volume(VolumeId=volume_id)

    # noinspection PyPep8
    def get_next_device_name(self, instance_id):
        """
        Returns the next device name.

        Args:
            self: (todo): write your description
            instance_id: (str): write your description
        """

        instance = self.get_instance(instance_id)
        assert instance is not None

        # finds first available device name for image
        root_device = instance["RootDeviceName"]

        m = regex.match("^/dev/(((?P<sd>sd)[a-z]\d?)|((?P<xvd>xvd)[a-z]))$", root_device)
        if m is None:
            return None

        p = m.group("sd", "xvd")
        device_prefix = "/dev/" + (p[0] if p[0] is not None else p[1])

        devices = [d["DeviceName"] for d in instance["BlockDeviceMappings"]]

        for a in [chr(i) for i in range(ord('f'), ord('p') + 1)]:
            if device_prefix + a not in devices:
                return device_prefix + a
        return None

    def release_addresses(self, allocation_ids):
        """
        Release all ec2 elastic ip addresses

        Args:
            self: (todo): write your description
            allocation_ids: (str): write your description
        """
        addresses = self.ec2_service.describe(services.ec2_service.ADDRESSES,
                                              region=self.region,
                                              Filters=[
                                                  {
                                                      "Name": "allocation-id", "Values": allocation_ids
                                                  }])

        for a in addresses:
            if a.get("InstanceId", None) is not None:
                self.ec2_client.disassociate_address(AssociationId=a["AssociationId"])

            self.ec2_client.release_address(AllocationId=a["AllocationId"])

    def get_address(self, allocation_id):
        """
        Returns an elastic ip address.

        Args:
            self: (todo): write your description
            allocation_id: (str): write your description
        """
        address = self.ec2_service.get(services.ec2_service.ADDRESSES,
                                       region=self.region,
                                       Filters=[
                                           {
                                               "Name": "allocation-id", "Values": [allocation_id]
                                           }])
        return address

    def get_default_security_group(self, vpc_id):
        """
        Returns the default security group for a vpc.

        Args:
            self: (todo): write your description
            vpc_id: (str): write your description
        """

        filters = [
            {
                "Name": "vpc-id",
                "Values": [
                    vpc_id,
                ]
            },
            {
                "Name": "description",
                "Values": [
                    "default VPC security group",
                ]
            }
        ]

        return self.ec2_service.get(services.ec2_service.SECURITY_GROUPS,
                                    region=self.region,
                                    Filters=filters)

    def delete_key_pair(self, key_pairname):
        """
        Deletes a public key pair.

        Args:
            self: (todo): write your description
            key_pairname: (str): write your description
        """
        try:
            self.ec2_client.delete_key_pair(KeyName=key_pairname)
        except Exception as ex:
            if "InvalidKeyPair.NotFound" not in ex.message:
                raise ex
