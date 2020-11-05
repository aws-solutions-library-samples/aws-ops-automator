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
import boto3


class S3(object):

    def __init__(self):
        """
        Initialize the s3.

        Args:
            self: (todo): write your description
        """
        self._s3_resources = None

    @property
    def s3_resources(self):
        """
        Str : class : class : class.

        Args:
            self: (todo): write your description
        """
        if self._s3_resources is None:
            self._s3_resources = boto3.resource("s3")
        return self._s3_resources

    def empty_bucket(self, bucket, exception_if_not_exists=True):
        """
        Checks if bucket exists.

        Args:
            self: (todo): write your description
            bucket: (todo): write your description
            exception_if_not_exists: (todo): write your description
        """
        try:
            self.s3_resources.Bucket(bucket).objects.all().delete()
        except Exception as ex:
            if type(ex).__name__ == "NoSuchBucket" and not exception_if_not_exists:
                pass
            else:
                raise ex

    def get_object(self, bucket, key):
        """
        Get object from s3.

        Args:
            self: (todo): write your description
            bucket: (str): write your description
            key: (str): write your description
        """
        # noinspection PyBroadException
        try:
            body = self.s3_resources.Object(bucket, key).get()["Body"]
            return body.read().decode('utf-8').split("\n")
        except Exception:
            return None

    def put_object(self, bucket, filename, key=None):
        """
        Copy an object to s3.

        Args:
            self: (todo): write your description
            bucket: (str): write your description
            filename: (str): write your description
            key: (str): write your description
        """
        if key is None:
            key = filename
        with open(filename) as f:
            self.s3_resources.Bucket(bucket).put_object(Key=key, Body=f)

    def get_bucket_tags(self, bucket):
        """
        Get all tags.

        Args:
            self: (todo): write your description
            bucket: (str): write your description
        """
        bucket_tagging = self.s3_resources.BucketTagging(bucket)
        return {t["Key"]: t["Value"] for t in bucket_tagging.tag_set}

    def delete_object(self, bucket, key, exception_if_bucket_not_exists=True):
        """
        Delete an object from s metadata.

        Args:
            self: (todo): write your description
            bucket: (str): write your description
            key: (str): write your description
            exception_if_bucket_not_exists: (todo): write your description
        """
        try:
            self.s3_resources.Object(bucket, key).delete()

        except Exception as ex:
            if type(ex).__name__ == "NoSuchBucket" and not exception_if_bucket_not_exists:
                pass
            else:
                raise ex

    def delete_bucket(self, bucket, exception_if_bucket_not_exists=True, delete_objects=True):
        """
        Deletes the given bucket.

        Args:
            self: (todo): write your description
            bucket: (str): write your description
            exception_if_bucket_not_exists: (todo): write your description
            delete_objects: (bool): write your description
        """
        try:
            if delete_objects:
                self.empty_bucket(bucket, exception_if_bucket_not_exists)
            self.s3_resources.Bucket(bucket).delete()

        except Exception as ex:
            if type(ex).__name__ == "NoSuchBucket" and not exception_if_bucket_not_exists:
                pass
            else:
                raise ex
