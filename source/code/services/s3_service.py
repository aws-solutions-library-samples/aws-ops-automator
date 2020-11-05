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

from helpers import as_namedtuple
from services.aws_service import AwsService

BUCKET_ACCELERATE_CONFIGURATION = "BucketAccelerateConfiguration"
BUCKET_ACL = "BucketAcl"
BUCKET_ANALYTICS_CONFIGURATION = "BucketAnalyticsConfiguration"
BUCKET_CORS = "BucketCors"
BUCKET_INVENTORY_CONFIGURATION = "BucketInventoryConfiguration"
BUCKET_LIFECYCLE = "BucketLifecycle"
BUCKET_LIFECYCLE_CONFIGURATION = "BucketLifecycleConfiguration"
BUCKET_LOCATION = "BucketLocation"
BUCKET_LOGGING = "BucketLogging"
BUCKET_METRIC_CONFIGURATION = "BucketMetricsConfiguration"
BUCKET_NOTIFICATION = "BucketNotification"
BUCKET_NOTIFICATION_CONFIGURATION = "BucketNotificationConfiguration"
BUCKET_POLICY = "BucketPolicy"
BUCKET_REPLICATION = "BucketReplication"
BUCKET_REQUEST_PAYMENT = "BucketRequestPayment"
BUCKET_TAGGING = "BucketTagging"
BUCKET_VERSIONING = "BucketVersioning"
BUCKET_WEBSITE = "BucketWebsite"
BUCKETS = "Buckets"
MULTIPART_UPLOADS = "MultipartUploads"
OBJECT = "Object"
OBJECT_ACL = "ObjectAcl"
OBJECT_TAGGING = "ObjectTagging"
OBJECT_TORRENT = "ObjectTorrent"
OBJECT_VERSIONS = "ObjectVersions"
OBJECTS = "Objects"
PARTS = "Parts"

CUSTOM_RESULT_PATHS = {
    MULTIPART_UPLOADS: "Uploads",
    OBJECT_VERSIONS: "[[Versions][].{Versions:@},[DeleteMarkers][].{DeleteMarkers:@}][]",
    OBJECTS: "Contents",
    BUCKET_ACCELERATE_CONFIGURATION: '{"Status": Status}',
    BUCKET_ACL: "",
    BUCKET_ANALYTICS_CONFIGURATION: "",
    BUCKET_CORS: "CORSRules",
    BUCKET_INVENTORY_CONFIGURATION: "InventoryConfiguration",
    BUCKET_LIFECYCLE: "Rules",
    BUCKET_LIFECYCLE_CONFIGURATION: "Rules",
    BUCKET_LOCATION: "",
    BUCKET_LOGGING: "",
    BUCKET_METRIC_CONFIGURATION: "MetricsConfiguration",
    BUCKET_NOTIFICATION: "",
    BUCKET_NOTIFICATION_CONFIGURATION: "",
    BUCKET_POLICY: '{ "Policy" : Policy }',
    BUCKET_REPLICATION: "ReplicationConfiguration",
    BUCKET_REQUEST_PAYMENT: '{"Payer" : Payer}',
    BUCKET_TAGGING: "TagSet",
    BUCKET_VERSIONING: "",
    BUCKET_WEBSITE: "",
    OBJECT: "",
    OBJECT_ACL: "",
    OBJECT_TAGGING: "TagSet",
    OBJECT_TORRENT: ""
}

RESOURCE_NAMES = [
    BUCKETS,
    MULTIPART_UPLOADS,
    OBJECT_VERSIONS,
    OBJECTS,
    PARTS,
    BUCKET_ACCELERATE_CONFIGURATION,
    BUCKET_ACL,
    BUCKET_ANALYTICS_CONFIGURATION,
    BUCKET_CORS,
    BUCKET_INVENTORY_CONFIGURATION,
    BUCKET_LIFECYCLE,
    BUCKET_LIFECYCLE_CONFIGURATION,
    BUCKET_LOCATION,
    BUCKET_LOGGING,
    BUCKET_METRIC_CONFIGURATION,
    BUCKET_NOTIFICATION,
    BUCKET_NOTIFICATION_CONFIGURATION,
    BUCKET_POLICY,
    BUCKET_REPLICATION,
    BUCKET_REQUEST_PAYMENT,
    BUCKET_TAGGING,
    BUCKET_VERSIONING,
    BUCKET_WEBSITE,
    OBJECT,
    OBJECT_ACL,
    OBJECT_TAGGING,
    OBJECT_TORRENT
]

RESOURCES_WITH_TAGS = [
    OBJECTS,
    BUCKETS
]

CONTINUATION_DATA = {
    MULTIPART_UPLOADS: [
        "KeyMarker",
        "NextKeyMarker",
        "MaxUploads"
    ],
    OBJECT_VERSIONS: [
        "KeyMarker",
        "NextKeyMarker",
        "MaxKeys"],
    OBJECTS: [
        "ContinuationToken",
        "NextContinuationToken",
        "MaxKeys"],
    PARTS: [
        "PartNumberMarker",
        "NextPartNUmberMarker",
        "MaxParts"]
}


class S3Service(AwsService):
    def __init__(self, role_arn=None, session=None, tags_as_dict=True, as_named_tuple=False, service_retry_strategy=None):
        """
        :param role_arn: Optional (cross account) role to use to retrieve services
        :param session: Optional session to use to retrieve services
        :param tags_as_dict: Set to True true to convert resource tags to dictionaries
        :param as_named_tuple: Set to True to return resources as named tuples instead of a dictionary
        :param service_retry_strategy: service retry strategy for making boto api calls
        """

        AwsService.__init__(self, service_name='s3',
                            resource_names=RESOURCE_NAMES,
                            resources_with_tags=RESOURCES_WITH_TAGS,
                            role_arn=role_arn,
                            session=session,
                            tags_as_dict=tags_as_dict,
                            as_named_tuple=as_named_tuple,
                            custom_result_paths=CUSTOM_RESULT_PATHS,
                            service_retry_strategy=service_retry_strategy)

        self._continuation_data = CONTINUATION_DATA

    @staticmethod
    def is_regional():
        """
        Determine if a regional is a callable.

        Args:
        """
        return False

    def _map_describe_function_parameters(self, resources, args):
        """
        Maps the parameter names passed to the service class describe call to names used to make the call the the boto
        service client describe call
        :param resources: Name of the resource type
        :param args: parameters to be mapped
        :return: mapped parameters
        """
        translated_args = args.copy()
        for arg in translated_args:
            if arg == "MaxResults" and resources in self._continuation_data:
                del translated_args[arg]
                translated_args[self._continuation_data[resources][2]] = args[arg]

        return AwsService._map_describe_function_parameters(self, resources, translated_args)

    def _next_token_argument_name(self, resources):
        """
        Returns the name of the argument. argument.

        Args:
            self: (todo): write your description
            resources: (todo): write your description
        """
        if resources in self._continuation_data:
            return self._continuation_data[resources][0]
        else:
            return AwsService._next_token_argument_name(self, resources)

    def _next_token_result_name(self, resources):
        """
        Returns the next token name.

        Args:
            self: (todo): write your description
            resources: (todo): write your description
        """
        if resources in self._continuation_data:
            return self._continuation_data[resources][1]
        else:
            return AwsService._next_token_result_name(self, resources)

    def _transform_returned_resource(self, client, resource, use_cached_tags=False):
        """
        Transform a named resource

        Args:
            self: (todo): write your description
            client: (todo): write your description
            resource: (todo): write your description
            use_cached_tags: (bool): write your description
        """
        name = ""
        data = ""
        if self._resource_name == OBJECT_VERSIONS:
            if "Versions" in resource:
                name = "Versions"
                data = resource["Versions"]
            elif "DeleteMarkers" in resource:
                name = "DeleteMarkers"
                data = resource["DeleteMarkers"]
        else:
            name = self._resource_name
            data = resource

        if self._resource_name == OBJECTS:
            data["Bucket"] = self._describe_args.get("Bucket")

        if self._tags:
            resource["Tags"] = self._get_tags_for_resource(client, resource)

        if "ResponseMetadata" in resource:
            resource.pop("ResponseMetadata")

        if self._tags_as_dict:
            self._convert_tags_to_dictionaries(data)
        if self._describe_args.get("use_tuple", False):
            return as_namedtuple(name, data, deep=True, name_func=self._tuple_name_func, excludes=self._tuple_excludes)
        else:
            return resource

    def describe_resources_function_name(self, resource_name):
        """
        Returns the name of the boto client method call to retrieve the specified resource.
        :param resource_name:
        :return: Name of the boto3 client function to retrieve the specified resource type
        """
        s = AwsService.describe_resources_function_name(self, resource_name)
        if resource_name in [BUCKETS, MULTIPART_UPLOADS, OBJECT_VERSIONS, OBJECTS, PARTS]:
            s = s.replace("describe_", "list_")
            if resource_name == OBJECTS:
                s += "_v2"
        else:
            s = s.replace("describe_", "get_")

        return s

    @staticmethod
    def use_cached_tags(resource, tags_to_retrieve):
        """
        Use this function to cache a list of a resource.

        Args:
            resource: (todo): write your description
            tags_to_retrieve: (bool): write your description
        """
        return resource == BUCKETS and tags_to_retrieve > 10

    def _get_tags_for_resource(self, client, resource):
        """
        Returns a list of the tags for the given resource.

        Args:
            self: (todo): write your description
            client: (todo): write your description
            resource: (str): write your description
        """
        if self._use_cached_tags:
            arn = "arn:aws:s3:::{}".format(resource["Name"])
            return self.cached_tags(resource_name="").get(arn)
        else:
            return AwsService._get_tags_for_resource(self, client, resource)

    def _get_tag_resource(self):
        """
        Returns the name of the resource to retrieve the tags for the resource of type specified by resource name
        :return: Name of the resource that will be used to retrieve the tags
        """
        if self._resource_name == BUCKETS:
            return BUCKET_TAGGING

        if self._resource_name == OBJECTS:
            return OBJECT_TAGGING

        return None
