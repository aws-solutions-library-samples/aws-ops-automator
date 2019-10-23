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
import http.client
import time

import boto3

import services.storagegateway_service
from helpers.timer import Timer
from services.ec2_service import Ec2Service
from services.storagegateway_service import StoragegatewayService


class StorageGateway(object):

    def __init__(self, region=None, session=None):

        self.region = region if region is not None else boto3.Session().region_name
        self.session = session if session is not None else boto3.Session(region_name=self.region)
        self.sgw_client = self.session.client("storagegateway", region_name=self.region)
        self.sgw_service = StoragegatewayService(session=self.session)

        self._latest_gateway_image = None

    @property
    def latest_gateway_image(self):
        if self._latest_gateway_image is None:
            ec2 = Ec2Service(session=self.session)
            sl = sorted([i for i in ec2.describe(services.ec2_service.IMAGES,
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
                                                              "Values": ["aws-storage-gateway-??.??.??.??"]
                                                          }
                                                 ],
                                                 ExecutableUsers=["all"])], key=lambda p: p["CreationDate"], reverse=True)
            assert (len(sl) > 0)
            self._latest_gateway_image = sl[0]
        return self._latest_gateway_image

    @classmethod
    def is_exception_with_code(cls, ex, code):
        return getattr(ex, "response", {}).get("Error", {}).get("Code", "") == code

    @classmethod
    def invalid_gateway(cls, ex):
        return cls.is_exception_with_code(ex, "InvalidGatewayRequestException")

    def build_storage_gateway(self, gateway_name, instance_public_address):

        def get_activation_key():
            key = None
            with Timer(timeout_seconds=1200, start=True) as activation_key_timer:
                while True:
                    # noinspection PyBroadException,PyPep8
                    try:
                        # start with initial wait as it will time for the instance to initialize
                        time.sleep(15)
                        # get key from redirection result
                        # see https://docs.aws.amazon.com/storagegateway/latest/userguide/get-activation-key.html
                        conn = http.client.HTTPConnection(instance_public_address)
                        conn.request("GET", "?activationRegion={}".format(self.region))
                        resp = conn.getresponse()
                        if resp.status == 302:
                            if resp.getheader("Location") is not None:
                                key = [q[1] for q in [h.split("=") for h in resp.getheader("Location").split('?')[1].split('&')] if
                                       q[0] == 'activationKey'][0]
                                break
                    except:
                        pass

                    if activation_key_timer.timeout:
                        break
                return key

        # get key to activate storage gateway on EC2 instance
        activation_key = get_activation_key()

        assert (activation_key is not None)

        # activate gateway with activation key
        self.sgw_client.activate_gateway(ActivationKey=activation_key,
                                         GatewayName=gateway_name,
                                         GatewayTimezone="GMT",
                                         GatewayRegion=self.region,
                                         GatewayType="CACHED")

        gw = self.get_gateway_by_name(gateway_name)
        assert (gw is not None)
        gw_arn = gw.get("GatewayARN")
        gw_id = gw.get("GatewayId")

        with Timer(timeout_seconds=1800, start=True) as activation_timer:
            while True:
                # noinspection PyBroadException,PyPep8
                try:
                    # wait until activated gateway becomes accessible
                    time.sleep(15)
                    # get the disks from the gateway
                    swg_disks = sorted(self.sgw_client.list_local_disks(GatewayARN=gw_arn)["Disks"],
                                       key=lambda d: d["DiskSizeInBytes"])
                    break
                except:
                    if activation_timer.timeout:
                        return None, None

        # create cache and upload buffer
        self.sgw_client.add_cache(GatewayARN=gw_arn, DiskIds=[swg_disks[0]["DiskId"]])
        self.sgw_client.add_upload_buffer(GatewayARN=gw_arn, DiskIds=[swg_disks[1]["DiskId"]])

        # create volume we can take a snapshot from
        network_interface = self.sgw_client.describe_gateway_information(
            GatewayARN=gw_arn)["GatewayNetworkInterfaces"][0]["Ipv4Address"]

        volume = self.sgw_client.create_cached_iscsi_volume(ClientToken="cached-test-volume", GatewayARN=gw_arn,
                                                            TargetName="cached-volume",
                                                            VolumeSizeInBytes=1024 * 1024 * 1024 * 150,
                                                            NetworkInterfaceId=network_interface)
        volume_arn = volume.get("VolumeARN")
        volume_id = volume_arn.split("/")[-1]
        volume_id = volume_id[0:4] + volume_id[4:].lower()

        # return both the gateway and volume arn
        return gw_arn, gw_id, volume_arn, volume_id

    def delete_gateway(self, gateway_arn):
        try:
            if gateway_arn is not None:
                self.sgw_client.delete_gateway(GatewayARN=gateway_arn)
        except Exception as ex:
            if not self.invalid_gateway(ex):
                raise ex

    def get_gateway_volumes_for_gateway(self, gateway_arn):
        try:
            volumes = self.sgw_service.describe(services.storagegateway_service.VOLUMES, GatewayARN=gateway_arn)
            return volumes
        except Exception as ex:
            if not self.invalid_gateway(ex):
                raise ex
            return []

    def get_tags(self, resource_arn):
        # noinspection PyBroadException,PyPep8
        try:
            tags = list(self.sgw_service.describe(services.storagegateway_service.TAGS_FOR_RESOURCE,
                                                  ResourceARN=resource_arn,
                                                  region=self.region))
            return {t["Key"]: t["Value"] for t in tags}
        except:
            return {}

    def add_tags(self, arn , tags):
        if len(tags) > 0:
            self.sgw_client.add_tags_to_resource(ResourceARN=arn, Tags=[{"Key": t, "Value": tags[t]} for t in tags])

    def get_gateway_by_name(self, gateway_name):
        gateways = [g for g in self.sgw_service.describe(services.storagegateway_service.GATEWAYS) if
                    g.get("GatewayName", "") == gateway_name]
        return gateways[0] if len(gateways) > 0 else None
