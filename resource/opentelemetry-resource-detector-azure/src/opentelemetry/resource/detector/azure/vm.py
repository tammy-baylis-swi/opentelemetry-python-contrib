# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from json import loads
from logging import getLogger
from urllib.error import URLError
from urllib.request import Request, urlopen

from opentelemetry.context import (
    _SUPPRESS_INSTRUMENTATION_KEY,
    attach,
    detach,
    set_value,
)
from opentelemetry.sdk.resources import Resource, ResourceDetector
from opentelemetry.semconv.resource import (
    CloudPlatformValues,
    CloudProviderValues,
    ResourceAttributes,
)

_AZURE_VM_METADATA_ENDPOINT = "http://169.254.169.254/metadata/instance/compute?api-version=2021-12-13&format=json"
_AZURE_VM_SCALE_SET_NAME_ATTRIBUTE = "azure.vm.scaleset.name"
_AZURE_VM_SKU_ATTRIBUTE = "azure.vm.sku"
_logger = getLogger(__name__)

EXPECTED_AZURE_AMS_ATTRIBUTES = [
    _AZURE_VM_SCALE_SET_NAME_ATTRIBUTE,
    _AZURE_VM_SKU_ATTRIBUTE,
    ResourceAttributes.CLOUD_PLATFORM,
    ResourceAttributes.CLOUD_PROVIDER,
    ResourceAttributes.CLOUD_REGION,
    ResourceAttributes.CLOUD_RESOURCE_ID,
    ResourceAttributes.HOST_ID,
    ResourceAttributes.HOST_NAME,
    ResourceAttributes.HOST_TYPE,
    ResourceAttributes.OS_TYPE,
    ResourceAttributes.OS_VERSION,
    ResourceAttributes.SERVICE_INSTANCE_ID,
]


class AzureVMResourceDetector(ResourceDetector):
    # pylint: disable=no-self-use
    def detect(self) -> "Resource":
        attributes = {}
        token = attach(set_value(_SUPPRESS_INSTRUMENTATION_KEY, True))
        metadata_json = _get_azure_vm_metadata()
        if not metadata_json:
            return Resource(attributes)
        for attribute_key in EXPECTED_AZURE_AMS_ATTRIBUTES:
            attributes[attribute_key] = _get_attribute_from_metadata(
                metadata_json, attribute_key
            )
        detach(token)
        return Resource(attributes)


def _get_azure_vm_metadata():
    request = Request(_AZURE_VM_METADATA_ENDPOINT)
    request.add_header("Metadata", "True")
    try:
        # VM metadata service should not take more than 200ms on success case
        with urlopen(request, timeout=0.2) as response:
            return loads(response.read())
    except URLError:
        # Not on Azure VM
        return None
    except Exception as e:  # pylint: disable=broad-except,invalid-name
        _logger.exception("Failed to receive Azure VM metadata: %s", e)
        return None


def _get_attribute_from_metadata(metadata_json, attribute_key):
    ams_value = ""
    if attribute_key == _AZURE_VM_SCALE_SET_NAME_ATTRIBUTE:
        ams_value = metadata_json["vmScaleSetName"]
    elif attribute_key == _AZURE_VM_SKU_ATTRIBUTE:
        ams_value = metadata_json["sku"]
    elif attribute_key == ResourceAttributes.CLOUD_PLATFORM:
        ams_value = CloudPlatformValues.AZURE_VM.value
    elif attribute_key == ResourceAttributes.CLOUD_PROVIDER:
        ams_value = CloudProviderValues.AZURE.value
    elif attribute_key == ResourceAttributes.CLOUD_REGION:
        ams_value = metadata_json["location"]
    elif attribute_key == ResourceAttributes.CLOUD_RESOURCE_ID:
        ams_value = metadata_json["resourceId"]
    elif attribute_key in (
        ResourceAttributes.HOST_ID,
        ResourceAttributes.SERVICE_INSTANCE_ID,
    ):
        ams_value = metadata_json["vmId"]
    elif attribute_key == ResourceAttributes.HOST_NAME:
        ams_value = metadata_json["name"]
    elif attribute_key == ResourceAttributes.HOST_TYPE:
        ams_value = metadata_json["vmSize"]
    elif attribute_key == ResourceAttributes.OS_TYPE:
        ams_value = metadata_json["osType"]
    elif attribute_key == ResourceAttributes.OS_VERSION:
        ams_value = metadata_json["version"]
    return ams_value
