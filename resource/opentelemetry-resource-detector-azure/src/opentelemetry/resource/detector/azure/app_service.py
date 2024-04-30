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

from os import environ

from opentelemetry.sdk.resources import Resource, ResourceDetector
from opentelemetry.semconv.resource import (
    CloudPlatformValues,
    CloudProviderValues,
    ResourceAttributes,
)

from ._constants import (
    _APP_SERVICE_ATTRIBUTE_ENV_VARS,
    _WEBSITE_OWNER_NAME,
    _WEBSITE_RESOURCE_GROUP,
    _WEBSITE_SITE_NAME,
)


class AzureAppServiceResourceDetector(ResourceDetector):
    def detect(self) -> Resource:
        attributes = {}
        website_site_name = environ.get(_WEBSITE_SITE_NAME)
        if website_site_name:
            attributes[ResourceAttributes.SERVICE_NAME] = website_site_name
            attributes[ResourceAttributes.CLOUD_PROVIDER] = (
                CloudProviderValues.AZURE.value
            )
            attributes[ResourceAttributes.CLOUD_PLATFORM] = (
                CloudPlatformValues.AZURE_APP_SERVICE.value
            )

            azure_resource_uri = _get_azure_resource_uri(website_site_name)
            if azure_resource_uri:
                attributes[ResourceAttributes.CLOUD_RESOURCE_ID] = (
                    azure_resource_uri
                )
            for key, env_var in _APP_SERVICE_ATTRIBUTE_ENV_VARS.items():
                value = environ.get(env_var)
                if value:
                    attributes[key] = value

        return Resource(attributes)


def _get_azure_resource_uri(website_site_name):
    website_resource_group = environ.get(_WEBSITE_RESOURCE_GROUP)
    website_owner_name = environ.get(_WEBSITE_OWNER_NAME)

    subscription_id = website_owner_name
    if website_owner_name and "+" in website_owner_name:
        subscription_id = website_owner_name[0 : website_owner_name.index("+")]

    if not (website_resource_group and subscription_id):
        return None

    return f"/subscriptions/{subscription_id}/resourceGroups/{website_resource_group}/providers/Microsoft.Web/sites/{website_site_name}"
