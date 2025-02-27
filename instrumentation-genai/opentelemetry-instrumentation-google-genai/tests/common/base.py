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

import os
import unittest

import google.genai

from .instrumentation_context import InstrumentationContext
from .otel_mocker import OTelMocker
from .requests_mocker import RequestsMocker


class _FakeCredentials(google.auth.credentials.AnonymousCredentials):
    def refresh(self, request):
        pass


class TestCase(unittest.TestCase):
    def setUp(self):
        self._otel = OTelMocker()
        self._otel.install()
        self._requests = RequestsMocker()
        self._requests.install()
        self._instrumentation_context = None
        self._api_key = "test-api-key"
        self._project = "test-project"
        self._location = "test-location"
        self._client = None
        self._uses_vertex = False
        self._credentials = _FakeCredentials()

    def _lazy_init(self):
        self._instrumentation_context = InstrumentationContext()
        self._instrumentation_context.install()

    @property
    def client(self):
        if self._client is None:
            self._client = self._create_client()
        return self._client

    @property
    def requests(self):
        return self._requests

    @property
    def otel(self):
        return self._otel

    def set_use_vertex(self, use_vertex):
        self._uses_vertex = use_vertex

    def _create_client(self):
        self._lazy_init()
        if self._uses_vertex:
            os.environ["GOOGLE_API_KEY"] = self._api_key
            return google.genai.Client(
                vertexai=True,
                project=self._project,
                location=self._location,
                credentials=self._credentials,
            )
        return google.genai.Client(api_key=self._api_key)

    def tearDown(self):
        if self._instrumentation_context is not None:
            self._instrumentation_context.uninstall()
        self._requests.uninstall()
        self._otel.uninstall()
