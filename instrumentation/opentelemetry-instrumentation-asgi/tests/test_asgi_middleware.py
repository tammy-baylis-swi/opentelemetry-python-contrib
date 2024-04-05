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

# pylint: disable=too-many-lines

import asyncio
import sys
import time
import unittest
from timeit import default_timer
from unittest import mock

import opentelemetry.instrumentation.asgi as otel_asgi
from opentelemetry import trace as trace_api
from opentelemetry.instrumentation.propagators import (
    TraceResponsePropagator,
    get_global_response_propagator,
    set_global_response_propagator,
)
from opentelemetry.sdk import resources
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.test.asgitestutil import (
    AsgiTestBase,
    setup_testing_defaults,
)
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind, format_span_id, format_trace_id
from opentelemetry.util.http import (
    _active_requests_count_attrs,
    _duration_attrs,
)

_expected_metric_names = [
    "http.server.active_requests",
    "http.server.duration",
    "http.server.response.size",
    "http.server.request.size",
]
_recommended_attrs = {
    "http.server.active_requests": _active_requests_count_attrs,
    "http.server.duration": _duration_attrs,
    "http.server.response.size": _duration_attrs,
    "http.server.request.size": _duration_attrs,
}

_SIMULATED_BACKGROUND_TASK_EXECUTION_TIME_S = 0.01


async def http_app(scope, receive, send):
    message = await receive()
    scope["headers"] = [(b"content-length", b"128")]
    assert scope["type"] == "http"
    if message.get("type") == "http.request":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"Content-Type", b"text/plain"],
                    [b"content-length", b"1024"],
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"*"})


async def websocket_app(scope, receive, send):
    assert scope["type"] == "websocket"
    while True:
        message = await receive()
        if message.get("type") == "websocket.connect":
            await send({"type": "websocket.accept"})

        if message.get("type") == "websocket.receive":
            if message.get("text") == "ping":
                await send({"type": "websocket.send", "text": "pong"})

        if message.get("type") == "websocket.disconnect":
            break


async def simple_asgi(scope, receive, send):
    assert isinstance(scope, dict)
    if scope["type"] == "http":
        await http_app(scope, receive, send)
    elif scope["type"] == "websocket":
        await websocket_app(scope, receive, send)


async def long_response_asgi(scope, receive, send):
    assert isinstance(scope, dict)
    assert scope["type"] == "http"
    message = await receive()
    scope["headers"] = [(b"content-length", b"128")]
    assert scope["type"] == "http"
    if message.get("type") == "http.request":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"Content-Type", b"text/plain"],
                    [b"content-length", b"1024"],
                ],
            }
        )
        await send(
            {"type": "http.response.body", "body": b"*", "more_body": True}
        )
        await send(
            {"type": "http.response.body", "body": b"*", "more_body": True}
        )
        await send(
            {"type": "http.response.body", "body": b"*", "more_body": True}
        )
        await send(
            {"type": "http.response.body", "body": b"*", "more_body": False}
        )


async def background_execution_asgi(scope, receive, send):
    assert isinstance(scope, dict)
    assert scope["type"] == "http"
    message = await receive()
    scope["headers"] = [(b"content-length", b"128")]
    assert scope["type"] == "http"
    if message.get("type") == "http.request":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"Content-Type", b"text/plain"],
                    [b"content-length", b"1024"],
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": b"*",
            }
        )
        time.sleep(_SIMULATED_BACKGROUND_TASK_EXECUTION_TIME_S)


async def background_execution_trailers_asgi(scope, receive, send):
    assert isinstance(scope, dict)
    assert scope["type"] == "http"
    message = await receive()
    scope["headers"] = [(b"content-length", b"128")]
    assert scope["type"] == "http"
    if message.get("type") == "http.request":
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"Content-Type", b"text/plain"],
                    [b"content-length", b"1024"],
                ],
                "trailers": True,
            }
        )
        await send(
            {"type": "http.response.body", "body": b"*", "more_body": True}
        )
        await send(
            {"type": "http.response.body", "body": b"*", "more_body": False}
        )
        await send(
            {
                "type": "http.response.trailers",
                "headers": [
                    [b"trailer", b"test-trailer"],
                ],
                "more_trailers": True,
            }
        )
        await send(
            {
                "type": "http.response.trailers",
                "headers": [
                    [b"trailer", b"second-test-trailer"],
                ],
                "more_trailers": False,
            }
        )
        time.sleep(_SIMULATED_BACKGROUND_TASK_EXECUTION_TIME_S)


async def error_asgi(scope, receive, send):
    assert isinstance(scope, dict)
    assert scope["type"] == "http"
    message = await receive()
    scope["headers"] = [(b"content-length", b"128")]
    if message.get("type") == "http.request":
        try:
            raise ValueError
        except ValueError:
            scope["hack_exc_info"] = sys.exc_info()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    [b"Content-Type", b"text/plain"],
                    [b"content-length", b"1024"],
                ],
            }
        )
        await send({"type": "http.response.body", "body": b"*"})


# pylint: disable=too-many-public-methods
class TestAsgiApplication(AsgiTestBase):
    def validate_outputs(self, outputs, error=None, modifiers=None):
        # Ensure modifiers is a list
        modifiers = modifiers or []
        # Check for expected outputs
        response_start = outputs[0]
        response_final_body = [
            output
            for output in outputs
            if output["type"] == "http.response.body"
        ][-1]

        self.assertEqual(response_start["type"], "http.response.start")
        self.assertEqual(response_final_body["type"], "http.response.body")
        self.assertEqual(response_final_body.get("more_body", False), False)

        # Check http response body
        self.assertEqual(response_final_body["body"], b"*")

        # Check http response start
        self.assertEqual(response_start["status"], 200)
        self.assertEqual(
            response_start["headers"],
            [[b"Content-Type", b"text/plain"], [b"content-length", b"1024"]],
        )

        exc_info = self.scope.get("hack_exc_info")
        if error:
            self.assertIs(exc_info[0], error)
            self.assertIsInstance(exc_info[1], error)
            self.assertIsNotNone(exc_info[2])
        else:
            self.assertIsNone(exc_info)

        # Check spans
        span_list = self.memory_exporter.get_finished_spans()
        expected = [
            {
                "name": "GET / http receive",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "http.request"},
            },
            {
                "name": "GET / http send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {
                    SpanAttributes.HTTP_STATUS_CODE: 200,
                    "asgi.event.type": "http.response.start",
                },
            },
            {
                "name": "GET / http send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "http.response.body"},
            },
            {
                "name": "GET /",
                "kind": trace_api.SpanKind.SERVER,
                "attributes": {
                    SpanAttributes.HTTP_METHOD: "GET",
                    SpanAttributes.HTTP_SCHEME: "http",
                    SpanAttributes.NET_HOST_PORT: 80,
                    SpanAttributes.HTTP_HOST: "127.0.0.1",
                    SpanAttributes.HTTP_FLAVOR: "1.0",
                    SpanAttributes.HTTP_TARGET: "/",
                    SpanAttributes.HTTP_URL: "http://127.0.0.1/",
                    SpanAttributes.NET_PEER_IP: "127.0.0.1",
                    SpanAttributes.NET_PEER_PORT: 32767,
                    SpanAttributes.HTTP_STATUS_CODE: 200,
                },
            },
        ]
        # Run our expected modifiers
        for modifier in modifiers:
            expected = modifier(expected)
        # Check that output matches
        self.assertEqual(len(span_list), len(expected))
        for span, expected in zip(span_list, expected):
            self.assertEqual(span.name, expected["name"])
            self.assertEqual(span.kind, expected["kind"])
            self.assertDictEqual(dict(span.attributes), expected["attributes"])

    def test_basic_asgi_call(self):
        """Test that spans are emitted as expected."""
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs)

    def test_asgi_not_recording(self):
        mock_tracer = mock.Mock()
        mock_span = mock.Mock()
        mock_span.is_recording.return_value = False
        mock_tracer.start_as_current_span.return_value = mock_span
        mock_tracer.start_as_current_span.return_value.__enter__ = mock_span
        mock_tracer.start_as_current_span.return_value.__exit__ = mock_span
        with mock.patch("opentelemetry.trace.get_tracer") as tracer:
            tracer.return_value = mock_tracer
            app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
            self.seed_app(app)
            self.send_default_request()
            self.assertFalse(mock_span.is_recording())
            self.assertTrue(mock_span.is_recording.called)
            self.assertFalse(mock_span.set_attribute.called)
            self.assertFalse(mock_span.set_status.called)

    def test_asgi_exc_info(self):
        """Test that exception information is emitted as expected."""
        app = otel_asgi.OpenTelemetryMiddleware(error_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs, error=ValueError)

    def test_long_response(self):
        """Test that the server span is ended on the final response body message.

        If the server span is ended early then this test will fail due
        to discrepancies in the expected list of spans and the emitted list of spans.
        """
        app = otel_asgi.OpenTelemetryMiddleware(long_response_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()

        def add_more_body_spans(expected: list):
            more_body_span = {
                "name": "GET / http send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "http.response.body"},
            }
            extra_spans = [more_body_span] * 3
            expected[2:2] = extra_spans
            return expected

        self.validate_outputs(outputs, modifiers=[add_more_body_spans])

    def test_background_execution(self):
        """Test that the server span is ended BEFORE the background task is finished."""
        app = otel_asgi.OpenTelemetryMiddleware(background_execution_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs)
        span_list = self.memory_exporter.get_finished_spans()
        server_span = span_list[-1]
        assert server_span.kind == SpanKind.SERVER
        span_duration_nanos = server_span.end_time - server_span.start_time
        self.assertLessEqual(
            span_duration_nanos,
            _SIMULATED_BACKGROUND_TASK_EXECUTION_TIME_S * 10**9,
        )

    def test_trailers(self):
        """Test that trailers are emitted as expected and that the server span is ended
        BEFORE the background task is finished."""
        app = otel_asgi.OpenTelemetryMiddleware(
            background_execution_trailers_asgi
        )
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()

        def add_body_and_trailer_span(expected: list):
            body_span = {
                "name": "GET / http send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "http.response.body"},
            }
            trailer_span = {
                "name": "GET / http send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "http.response.trailers"},
            }
            expected[2:2] = [body_span]
            expected[4:4] = [trailer_span] * 2
            return expected

        self.validate_outputs(outputs, modifiers=[add_body_and_trailer_span])
        span_list = self.memory_exporter.get_finished_spans()
        server_span = span_list[-1]
        assert server_span.kind == SpanKind.SERVER
        span_duration_nanos = server_span.end_time - server_span.start_time
        self.assertLessEqual(
            span_duration_nanos,
            _SIMULATED_BACKGROUND_TASK_EXECUTION_TIME_S * 10**9,
        )

    def test_override_span_name(self):
        """Test that default span_names can be overwritten by our callback function."""
        span_name = "Dymaxion"

        def get_predefined_span_details(_):
            return span_name, {}

        def update_expected_span_name(expected):
            for entry in expected:
                if entry["kind"] == trace_api.SpanKind.SERVER:
                    entry["name"] = span_name
                else:
                    entry["name"] = " ".join(
                        [span_name] + entry["name"].split(" ")[2:]
                    )
            return expected

        app = otel_asgi.OpenTelemetryMiddleware(
            simple_asgi, default_span_details=get_predefined_span_details
        )
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs, modifiers=[update_expected_span_name])

    def test_custom_tracer_provider_otel_asgi(self):
        resource = resources.Resource.create({"service-test-key": "value"})
        result = TestBase.create_tracer_provider(resource=resource)
        tracer_provider, exporter = result

        app = otel_asgi.OpenTelemetryMiddleware(
            simple_asgi, tracer_provider=tracer_provider
        )
        self.seed_app(app)
        self.send_default_request()
        span_list = exporter.get_finished_spans()
        for span in span_list:
            self.assertEqual(
                span.resource.attributes["service-test-key"], "value"
            )

    def test_no_op_tracer_provider_otel_asgi(self):
        app = otel_asgi.OpenTelemetryMiddleware(
            simple_asgi, tracer_provider=trace_api.NoOpTracerProvider()
        )
        self.seed_app(app)
        self.send_default_request()

        response_start, response_body, *_ = self.get_all_output()
        self.assertEqual(response_body["body"], b"*")
        self.assertEqual(response_start["status"], 200)

        span_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(span_list), 0)

    def test_behavior_with_scope_server_as_none(self):
        """Test that middleware is ok when server is none in scope."""

        def update_expected_server(expected):
            expected[3]["attributes"].update(
                {
                    SpanAttributes.HTTP_HOST: "0.0.0.0",
                    SpanAttributes.NET_HOST_PORT: 80,
                    SpanAttributes.HTTP_URL: "http://0.0.0.0/",
                }
            )
            return expected

        self.scope["server"] = None
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs, modifiers=[update_expected_server])

    def test_host_header(self):
        """Test that host header is converted to http.server_name."""
        hostname = b"server_name_1"

        def update_expected_server(expected):
            expected[3]["attributes"].update(
                {SpanAttributes.HTTP_SERVER_NAME: hostname.decode("utf8")}
            )
            return expected

        self.scope["headers"].append([b"host", hostname])
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs, modifiers=[update_expected_server])

    def test_user_agent(self):
        """Test that host header is converted to http.server_name."""
        user_agent = b"test-agent"

        def update_expected_user_agent(expected):
            expected[3]["attributes"].update(
                {SpanAttributes.HTTP_USER_AGENT: user_agent.decode("utf8")}
            )
            return expected

        self.scope["headers"].append([b"user-agent", user_agent])
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(outputs, modifiers=[update_expected_user_agent])

    def test_traceresponse_header(self):
        """Test a traceresponse header is sent when a global propagator is set."""

        orig = get_global_response_propagator()
        set_global_response_propagator(TraceResponsePropagator())

        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()

        span = self.memory_exporter.get_finished_spans()[-1]
        self.assertEqual(trace_api.SpanKind.SERVER, span.kind)

        response_start, response_body, *_ = self.get_all_output()
        self.assertEqual(response_body["body"], b"*")
        self.assertEqual(response_start["status"], 200)

        trace_id = format_trace_id(span.get_span_context().trace_id)
        span_id = format_span_id(span.get_span_context().span_id)
        traceresponse = f"00-{trace_id}-{span_id}-01"

        self.assertListEqual(
            response_start["headers"],
            [
                [b"Content-Type", b"text/plain"],
                [b"content-length", b"1024"],
                [b"traceresponse", f"{traceresponse}".encode()],
                [b"access-control-expose-headers", b"traceresponse"],
            ],
        )

        set_global_response_propagator(orig)

    def test_websocket(self):
        self.scope = {
            "type": "websocket",
            "http_version": "1.1",
            "scheme": "ws",
            "path": "/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 32767),
            "server": ("127.0.0.1", 80),
        }
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_input({"type": "websocket.connect"})
        self.send_input({"type": "websocket.receive", "text": "ping"})
        self.send_input({"type": "websocket.disconnect"})
        self.get_all_output()
        span_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(span_list), 6)
        expected = [
            {
                "name": "/ websocket receive",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "websocket.connect"},
            },
            {
                "name": "/ websocket send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "websocket.accept"},
            },
            {
                "name": "/ websocket receive",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {
                    "asgi.event.type": "websocket.receive",
                    SpanAttributes.HTTP_STATUS_CODE: 200,
                },
            },
            {
                "name": "/ websocket send",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {
                    "asgi.event.type": "websocket.send",
                    SpanAttributes.HTTP_STATUS_CODE: 200,
                },
            },
            {
                "name": "/ websocket receive",
                "kind": trace_api.SpanKind.INTERNAL,
                "attributes": {"asgi.event.type": "websocket.disconnect"},
            },
            {
                "name": "/",
                "kind": trace_api.SpanKind.SERVER,
                "attributes": {
                    SpanAttributes.HTTP_SCHEME: self.scope["scheme"],
                    SpanAttributes.NET_HOST_PORT: self.scope["server"][1],
                    SpanAttributes.HTTP_HOST: self.scope["server"][0],
                    SpanAttributes.HTTP_FLAVOR: self.scope["http_version"],
                    SpanAttributes.HTTP_TARGET: self.scope["path"],
                    SpanAttributes.HTTP_URL: f'{self.scope["scheme"]}://{self.scope["server"][0]}{self.scope["path"]}',
                    SpanAttributes.NET_PEER_IP: self.scope["client"][0],
                    SpanAttributes.NET_PEER_PORT: self.scope["client"][1],
                    SpanAttributes.HTTP_STATUS_CODE: 200,
                },
            },
        ]
        for span, expected in zip(span_list, expected):
            self.assertEqual(span.name, expected["name"])
            self.assertEqual(span.kind, expected["kind"])
            self.assertDictEqual(dict(span.attributes), expected["attributes"])

    def test_websocket_traceresponse_header(self):
        """Test a traceresponse header is set for websocket messages"""

        orig = get_global_response_propagator()
        set_global_response_propagator(TraceResponsePropagator())

        self.scope = {
            "type": "websocket",
            "http_version": "1.1",
            "scheme": "ws",
            "path": "/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 32767),
            "server": ("127.0.0.1", 80),
        }
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_input({"type": "websocket.connect"})
        self.send_input({"type": "websocket.receive", "text": "ping"})
        self.send_input({"type": "websocket.disconnect"})
        _, socket_send, *_ = self.get_all_output()

        span = self.memory_exporter.get_finished_spans()[-1]
        self.assertEqual(trace_api.SpanKind.SERVER, span.kind)

        trace_id = format_trace_id(span.get_span_context().trace_id)
        span_id = format_span_id(span.get_span_context().span_id)
        traceresponse = f"00-{trace_id}-{span_id}-01"

        self.assertListEqual(
            socket_send["headers"],
            [
                [b"traceresponse", f"{traceresponse}".encode()],
                [b"access-control-expose-headers", b"traceresponse"],
            ],
        )

        set_global_response_propagator(orig)

    def test_lifespan(self):
        self.scope["type"] = "lifespan"
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()
        span_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(span_list), 0)

    def test_hooks(self):
        def server_request_hook(span, scope):
            span.update_name("name from server hook")

        def client_request_hook(recieve_span, request):
            recieve_span.update_name("name from client request hook")

        def client_response_hook(send_span, response):
            send_span.set_attribute("attr-from-hook", "value")

        def update_expected_hook_results(expected):
            for entry in expected:
                if entry["kind"] == trace_api.SpanKind.SERVER:
                    entry["name"] = "name from server hook"
                elif entry["name"] == "GET / http receive":
                    entry["name"] = "name from client request hook"
                elif entry["name"] == "GET / http send":
                    entry["attributes"].update({"attr-from-hook": "value"})
            return expected

        app = otel_asgi.OpenTelemetryMiddleware(
            simple_asgi,
            server_request_hook=server_request_hook,
            client_request_hook=client_request_hook,
            client_response_hook=client_response_hook,
        )
        self.seed_app(app)
        self.send_default_request()
        outputs = self.get_all_output()
        self.validate_outputs(
            outputs, modifiers=[update_expected_hook_results]
        )

    def test_asgi_metrics(self):
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_default_request()
        self.seed_app(app)
        self.send_default_request()
        self.seed_app(app)
        self.send_default_request()
        metrics_list = self.memory_metrics_reader.get_metrics_data()
        number_data_point_seen = False
        histogram_data_point_seen = False
        self.assertTrue(len(metrics_list.resource_metrics) != 0)
        for resource_metric in metrics_list.resource_metrics:
            self.assertTrue(len(resource_metric.scope_metrics) != 0)
            for scope_metric in resource_metric.scope_metrics:
                self.assertTrue(len(scope_metric.metrics) != 0)
                for metric in scope_metric.metrics:
                    self.assertIn(metric.name, _expected_metric_names)
                    data_points = list(metric.data.data_points)
                    self.assertEqual(len(data_points), 1)
                    for point in data_points:
                        if isinstance(point, HistogramDataPoint):
                            self.assertEqual(point.count, 3)
                            histogram_data_point_seen = True
                        if isinstance(point, NumberDataPoint):
                            number_data_point_seen = True
                        for attr in point.attributes:
                            self.assertIn(
                                attr, _recommended_attrs[metric.name]
                            )
        self.assertTrue(number_data_point_seen and histogram_data_point_seen)

    def test_basic_metric_success(self):
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        start = default_timer()
        self.send_default_request()
        duration = max(round((default_timer() - start) * 1000), 0)
        expected_duration_attributes = {
            "http.method": "GET",
            "http.host": "127.0.0.1",
            "http.scheme": "http",
            "http.flavor": "1.0",
            "net.host.port": 80,
            "http.status_code": 200,
        }
        expected_requests_count_attributes = {
            "http.method": "GET",
            "http.host": "127.0.0.1",
            "http.scheme": "http",
            "http.flavor": "1.0",
        }
        metrics_list = self.memory_metrics_reader.get_metrics_data()
        # pylint: disable=too-many-nested-blocks
        for resource_metric in metrics_list.resource_metrics:
            for scope_metrics in resource_metric.scope_metrics:
                for metric in scope_metrics.metrics:
                    for point in list(metric.data.data_points):
                        if isinstance(point, HistogramDataPoint):
                            self.assertDictEqual(
                                expected_duration_attributes,
                                dict(point.attributes),
                            )
                            self.assertEqual(point.count, 1)
                            if metric.name == "http.server.duration":
                                self.assertAlmostEqual(
                                    duration, point.sum, delta=5
                                )
                            elif metric.name == "http.server.response.size":
                                self.assertEqual(1024, point.sum)
                            elif metric.name == "http.server.request.size":
                                self.assertEqual(128, point.sum)
                        elif isinstance(point, NumberDataPoint):
                            self.assertDictEqual(
                                expected_requests_count_attributes,
                                dict(point.attributes),
                            )
                            self.assertEqual(point.value, 0)

    def test_metric_target_attribute(self):
        expected_target = "/api/user/{id}"

        class TestRoute:
            path_format = expected_target

        async def target_asgi(scope, receive, send):
            assert isinstance(scope, dict)
            if scope["type"] == "http":
                await http_app(scope, receive, send)
                scope["route"] = TestRoute()
            else:
                raise ValueError("websockets not supported")

        app = otel_asgi.OpenTelemetryMiddleware(target_asgi)
        self.seed_app(app)
        self.send_default_request()
        metrics_list = self.memory_metrics_reader.get_metrics_data()
        assertions = 0
        for resource_metric in metrics_list.resource_metrics:
            for scope_metrics in resource_metric.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name == "http.server.active_requests":
                        continue
                    for point in metric.data.data_points:
                        if isinstance(point, HistogramDataPoint):
                            self.assertEqual(
                                point.attributes["http.target"],
                                expected_target,
                            )
                            assertions += 1
        self.assertEqual(assertions, 3)

    def test_no_metric_for_websockets(self):
        self.scope = {
            "type": "websocket",
            "http_version": "1.1",
            "scheme": "ws",
            "path": "/",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 32767),
            "server": ("127.0.0.1", 80),
        }
        app = otel_asgi.OpenTelemetryMiddleware(simple_asgi)
        self.seed_app(app)
        self.send_input({"type": "websocket.connect"})
        self.send_input({"type": "websocket.receive", "text": "ping"})
        self.send_input({"type": "websocket.disconnect"})
        self.get_all_output()
        self.assertIsNone(self.memory_metrics_reader.get_metrics_data())


class TestAsgiAttributes(unittest.TestCase):
    def setUp(self):
        self.scope = {}
        setup_testing_defaults(self.scope)
        self.span = mock.create_autospec(trace_api.Span, spec_set=True)

    def test_request_attributes(self):
        self.scope["query_string"] = b"foo=bar"
        headers = []
        headers.append((b"host", b"test"))
        self.scope["headers"] = headers

        attrs = otel_asgi.collect_request_attributes(self.scope)

        self.assertDictEqual(
            attrs,
            {
                SpanAttributes.HTTP_METHOD: "GET",
                SpanAttributes.HTTP_HOST: "127.0.0.1",
                SpanAttributes.HTTP_TARGET: "/",
                SpanAttributes.HTTP_URL: "http://127.0.0.1/?foo=bar",
                SpanAttributes.NET_HOST_PORT: 80,
                SpanAttributes.HTTP_SCHEME: "http",
                SpanAttributes.HTTP_SERVER_NAME: "test",
                SpanAttributes.HTTP_FLAVOR: "1.0",
                SpanAttributes.NET_PEER_IP: "127.0.0.1",
                SpanAttributes.NET_PEER_PORT: 32767,
            },
        )

    def test_query_string(self):
        self.scope["query_string"] = b"foo=bar"
        attrs = otel_asgi.collect_request_attributes(self.scope)
        self.assertEqual(
            attrs[SpanAttributes.HTTP_URL], "http://127.0.0.1/?foo=bar"
        )

    def test_query_string_percent_bytes(self):
        self.scope["query_string"] = b"foo%3Dbar"
        attrs = otel_asgi.collect_request_attributes(self.scope)
        self.assertEqual(
            attrs[SpanAttributes.HTTP_URL], "http://127.0.0.1/?foo=bar"
        )

    def test_query_string_percent_str(self):
        self.scope["query_string"] = "foo%3Dbar"
        attrs = otel_asgi.collect_request_attributes(self.scope)
        self.assertEqual(
            attrs[SpanAttributes.HTTP_URL], "http://127.0.0.1/?foo=bar"
        )

    def test_response_attributes(self):
        otel_asgi.set_status_code(self.span, 404)
        expected = (mock.call(SpanAttributes.HTTP_STATUS_CODE, 404),)
        self.assertEqual(self.span.set_attribute.call_count, 1)
        self.assertEqual(self.span.set_attribute.call_count, 1)
        self.span.set_attribute.assert_has_calls(expected, any_order=True)

    def test_response_attributes_invalid_status_code(self):
        otel_asgi.set_status_code(self.span, "Invalid Status Code")
        self.assertEqual(self.span.set_status.call_count, 1)

    def test_credential_removal(self):
        self.scope["server"] = ("username:password@mock", 80)
        self.scope["path"] = "/status/200"
        attrs = otel_asgi.collect_request_attributes(self.scope)
        self.assertEqual(
            attrs[SpanAttributes.HTTP_URL], "http://mock/status/200"
        )

    def test_collect_target_attribute_missing(self):
        self.assertIsNone(otel_asgi._collect_target_attribute(self.scope))

    def test_collect_target_attribute_fastapi(self):
        class TestRoute:
            path_format = "/api/users/{user_id}"

        self.scope["route"] = TestRoute()
        self.assertEqual(
            otel_asgi._collect_target_attribute(self.scope),
            "/api/users/{user_id}",
        )

    def test_collect_target_attribute_fastapi_mounted(self):
        class TestRoute:
            path_format = "/users/{user_id}"

        self.scope["route"] = TestRoute()
        self.scope["root_path"] = "/api/v2"
        self.assertEqual(
            otel_asgi._collect_target_attribute(self.scope),
            "/api/v2/users/{user_id}",
        )

    def test_collect_target_attribute_fastapi_starlette_invalid(self):
        self.scope["route"] = object()
        self.assertIsNone(
            otel_asgi._collect_target_attribute(self.scope),
            "HTTP_TARGET values is not None",
        )


class TestWrappedApplication(AsgiTestBase):
    def test_mark_span_internal_in_presence_of_span_from_other_framework(self):
        tracer_provider, exporter = TestBase.create_tracer_provider()
        tracer = tracer_provider.get_tracer(__name__)
        app = otel_asgi.OpenTelemetryMiddleware(
            simple_asgi, tracer_provider=tracer_provider
        )

        # Wrapping the otel intercepted app with server span
        async def wrapped_app(scope, receive, send):
            with tracer.start_as_current_span(
                "test", kind=SpanKind.SERVER
            ) as _:
                await app(scope, receive, send)

        self.seed_app(wrapped_app)
        self.send_default_request()
        span_list = exporter.get_finished_spans()

        self.assertEqual(SpanKind.INTERNAL, span_list[0].kind)
        self.assertEqual(SpanKind.INTERNAL, span_list[1].kind)
        self.assertEqual(SpanKind.INTERNAL, span_list[2].kind)
        self.assertEqual(trace_api.SpanKind.INTERNAL, span_list[3].kind)

        # SERVER "test"
        self.assertEqual(SpanKind.SERVER, span_list[4].kind)

        # internal span should be child of the test span we have provided
        self.assertEqual(
            span_list[4].context.span_id, span_list[3].parent.span_id
        )


class TestAsgiApplicationRaisingError(AsgiTestBase):
    def tearDown(self):
        pass

    def test_asgi_issue_1883(self):
        """
        Test that exception UnboundLocalError local variable 'start' referenced before assignment is not raised
        See https://github.com/open-telemetry/opentelemetry-python-contrib/issues/1883
        """

        async def bad_app(_scope, _receive, _send):
            raise ValueError("whatever")

        app = otel_asgi.OpenTelemetryMiddleware(bad_app)
        self.seed_app(app)
        self.send_default_request()
        try:
            asyncio.get_event_loop().run_until_complete(
                self.communicator.stop()
            )
        except ValueError as exc_info:
            self.assertEqual(exc_info.args[0], "whatever")
        except Exception as exc_info:  # pylint: disable=W0703
            self.fail(
                "expecting ValueError('whatever'), received instead: "
                + str(exc_info)
            )
        else:
            self.fail("expecting ValueError('whatever')")


if __name__ == "__main__":
    unittest.main()
