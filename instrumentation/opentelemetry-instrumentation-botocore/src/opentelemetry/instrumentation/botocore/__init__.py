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

"""
Instrument `Botocore`_ to trace service requests.

There are two options for instrumenting code. The first option is to use the
``opentelemetry-instrument`` executable which will automatically
instrument your Botocore client. The second is to programmatically enable
instrumentation via the following code:

.. _Botocore: https://pypi.org/project/botocore/

Usage
-----

.. code:: python

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    import botocore


    # Instrument Botocore
    BotocoreInstrumentor().instrument()

    # This will create a span with Botocore-specific attributes
    session = botocore.session.get_session()
    session.set_credentials(
        access_key="access-key", secret_key="secret-key"
    )
    ec2 = self.session.create_client("ec2", region_name="us-west-2")
    ec2.describe_instances()

API
---

The `instrument` method accepts the following keyword args:

tracer_provider (TracerProvider) - an optional tracer provider
request_hook (Callable) - a function with extra user-defined logic to be performed before performing the request
this function signature is:  def request_hook(span: Span, service_name: str, operation_name: str, api_params: dict) -> None
response_hook (Callable) - a function with extra user-defined logic to be performed after performing the request
this function signature is:  def response_hook(span: Span, service_name: str, operation_name: str, result: dict) -> None

for example:

.. code: python

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    import botocore

    def request_hook(span, service_name, operation_name, api_params):
        # request hook logic

    def response_hook(span, service_name, operation_name, result):
        # response hook logic

    # Instrument Botocore with hooks
    BotocoreInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

    # This will create a span with Botocore-specific attributes, including custom attributes added from the hooks
    session = botocore.session.get_session()
    session.set_credentials(
        access_key="access-key", secret_key="secret-key"
    )
    ec2 = self.session.create_client("ec2", region_name="us-west-2")
    ec2.describe_instances()
"""

import logging
from typing import Any, Callable, Collection, Dict, Optional, Tuple

from botocore.client import BaseClient
from botocore.endpoint import Endpoint
from botocore.exceptions import ClientError
from wrapt import wrap_function_wrapper

from opentelemetry.instrumentation.botocore.extensions import _find_extension
from opentelemetry.instrumentation.botocore.extensions.types import (
    _AwsSdkCallContext,
)
from opentelemetry.instrumentation.botocore.package import _instruments
from opentelemetry.instrumentation.botocore.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import (
    is_instrumentation_enabled,
    suppress_http_instrumentation,
    unwrap,
)
from opentelemetry.propagators.aws.aws_xray_propagator import AwsXRayPropagator
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import get_tracer
from opentelemetry.trace.span import Span

logger = logging.getLogger(__name__)


class BotocoreInstrumentor(BaseInstrumentor):
    """An instrumentor for Botocore.

    See `BaseInstrumentor`
    """

    def __init__(self):
        super().__init__()
        self.request_hook = None
        self.response_hook = None
        self.propagator = AwsXRayPropagator()

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        # pylint: disable=attribute-defined-outside-init
        self._tracer = get_tracer(
            __name__,
            __version__,
            kwargs.get("tracer_provider"),
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )

        self.request_hook = kwargs.get("request_hook")
        self.response_hook = kwargs.get("response_hook")

        propagator = kwargs.get("propagator")
        if propagator is not None:
            self.propagator = propagator

        wrap_function_wrapper(
            "botocore.client",
            "BaseClient._make_api_call",
            self._patched_api_call,
        )

        wrap_function_wrapper(
            "botocore.endpoint",
            "Endpoint.prepare_request",
            self._patched_endpoint_prepare_request,
        )

    def _uninstrument(self, **kwargs):
        unwrap(BaseClient, "_make_api_call")
        unwrap(Endpoint, "prepare_request")

    # pylint: disable=unused-argument
    def _patched_endpoint_prepare_request(
        self, wrapped, instance, args, kwargs
    ):
        request = args[0]
        headers = request.headers

        # Only the x-ray header is propagated by AWS services. Using any
        # other propagator will lose the trace context.
        self.propagator.inject(headers)

        return wrapped(*args, **kwargs)

    # pylint: disable=too-many-branches
    def _patched_api_call(self, original_func, instance, args, kwargs):
        if not is_instrumentation_enabled():
            return original_func(*args, **kwargs)

        call_context = _determine_call_context(instance, args)
        if call_context is None:
            return original_func(*args, **kwargs)

        extension = _find_extension(call_context)
        if not extension.should_trace_service_call():
            return original_func(*args, **kwargs)

        attributes = {
            SpanAttributes.RPC_SYSTEM: "aws-api",
            SpanAttributes.RPC_SERVICE: call_context.service_id,
            SpanAttributes.RPC_METHOD: call_context.operation,
            # TODO: update when semantic conventions exist
            "aws.region": call_context.region,
        }

        _safe_invoke(extension.extract_attributes, attributes)
        end_span_on_exit = extension.should_end_span_on_exit()

        with self._tracer.start_as_current_span(
            call_context.span_name,
            kind=call_context.span_kind,
            attributes=attributes,
            # tracing streaming services require to close the span manually
            # at a later time after the stream has been consumed
            end_on_exit=end_span_on_exit,
        ) as span:
            _safe_invoke(extension.before_service_call, span)
            self._call_request_hook(span, call_context)

            try:
                with suppress_http_instrumentation():
                    result = None
                    try:
                        result = original_func(*args, **kwargs)
                    except ClientError as error:
                        result = getattr(error, "response", None)
                        _apply_response_attributes(span, result)
                        _safe_invoke(extension.on_error, span, error)
                        raise
                    _apply_response_attributes(span, result)
                    _safe_invoke(extension.on_success, span, result)
            finally:
                _safe_invoke(extension.after_service_call)
                self._call_response_hook(span, call_context, result)

            return result

    def _call_request_hook(self, span: Span, call_context: _AwsSdkCallContext):
        if not callable(self.request_hook):
            return
        self.request_hook(
            span,
            call_context.service,
            call_context.operation,
            call_context.params,
        )

    def _call_response_hook(
        self, span: Span, call_context: _AwsSdkCallContext, result
    ):
        if not callable(self.response_hook):
            return
        self.response_hook(
            span, call_context.service, call_context.operation, result
        )


def _apply_response_attributes(span: Span, result):
    if result is None or not span.is_recording():
        return

    metadata = result.get("ResponseMetadata")
    if metadata is None:
        return

    request_id = metadata.get("RequestId")
    if request_id is None:
        headers = metadata.get("HTTPHeaders")
        if headers is not None:
            request_id = (
                headers.get("x-amzn-RequestId")
                or headers.get("x-amz-request-id")
                or headers.get("x-amz-id-2")
            )
    if request_id:
        # TODO: update when semantic conventions exist
        span.set_attribute("aws.request_id", request_id)

    retry_attempts = metadata.get("RetryAttempts")
    if retry_attempts is not None:
        # TODO: update when semantic conventions exists
        span.set_attribute("retry_attempts", retry_attempts)

    status_code = metadata.get("HTTPStatusCode")
    if status_code is not None:
        span.set_attribute(SpanAttributes.HTTP_STATUS_CODE, status_code)


def _determine_call_context(
    client: BaseClient, args: Tuple[str, Dict[str, Any]]
) -> Optional[_AwsSdkCallContext]:
    try:
        call_context = _AwsSdkCallContext(client, args)

        logger.debug(
            "AWS SDK invocation: %s %s",
            call_context.service,
            call_context.operation,
        )

        return call_context
    except Exception as ex:  # pylint:disable=broad-except
        # this shouldn't happen actually unless internals of botocore changed and
        # extracting essential attributes ('service' and 'operation') failed.
        logger.error("Error when initializing call context", exc_info=ex)
        return None


def _safe_invoke(function: Callable, *args):
    function_name = "<unknown>"
    try:
        function_name = function.__name__
        function(*args)
    except Exception as ex:  # pylint:disable=broad-except
        logger.error(
            "Error when invoking function '%s'", function_name, exc_info=ex
        )
