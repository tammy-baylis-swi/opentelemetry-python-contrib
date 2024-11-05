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


import logging
import re
from unittest import mock

from opentelemetry import context
from opentelemetry import trace as trace_api
from opentelemetry.instrumentation import dbapi
from opentelemetry.sdk import resources
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.test.test_base import TestBase


# pylint: disable=too-many-public-methods
class TestDBApiIntegration(TestBase):
    def setUp(self):
        super().setUp()
        self.tracer = self.tracer_provider.get_tracer(__name__)

    def test_span_succeeded(self):
        connection_props = {
            "database": "testdatabase",
            "server_host": "testhost",
            "server_port": 123,
            "user": "testuser",
        }
        connection_attributes = {
            "database": "database",
            "port": "server_port",
            "host": "server_host",
            "user": "user",
        }
        db_integration = dbapi.DatabaseApiIntegration(
            "testname", "testcomponent", connection_attributes
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, connection_props
        )
        cursor = mock_connection.cursor()
        cursor.execute("Test query", ("param1Value", False))
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertEqual(span.name, "Test")
        self.assertIs(span.kind, trace_api.SpanKind.CLIENT)

        self.assertEqual(
            span.attributes[SpanAttributes.DB_SYSTEM], "testcomponent"
        )
        self.assertEqual(
            span.attributes[SpanAttributes.DB_NAME], "testdatabase"
        )
        self.assertEqual(
            span.attributes[SpanAttributes.DB_STATEMENT], "Test query"
        )
        self.assertFalse("db.statement.parameters" in span.attributes)
        self.assertEqual(span.attributes[SpanAttributes.DB_USER], "testuser")
        self.assertEqual(
            span.attributes[SpanAttributes.NET_PEER_NAME], "testhost"
        )
        self.assertEqual(span.attributes[SpanAttributes.NET_PEER_PORT], 123)
        self.assertIs(span.status.status_code, trace_api.StatusCode.UNSET)

    def test_span_name(self):
        db_integration = dbapi.DatabaseApiIntegration(
            "testname", "testcomponent", {}
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.execute("Test query", ("param1Value", False))
        cursor.execute(
            """multi
        line
        query"""
        )
        cursor.execute("tab\tseparated query")
        cursor.execute("/* leading comment */ query")
        cursor.execute("/* leading comment */ query /* trailing comment */")
        cursor.execute("query /* trailing comment */")
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 6)
        self.assertEqual(spans_list[0].name, "Test")
        self.assertEqual(spans_list[1].name, "multi")
        self.assertEqual(spans_list[2].name, "tab")
        self.assertEqual(spans_list[3].name, "query")
        self.assertEqual(spans_list[4].name, "query")
        self.assertEqual(spans_list[5].name, "query")

    def test_span_succeeded_with_capture_of_statement_parameters(self):
        connection_props = {
            "database": "testdatabase",
            "server_host": "testhost",
            "server_port": 123,
            "user": "testuser",
        }
        connection_attributes = {
            "database": "database",
            "port": "server_port",
            "host": "server_host",
            "user": "user",
        }
        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "testcomponent",
            connection_attributes,
            capture_parameters=True,
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, connection_props
        )
        cursor = mock_connection.cursor()
        cursor.execute("Test query", ("param1Value", False))
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertEqual(span.name, "Test")
        self.assertIs(span.kind, trace_api.SpanKind.CLIENT)

        self.assertEqual(
            span.attributes[SpanAttributes.DB_SYSTEM], "testcomponent"
        )
        self.assertEqual(
            span.attributes[SpanAttributes.DB_NAME], "testdatabase"
        )
        self.assertEqual(
            span.attributes[SpanAttributes.DB_STATEMENT], "Test query"
        )
        self.assertEqual(
            span.attributes["db.statement.parameters"],
            "('param1Value', False)",
        )
        self.assertEqual(span.attributes[SpanAttributes.DB_USER], "testuser")
        self.assertEqual(
            span.attributes[SpanAttributes.NET_PEER_NAME], "testhost"
        )
        self.assertEqual(span.attributes[SpanAttributes.NET_PEER_PORT], 123)
        self.assertIs(span.status.status_code, trace_api.StatusCode.UNSET)

    def test_span_not_recording(self):
        connection_props = {
            "database": "testdatabase",
            "server_host": "testhost",
            "server_port": 123,
            "user": "testuser",
        }
        connection_attributes = {
            "database": "database",
            "port": "server_port",
            "host": "server_host",
            "user": "user",
        }
        mock_span = mock.Mock()
        mock_span.is_recording.return_value = False
        db_integration = dbapi.DatabaseApiIntegration(
            "testname", "testcomponent", connection_attributes
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, connection_props
        )
        cursor = mock_connection.cursor()
        cursor.execute("Test query", ("param1Value", False))
        self.assertFalse(mock_span.is_recording())
        self.assertTrue(mock_span.is_recording.called)
        self.assertFalse(mock_span.set_attribute.called)
        self.assertFalse(mock_span.set_status.called)

    def test_span_failed(self):
        db_integration = dbapi.DatabaseApiIntegration(
            self.tracer, "testcomponent"
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        with self.assertRaises(Exception):
            cursor.execute("Test query", throw_exception=True)

        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertEqual(
            span.attributes[SpanAttributes.DB_STATEMENT], "Test query"
        )
        self.assertIs(span.status.status_code, trace_api.StatusCode.ERROR)
        self.assertEqual(span.status.description, "Exception: Test Exception")

    def test_custom_tracer_provider_dbapi(self):
        resource = resources.Resource.create({"db-resource-key": "value"})
        result = self.create_tracer_provider(resource=resource)
        tracer_provider, exporter = result

        db_integration = dbapi.DatabaseApiIntegration(
            self.tracer, "testcomponent", tracer_provider=tracer_provider
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        with self.assertRaises(Exception):
            cursor.execute("Test query", throw_exception=True)

        spans_list = exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertEqual(span.resource.attributes["db-resource-key"], "value")
        self.assertIs(span.status.status_code, trace_api.StatusCode.ERROR)

    def test_no_op_tracer_provider(self):
        db_integration = dbapi.DatabaseApiIntegration(
            self.tracer,
            "testcomponent",
            tracer_provider=trace_api.NoOpTracerProvider(),
        )

        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Test query")
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 0)

    def test_executemany(self):
        db_integration = dbapi.DatabaseApiIntegration(
            "testname", "testcomponent"
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Test query")
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertEqual(
            span.attributes[SpanAttributes.DB_STATEMENT], "Test query"
        )

    def test_executemany_comment(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "test"
        connect_module.__version__ = mock.MagicMock()
        connect_module.__libpq_version__ = 123
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            commenter_options={"db_driver": False, "dbapi_level": False},
            connect_module=connect_module,
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*dbapi_threadsafety=123,driver_paramstyle='test',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_comment_non_pep_249_compliant(self):
        class MockConnectModule:
            def __getattr__(self, name):
                if name == "__name__":
                    return "test"
                if name == "__version__":
                    return mock.MagicMock()
                if name == "__libpq_version__":
                    return 123
                raise AttributeError("attribute missing")

        connect_module = MockConnectModule()
        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            connect_module=connect_module,
            commenter_options={"db_driver": False},
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*dbapi_level='1.0',dbapi_threadsafety='unknown',driver_paramstyle='unknown',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_comment_matches_db_statement_attribute(self):
        connect_module = mock.MagicMock()
        connect_module.__version__ = mock.MagicMock()
        connect_module.__libpq_version__ = 123
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            commenter_options={"db_driver": False, "dbapi_level": False},
            connect_module=connect_module,
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*dbapi_threadsafety=123,driver_paramstyle='test',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertRegex(
            span.attributes[SpanAttributes.DB_STATEMENT],
            r"Select 1 /\*dbapi_threadsafety=123,driver_paramstyle='test',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/",
        )

        cursor_span_id = re.search(r"[a-zA-Z0-9_]{16}", cursor.query).group()
        db_statement_span_id = re.search(
            r"[a-zA-Z0-9_]{16}", span.attributes[SpanAttributes.DB_STATEMENT]
        ).group()
        self.assertEqual(cursor_span_id, db_statement_span_id)

    def test_compatible_build_version_psycopg_psycopg2_libpq(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "test"
        connect_module.__version__ = mock.MagicMock()
        connect_module.pq = mock.MagicMock()
        connect_module.pq.__build_version__ = 123
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            commenter_options={"db_driver": False, "dbapi_level": False},
            connect_module=connect_module,
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*dbapi_threadsafety=123,driver_paramstyle='test',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_psycopg2_integration_comment(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "psycopg2"
        connect_module.__version__ = "1.2.3"
        connect_module.__libpq_version__ = 123
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            commenter_options={"db_driver": True, "dbapi_level": False},
            connect_module=connect_module,
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*db_driver='psycopg2%%3A1.2.3',dbapi_threadsafety=123,driver_paramstyle='test',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_psycopg_integration_comment(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "psycopg"
        connect_module.__version__ = "1.2.3"
        connect_module.pq = mock.MagicMock()
        connect_module.pq.__build_version__ = 123
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            commenter_options={"db_driver": True, "dbapi_level": False},
            connect_module=connect_module,
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*db_driver='psycopg%%3A1.2.3',dbapi_threadsafety=123,driver_paramstyle='test',libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_mysqlconnector_integration_comment(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "mysql.connector"
        connect_module.__version__ = "1.2.3"
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "mysql",
            enable_commenter=True,
            commenter_options={"db_driver": True, "dbapi_level": False},
            connect_module=connect_module,
        )

        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*db_driver='mysql.connector%%3A1.2.3',dbapi_threadsafety=123,driver_paramstyle='test',mysql_client_version='1.2.3',traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    @mock.patch("opentelemetry.instrumentation.dbapi.util_version")
    def test_executemany_mysqlclient_integration_comment(
        self,
        mock_dbapi_util_version,
    ):
        mock_dbapi_util_version.return_value = "1.2.3"
        connect_module = mock.MagicMock()
        connect_module.__name__ = "MySQLdb"
        connect_module.__version__ = "1.2.3"
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"
        connect_module._mysql = mock.MagicMock()
        connect_module._mysql.get_client_info = mock.MagicMock(
            return_value="123"
        )

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "mysql",
            enable_commenter=True,
            commenter_options={"db_driver": True, "dbapi_level": False},
            connect_module=connect_module,
        )

        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*db_driver='MySQLdb%%3A1.2.3',dbapi_threadsafety=123,driver_paramstyle='test',mysql_client_version='123',traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_pymysql_integration_comment(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "pymysql"
        connect_module.__version__ = "1.2.3"
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"
        connect_module.get_client_info = mock.MagicMock(return_value="123")

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "mysql",
            enable_commenter=True,
            commenter_options={"db_driver": True, "dbapi_level": False},
            connect_module=connect_module,
        )

        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*db_driver='pymysql%%3A1.2.3',dbapi_threadsafety=123,driver_paramstyle='test',mysql_client_version='123',traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

    def test_executemany_flask_integration_comment(self):
        connect_module = mock.MagicMock()
        connect_module.__name__ = "test"
        connect_module.__version__ = mock.MagicMock()
        connect_module.__libpq_version__ = 123
        connect_module.apilevel = 123
        connect_module.threadsafety = 123
        connect_module.paramstyle = "test"

        db_integration = dbapi.DatabaseApiIntegration(
            "testname",
            "postgresql",
            enable_commenter=True,
            commenter_options={"db_driver": False, "dbapi_level": False},
            connect_module=connect_module,
        )
        current_context = context.get_current()
        sqlcommenter_context = context.set_value(
            "SQLCOMMENTER_ORM_TAGS_AND_VALUES", {"flask": 1}, current_context
        )
        context.attach(sqlcommenter_context)

        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.executemany("Select 1;")
        self.assertRegex(
            cursor.query,
            r"Select 1 /\*dbapi_threadsafety=123,driver_paramstyle='test',flask=1,libpq_version=123,traceparent='\d{1,2}-[a-zA-Z0-9_]{32}-[a-zA-Z0-9_]{16}-\d{1,2}'\*/;",
        )

        clear_context = context.set_value(
            "SQLCOMMENTER_ORM_TAGS_AND_VALUES", {}, current_context
        )
        context.attach(clear_context)

    def test_callproc(self):
        db_integration = dbapi.DatabaseApiIntegration(
            "testname", "testcomponent"
        )
        mock_connection = db_integration.wrapped_connection(
            mock_connect, {}, {}
        )
        cursor = mock_connection.cursor()
        cursor.callproc("Test stored procedure")
        spans_list = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans_list), 1)
        span = spans_list[0]
        self.assertEqual(
            span.attributes[SpanAttributes.DB_STATEMENT],
            "Test stored procedure",
        )

    @mock.patch("opentelemetry.instrumentation.dbapi")
    def test_wrap_connect(self, mock_dbapi):
        dbapi.wrap_connect(self.tracer, mock_dbapi, "connect", "-")
        connection = mock_dbapi.connect()
        self.assertEqual(mock_dbapi.connect.call_count, 1)
        self.assertIsInstance(connection.__wrapped__, mock.Mock)

    @mock.patch("opentelemetry.instrumentation.dbapi")
    def test_unwrap_connect(self, mock_dbapi):
        dbapi.wrap_connect(self.tracer, mock_dbapi, "connect", "-")
        connection = mock_dbapi.connect()
        self.assertEqual(mock_dbapi.connect.call_count, 1)

        dbapi.unwrap_connect(mock_dbapi, "connect")
        connection = mock_dbapi.connect()
        self.assertEqual(mock_dbapi.connect.call_count, 2)
        self.assertIsInstance(connection, mock.Mock)

    def test_instrument_connection(self):
        connection = mock.Mock()
        # Avoid get_attributes failing because can't concatenate mock
        connection.database = "-"
        connection2 = dbapi.instrument_connection(self.tracer, connection, "-")
        self.assertIs(connection2.__wrapped__, connection)

    def test_uninstrument_connection(self):
        connection = mock.Mock()
        # Set connection.database to avoid a failure because mock can't
        # be concatenated
        connection.database = "-"
        connection2 = dbapi.instrument_connection(self.tracer, connection, "-")
        self.assertIs(connection2.__wrapped__, connection)

        connection3 = dbapi.uninstrument_connection(connection2)
        self.assertIs(connection3, connection)

        with self.assertLogs(level=logging.WARNING):
            connection4 = dbapi.uninstrument_connection(connection)
        self.assertIs(connection4, connection)


# pylint: disable=unused-argument
def mock_connect(*args, **kwargs):
    database = kwargs.get("database")
    server_host = kwargs.get("server_host")
    server_port = kwargs.get("server_port")
    user = kwargs.get("user")
    return MockConnection(database, server_port, server_host, user)


class MockConnection:
    def __init__(self, database, server_port, server_host, user):
        self.database = database
        self.server_port = server_port
        self.server_host = server_host
        self.user = user

    # pylint: disable=no-self-use
    def cursor(self):
        return MockCursor()


class MockCursor:
    def __init__(self) -> None:
        self.query = ""
        self.params = None
        # Mock mysql.connector modules and method
        self._cnx = mock.MagicMock()
        self._cnx._cmysql = mock.MagicMock()
        self._cnx._cmysql.get_client_info = mock.MagicMock(
            return_value="1.2.3"
        )

    # pylint: disable=unused-argument, no-self-use
    def execute(self, query, params=None, throw_exception=False):
        if throw_exception:
            # pylint: disable=broad-exception-raised
            raise Exception("Test Exception")

    # pylint: disable=unused-argument, no-self-use
    def executemany(self, query, params=None, throw_exception=False):
        if throw_exception:
            # pylint: disable=broad-exception-raised
            raise Exception("Test Exception")
        self.query = query
        self.params = params

    # pylint: disable=unused-argument, no-self-use
    def callproc(self, query, params=None, throw_exception=False):
        if throw_exception:
            # pylint: disable=broad-exception-raised
            raise Exception("Test Exception")
