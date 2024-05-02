OpenTelemetry WSGI Middleware
=============================

|pypi|

.. |pypi| image:: https://badge.fury.io/py/opentelemetry-instrumentation-wsgi.svg
   :target: https://pypi.org/project/opentelemetry-instrumentation-wsgi/


This library provides a WSGI middleware that can be used on any WSGI framework
(such as Django / Flask) to track requests timing through OpenTelemetry.


About this
------------

This is a fork of ``opentelemetry-instrumentation-wsgi`` that uses custom ``sw-apm-opentelemetry-instrumentation``.


Installation
------------

::

    pip install sw-apm-opentelemetry-instrumentation-wsgi

References
----------

* `OpenTelemetry WSGI Middleware <https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/wsgi/wsgi.html>`_
* `OpenTelemetry Project <https://opentelemetry.io/>`_
* `WSGI <https://www.python.org/dev/peps/pep-3333>`_
* `OpenTelemetry Python Examples <https://github.com/open-telemetry/opentelemetry-python/tree/main/docs/examples>`_
