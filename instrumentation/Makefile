SHELL=bash

.DEFAULT_GOAL := nothing

INSTRUMENTORS := "asgi dbapi wsgi"

nothing:
	@echo -e "Welcome! Options:"
	@echo -e "  - 'make package':"
	@echo -e "          Build all custom instrumentors (sdist and bdist)."

package:
	@for i in asgi dbapi wsgi; do \
		echo "Packaging instrumentor for $$i"; \
		rm -rf ./opentelemetry-instrumentation-$$i/dist; \
		python3.8 -m build ./opentelemetry-instrumentation-$$i --outdir ./opentelemetry-instrumentation-$$i/dist; \
	done
	@echo "Done"

publish-wheels:
	@for i in asgi dbapi wsgi; do \
		python3.8 -m twine upload ./opentelemetry-instrumentation-$$i/dist/*.whl; \
	done
	@echo "Done"

clean:
	@for i in asgi dbapi wsgi; do \
		echo "Removing builds for instrumentor of $$i"; \
		rm -rf ./opentelemetry-instrumentation-$$i/dist/*.whl; \
		rm -rf ./opentelemetry-instrumentation-$$i/dist/*.tar.gz; \
	done
	@echo "Done"
