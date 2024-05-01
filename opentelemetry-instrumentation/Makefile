SHELL=bash

.DEFAULT_GOAL := nothing

nothing:
	@echo -e "Welcome! Options:"
	@echo -e "  - 'make package':"
	@echo -e "          Build the agent package distribution (sdist and bdist)."

package:
	@rm -rf ./dist
	@python3.8 -m build --outdir ./dist

clean:
	@rm -rf dist/
