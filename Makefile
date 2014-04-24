all:
	@echo
	@echo 'Usage: make <target>'
	@echo
	@echo '    unittest			run unit tests'
	@echo '    functest			run funtional tests'
	@echo

unittest:
	@bash .unittests

functest:
	@bash .functests

pylint:
	@bash .pylint

.PHONY: all unittest functest pylint
