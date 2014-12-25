all:
	@echo
	@echo 'Usage: make <target>'
	@echo
	@echo '    unittest			run unit tests'
	@echo '    functest			run funtional tests'
	@echo

dist:
	tar czv \
	--exclude='*.git*' \
	--exclude='*.log' \
	--exclude='*~' \
	--exclude='*#' \
	--exclude='*.gz' \
	--exclude='*.pyc' \
	-f seafobj.tar.gz seafobj

unittest:
	@bash .unittests

functest:
	@bash .functests

pylint:
	@bash .pylint

.PHONY: all unittest functest pylint
