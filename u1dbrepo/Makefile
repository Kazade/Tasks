
.PHONY: check check-verbose html-docs check-valgrind build-debug

check: build-inplace
	python -m testtools.run discover

build-inplace: build-cmake
	export CFLAGS='-Werror';\
	python setup.py build_ext -i

build-debug: build-cmake
	export CFLAGS='-Werror';\
	python-dbg setup.py build_ext -i

check-valgrind: build-debug
	valgrind --tool=memcheck \
	--suppressions=custom.supp \
	python-dbg -m testtools.run discover

check-valgrind-leaks: build-debug
	valgrind --tool=memcheck --suppressions=custom.supp \
	--track-origins=yes --num-callers=40 --leak-resolution=high \
	--leak-check=full python-dbg -m testtools.run discover

check-verbose:
	python -c "import unittest, sys; from testtools import run; run.TestProgram(argv=sys.argv, testRunner=unittest.TextTestRunner(verbosity=2), stdout=sys.stdout)" discover

html-docs:
	cd html-docs; make html

build-cmake:
	export CFLAGS='-fPIC';\
	cd src; cmake . ; make
