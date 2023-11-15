SHELL := /bin/bash

example: libgreptimedb
	./scripts/build_example.sh

libgreptimedb:
	./scripts/build_so.sh

format:
	./scripts/format.sh