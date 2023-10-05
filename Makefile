project_name = udfs
build_dir = dist
outputs = $(build_dir)/whylogs_udf.py $(build_dir)/whylabs_upload_udf.py
src := $(shell find $(project_name)/ -name "*.py" -type f)

.PHONY: udfs lint format format-fix setup test help populate_demo_table all

default:help

all: $(project_name) ./dist/setup.sql

udfs: $(outputs)

./dist/setup.sql: build_dir ./sql/*.sql
	touch ./dist/setup.sql
	cat ./sql/networking.sql >> ./dist/setup.sql
	cat ./sql/integrations.sql >> ./dist/setup.sql
	cat ./sql/storage.sql >> ./dist/setup.sql
	cat ./sql/create-udf.sql >> ./dist/setup.sql

build_dir:
	mkdir -p $(build_dir)

$(build_dir)/whylogs_udf.py: udfs/whylogs_udf.py build_dir
	python ./scripts/merger.py --entry udfs/whylogs_udf.py --output $(build_dir)/whylogs_udf.py

$(build_dir)/whylabs_upload_udf.py: udfs/whylabs_upload_udf.py build_dir
	python ./scripts/merger.py --entry udfs/whylabs_upload_udf.py --output $(build_dir)/whylabs_upload_udf.py

lint: ## Check for type issues with mypy
	poetry run mypy $(project_name)/

format: ## Check for formatting issues
	poetry run black --check --line-length 120 $(project_name)
	poetry run autoflake --check --in-place --remove-unused-variables $(src)

format-fix: ## Fix formatting issues
	poetry run black --line-length 120 $(project_name)
	poetry run autoflake --in-place --remove-unused-variables $(src)

setup: ## Install dependencies with poetry
	poetry install

test: ## Run unit tests
	PYTHONPATH=. poetry run pytest

populate_demo_table: ## Use the data gen script to upload new data to the dummy table.
	for i in $$(seq 1 100); do python ./generate-data.py | snowsql -c whylabs; done

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
