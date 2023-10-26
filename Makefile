project_name = udfs
build_dir = dist
src := $(shell find $(project_name)/ -name "*.py" -type f)
setup_sql = ./dist/setup.sql
random_alphanum=$(shell cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)
output_udfs = $(build_dir)/whylogs_udf.py $(build_dir)/whylabs_upload_udf.py
outputs = $(output_udfs) $(build_dir)/version.sha $(setup_sql)
VERSION ?= dev

ifndef VERSION
$(error VERSION is not set)
endif

.PHONY: udfs lint format format-fix setup test help populate_demo_table all version_py clean $(build_dir)/version.sha

default:help

all: $(outputs) version_py  ## Build the UDFs and the setup script

udfs: $(output_udfs)

clean:  ## Remove generated build files
	rm -rf dist/

upload-dev: all  ## Upload dev UDFs to the public bucket
	@$(call i, Uploading UDFs to public dev bucket)
	aws s3 sync "./$(build_dir)/" s3://whylabs-snowflake-udfs/udfs/dev/$(random_alphanum)/
	@echo "Uploaded to s3://whylabs-snowflake-udfs/udfs/dev/$(random_alphanum)"
	@echo "whylogs_udf: '@whylabs_udf_stage/dev/$(random_alphanum)/whylogs_udf.py'" 
	@echo "whylabs_upload_udf: '@whylabs_udf_stage/dev/$(random_alphanum)/whylogs_upload_udf.py'" 

upload-dev-local: all  ## Upload dev UDFs to the Snowflake account
	@$(call i, Uploading dev mode UDFs to Snowflake @dev stage)
	snowsql -c whylabs --query "create stage if not exists dev;" 
	snowsql -c whylabs --query "put file://./dist/*.py @dev/ auto_compress=false overwrite=true;" 

$(setup_sql): $(build_dir) ./sql/*.sql
	@$(call i, Generating the setup.sql file)
	rm -f $(setup_sql) && touch $(setup_sql)

	echo "-- Set up network rules" >> $(setup_sql)
	cat ./sql/networking.sql >> $(setup_sql)

	echo "\n-- Set up integrations" >> $(setup_sql)
	cat ./sql/integrations.sql >> $(setup_sql)

	echo "\n-- Set up storage integrations" >> $(setup_sql)
	cat ./sql/storage.sql >> $(setup_sql)

	echo "\n-- Create the UDFs" >> $(setup_sql)
	cat ./sql/create-udf.sql >> $(setup_sql)

./udfs/version.py:
	poetry run python ./scripts/create_version.py $(VERSION) > ./udfs/version.py

$(build_dir)/version.sha: $(build_dir)
	@$(call i, Generating the setup.sha file)
	echo $(VERSION) > $(build_dir)/version.sha

version_py: ./udfs/version.py  ## Generate the version python module

$(build_dir):
	@$(call i, Generating the build dir)
	mkdir -p $(build_dir)

$(build_dir)/whylogs_udf.py: lint format udfs/whylogs_udf.py $(build_dir)
	@$(call i, Generating the whylogs_udf.py file)
	poetry run python ./scripts/merger.py --entry udfs/whylogs_udf.py --output $(build_dir)/whylogs_udf.py
	poetry run python ./scripts/remove_relative_imports.py $(build_dir)/whylogs_udf.py

$(build_dir)/whylabs_upload_udf.py: lint format udfs/whylabs_upload_udf.py $(build_dir)
	@$(call i, Generating the whylabs_upload_udf.py file)
	poetry run python ./scripts/merger.py --entry udfs/whylabs_upload_udf.py --output $(build_dir)/whylabs_upload_udf.py
	poetry run python ./scripts/remove_relative_imports.py $(build_dir)/whylabs_upload_udf.py

lint: ./udfs/version.py  ## Check for type issues with mypy
	@$(call i, Linting with mypy)
	poetry run mypy $(project_name)/

format: ## Check for formatting issues
	@$(call i, Formatting with black)
	poetry run black --check --exclude version.py $(project_name)
	poetry run autoflake --check --in-place --remove-unused-variables $(src)

format-fix: ## Fix formatting issues
	@$(call i, Formatting and fixing with black)
	poetry run black --check --exclude version.py $(project_name)
	poetry run black --exclude version.py  $(project_name)
	poetry run autoflake --in-place --remove-unused-variables $(src)

setup: ## Install dependencies with poetry
	poetry install

test: ## Run unit tests
	@$(call i, Running unit tests)
	PYTHONPATH=. poetry run pytest

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'

define i
echo "\n\e[1;34m[INFO]$(1)\e[0m\n"
endef

define w
echo "\n\e[1;93m[WARN]$(1)\e[0m\n"
endef

define e
echo "\n\e[1;91m[ERROR]$(1)\e[0m\n"
endef