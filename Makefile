## help: Display list of commands (from gazr.io)
.PHONY: help
help: Makefile
	@sed -n 's|^##||p' $< | column -t -s ':' | sed -e 's|^| |'

## init: Init dev env
.PHONY: init
init:
	pip install --user twine
	pip install --user black
	pip install --user isort
	pip install --user flake8
	pip install --user pytest
	pip install --user psycopg2

## build: Create dist and wheel
.PHONY: build
build: clean format style test
	python setup.py sdist bdist_wheel
	twine check dist/*

## distribute: Publish version to pypi
.PHONY: distribute
distribute: build
	twine upload dist/*

## format: Autoformat code
.PHONY: format
format:
	black csv2pg tests
	isort csv2pg tests

## style: Test code format
.PHONY: style
style:
	black --check csv2pg tests
	isort --check-only csv2pg tests
	flake8 csv2pg tests

## init-test: Functional tests
.PHONY: init-test
init-test:
	docker stop csv2pg-test -t 10 || true
	docker run -d --rm \
		-p 25432:5432 \
		--name csv2pg-test \
		-e POSTGRES_DB=test \
		-e POSTGRES_USER=test \
		-e POSTGRES_PASSWORD=test \
		postgres
	sleep 5

## test: Functional tests
.PHONY: test
test: init-test
	PYTHONPATH=. pytest --pdb tests
	docker stop csv2pg-test -t 10 || true

## clean: Remove temporary files
.PHONY: clean
clean:
	rm -rf *.egg-info dist/ build/
	find . -name '*.pyc' -delete
	find . -name "__pycache__" -type d -prune -exec rm -rf '{}' +
