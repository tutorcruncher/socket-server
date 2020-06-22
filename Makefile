.DEFAULT_GOAL := all

.PHONY: install
install:
	pip install -r requirements.txt

.PHONY: format
format:
	isort -rc -w 120 tcsocket
	isort -rc -w 120 tests
	black -S -l 120 --target-version py38 tcsocket tests

.PHONY: isort
isort:
	isort -rc -w 120 tcsocket
	isort -rc -w 120 tests

.PHONY: lint
lint:
	flake8 tcsocket/ tests/
	pytest tcsocket -p no:sugar -q --cache-clear

.PHONY: test
test:
	pytest --cov=tcsocket

.PHONY: build
build:
	docker build tcsocket/ -t tcsocket

.PHONY: prod-push
prod-push:
	git push heroku `git rev-parse --abbrev-ref HEAD`:master
