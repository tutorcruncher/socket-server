.PHONY: install
install:
	pip install -r requirements.txt

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

.PHONY: testcov
testcov: test
	coverage html

.PHONY: all
all: testcov lint

