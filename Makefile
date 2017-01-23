.PHONY: install
install:
	pip install -U pip
	pip install -r requirements.txt
	pip install -r tests/requirements.txt

.PHONY: isort
isort:
	isort -rc -w 120 app
	isort -rc -w 120 tests

.PHONY: lint
lint:
	flake8 app/ tests/
	pytest app -p no:sugar -q --cache-clear

.PHONY: test
test:
	py.test --cov=app

.PHONY: testcov
testcov: test
	echo "building coverage html"; coverage html

.PHONY: all
all: testcov lint
	echo "building coverage html"; coverage html
