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
	py.test --cov=tcsocket

.PHONY: testcov
testcov: test
	echo "building coverage html"; coverage html

.PHONY: all
all: testcov lint

