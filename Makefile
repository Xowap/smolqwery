PYTHON_BIN ?= poetry run python
ENV ?= pypitest

format: isort black

black:
	$(PYTHON_BIN) -m black --target-version py38 src example

isort:
	$(PYTHON_BIN) -m isort src example

build:
	poetry install
	cd docs && poetry run make html
	poetry run pip list --format=freeze | grep -v typefit > requirements.txt

publish:
	poetry publish --build
