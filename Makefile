.poetry.timestamps: pyproject.toml poetry.lock
	poetry install
	touch $@

.PHONY: prospector
prospector: .poetry.timestamps
	poetry run prospector --output=pylint -X

.PHONY: pyprest
pytest: .poetry.timestamps
	poetry run pytest --verbose -vv tests
