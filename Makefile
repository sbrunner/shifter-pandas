
.pipenv.timestamps: Pipfile.lock
	pipenv sync --dev
	pipenv install --skip-lock --editable .
	touch $@

.PHONY: prospector
prospector: .pipenv.timestamps
	pipenv run prospector --output=pylint

.PHONY: pyprest
pytest: .pipenv.timestamps
	pipenv run pytest --verbose --cov=shifter_panda -vv --cov-report=term-missing
