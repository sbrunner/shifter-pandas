
.venv:
	python3 -m venv $@

.ci.requirements: .venv
	./.venv/bin/pip install -r ci/requirements.txt
	touch $@

requirements.txt: .ci.requirements pyproject.toml poetry.lock
	poetry export --output=$@
	./.venv/bin/pip install --requirement=$@

requirements-dev.txt: .ci.requirements pyproject.toml poetry.lock
	poetry export --dev --output=$@
	./.venv/bin/pip install --requirement=$@

.PHONY: prospector
prospector: requirements-dev.txt
	./.venv/bin/prospector --output=pylint -X

.PHONY: pyprest
pytest: requirements-dev.txt
	@echo not implemented yet
	#./.venv/bin/pytest --verbose --cov=shifter_panda -vv --cov-report=term-missing
