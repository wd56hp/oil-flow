# Use the project venv so pytest and SQLAlchemy match requirements.txt (not system Python).
.PHONY: test install

test:
	.venv/bin/pytest tests/ -v

install:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt
