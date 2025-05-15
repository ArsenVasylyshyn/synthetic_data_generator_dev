.PHONY: run validate all

run:
	python main.py

validate:
	python scripts/validate.py

all: run validate
