install:
	poetry install

define port-command
	$(shell [ -z "$(PORT)" ] && echo "" || echo "--port $(PORT)")
endef

run:
	poetry run fastapi run $(call port-command)

dev:
	poetry run fastapi dev $(call port-command)

test:
	poetry run pytest
