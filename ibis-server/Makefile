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

docker-build:
	docker image build . -t wren-engine-ibis

docker-run:
	docker run -it --rm -p 8000:8000 --env-file .env wren-engine-ibis
