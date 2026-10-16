# Local workflow. Venvs: service/.venv (vq-service + `-e ../gen` for tests), gen/.venv (generator).
BOOTSTRAP ?= localhost:9092
DEMO ?= /tmp/vq-demo

.PHONY: help kafka-up kafka-down test test-kafka demo

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/'

kafka-up: ## start a local single-node Kafka 3.9.1 (KRaft, no Docker) on localhost:9092
	gen/scripts/kafka_local.sh start

kafka-down: ## stop it (only the broker started by kafka-up; its data is removed)
	gen/scripts/kafka_local.sh stop

test: ## both test suites, no broker needed (Kafka e2e tests are skipped)
	cd service && .venv/bin/python -m pytest -q
	cd gen && .venv/bin/python -m pytest -q

test-kafka: ## Kafka end-to-end tests of both projects against $(BOOTSTRAP), with measurements (make kafka-up first)
	cd service && KAFKA_BOOTSTRAP=$(BOOTSTRAP) .venv/bin/python -m pytest -q -s -m kafka
	cd gen && KAFKA_BOOTSTRAP=$(BOOTSTRAP) .venv/bin/python -m pytest -q -m kafka

demo: ## offline: 1 h generator record of site.yaml -> vq-service process -> $(DEMO)/out.jsonl
	mkdir -p $(DEMO)
	cd gen && .venv/bin/python -m generator run --site ../site.yaml --no-kafka --speed 0 --duration-s 3600 \
		--record $(DEMO)/rec.jsonl --truth $(DEMO)/truth.jsonl
	cd service && .venv/bin/python -m vqservice process --config service.yaml --record $(DEMO)/rec.jsonl \
		--out $(DEMO)/out.jsonl
	@echo "first decisions:" && grep -m 5 '"decision.v1"' $(DEMO)/out.jsonl
