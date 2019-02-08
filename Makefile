test: run
	# run tests and stop container, so it gets removed
	python -m unittest discover ; docker stop clickhouse-test-server

run:
	# start local temporary clickhouse server: https://github.com/yandex/ClickHouse/tree/master/docker/server
	docker run -d -p 8123:8123 -p 9000:9000 --rm --name clickhouse-test-server --ulimit nofile=262144:262144 yandex/clickhouse-server

stop:
	docker stop clickhouse-test-server || true

build:
	sh ./build.sh

to_3:
	rm Pipfile.lock || true
	pipenv install --dev --python 3

to_2:
	rm Pipfile.lock || true
	pipenv install --dev --python 2.7


.PHONY: test run stop build