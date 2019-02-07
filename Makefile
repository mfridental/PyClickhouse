test:
	# start local temporary clickhouse server: https://github.com/yandex/ClickHouse/tree/master/docker/server
	docker run -d -p 8123:8123 -p 9000:9000 --rm --name clickhouse-test-server --ulimit nofile=262144:262144 yandex/clickhouse-server
	# run tests and stop container, so it gets removed
	python -m unittest discover ; docker stop clickhouse-test-server

stop:
	docker stop clickhouse-test-server || true

build:
	sh ./build.sh

.PHONY: test