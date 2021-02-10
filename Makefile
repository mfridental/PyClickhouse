test: run
	# run tests and stop container, so it gets removed
	python -m unittest discover ; docker stop clickhouse-test-server

run:
	# start local temporary clickhouse server: https://github.com/yandex/ClickHouse/tree/master/docker/server
	docker run -d -p 8124:8123 -p 9001:9000 --rm --name clickhouse-test-server --ulimit nofile=262144:262144 yandex/clickhouse-server

stop:
	docker stop clickhouse-test-server || true

build:
	sh ./build.sh

to_3:
	rm -f Pipfile.lock
	pipenv install --dev --python 3

to_2:
	rm -f Pipfile.lock
	pipenv install --dev --python 2.7

test_2: to_2
	pipenv run python -m unittest discover -s test

test_3: to_3
	pipenv run python -m unittest


.PHONY: test run stop build test_2 test_3 to_2 to_3