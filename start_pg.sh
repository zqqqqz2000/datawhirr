#!/usr/bin/bash

podman run -d \
	-p 5432:5432 \
	--name test_pg \
  -e POSTGRES_PASSWORD=test \
	-e POSTGRES_DB=test \
	-e POSTGRES_USER=test \
	-d postgres:16.3
