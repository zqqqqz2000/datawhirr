#!/usr/bin/bash

function startup_docker() {
	podman stop test_pg
	podman rm test_pg
	podman run -d \
		-p 5432:5432 \
		--name test_pg \
		-e POSTGRES_PASSWORD=test \
		-e POSTGRES_DB=test \
		-e POSTGRES_USER=test \
		-d postgres:16.3
}

function insert_test_data() {
	podman exec test_pg psql -Utest -c "$(
		cat <<EOF
  create table test(
    id  SERIAL PRIMARY KEY,
    a   varchar(128) not null,
    b   REAL null
  );
  insert into test(a, b) values 
    ('123', 12),
    ('456', null),
    ('789', 45),
    ('xxx', 192)
  ;
EOF
	)"
}

case "$1" in
startup)
	startup_docker
	;;
insert)
	insert_test_data
	;;
*)
	echo "must specify a command in 'startup', 'insert'"
	;;
esac
