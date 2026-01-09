#!/bin/bash

ROOT_DIR=$(dirname $(dirname $(dirname $(realpath "$0"))))

docker run --rm \
           --name postgres \
           --publish=5432:5432 \
           --volume=$ROOT_DIR/Volumes/PostgreSQL/data:/var/lib/postgresql/data \
           --volume=$ROOT_DIR/Datasets:/var/lib/postgresql/import \
           --env=POSTGRES_PASSWORD="12345678" \
           --env=PGDATA="/var/lib/postgresql/data/pgdata" \
           pgrouting/pgrouting:17-3.5-main
