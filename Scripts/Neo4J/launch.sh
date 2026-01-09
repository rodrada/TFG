#!/bin/bash

ROOT_DIR=$(dirname $(dirname $(dirname $(realpath "$0"))))

docker run --rm \
           --name neo4j \
           --publish=7474:7474 \
           --publish=7687:7687 \
           --volume="$ROOT_DIR/Volumes/Neo4J/data":/data \
           --volume="$ROOT_DIR/Volumes/Neo4J/plugins":/plugins \
           --volume="$ROOT_DIR/Datasets":/import \
           --env=NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
           --env=NEO4J_dbms_cypher_lenient__create__relationship="true" \
           --env=NEO4J_dbms_security_auth__enabled="false" \
           --env=NEO4J_dbms_security_procedures_allowlist="spatial.*,apoc.*" \
           --env=NEO4J_dbms_security_procedures_unrestricted="spatial.*,apoc.*" \
           --env=NEO4J_apoc_trigger_enabled="true" \
           --env=NEO4J_PLUGINS='["apoc"]' \
           neo4j:5.19.0-enterprise
