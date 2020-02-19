#!/bin/bash
source config.sh

START=$(date +%s);
# Generate $(pwd)/output tables
psql $BUILD_ENGINE -f sql/export.sql

# -- export
# --all records
psql $BUILD_ENGINE -c "\copy (SELECT * FROM bc_export) TO '$(pwd)/output/bc.csv' DELIMITER ',' CSV HEADER;"