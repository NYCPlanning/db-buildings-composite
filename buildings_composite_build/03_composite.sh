#!/bin/bash
source config.sh

START=$(date +%s);

echo "Starting to build building composite"
psql $BUILD_ENGINE -f sql/merge_pluto_footprints.sql
psql $BUILD_ENGINE -f sql/melissa_zips.sql
psql $BUILD_ENGINE -f sql/merge_pad_addr.sql


END=$(date +%s);
echo $((END-START)) | awk '{print int($1/60)" minutes and "int($1%60)" seconds elapsed."}'