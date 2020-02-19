#!/bin/bash
source config.sh

START=$(date +%s);

docker run --rm\
            -v `pwd`:/home/buildings_composite_build\
            -w /home/buildings_composite_build\
            --env-file .env\
            sptkl/cook:latest bash -c "pip3 install -r python/requirements.txt; python3 python/dataloading.py"

END=$(date +%s);
echo $((END-START)) | awk '{print int($1/60)" minutes and "int($1%60)" seconds elapsed."}'