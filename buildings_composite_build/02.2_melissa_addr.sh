#!/bin/bash
source config.sh

docker run --rm\
    -v `pwd`:/home/buildings_composite_build\
    -w /home/buildings_composite_build\
    --env-file .env\
    sptkl/cook:latest bash -c "pip3 install -r python/requirements.txt; python3 python/melissa_looped.py"
