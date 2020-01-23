DB_CONTAINER_NAME=bc

docker run -it --rm\
            --network=host\
            -v `pwd`:/home/db-buildings-composite\
            -w /home/db-buildings-composite\
            --env-file .env\
            sptkl/docker-geosupport:19d bash -c "pip3 install -r python/requirements.txt; python3 python/melissa.py"
