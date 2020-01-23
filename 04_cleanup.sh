DB_CONTAINER_NAME=bc

docker kill $DB_CONTAINER_NAME
docker container prune -f;
docker volume prune -f
