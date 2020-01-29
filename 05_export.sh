DB_CONTAINER_NAME=bc

docker exec $DB_CONTAINER_NAME psql -h localhost -U postgres -c "\copy (SELECT * FROM pad_address_reformat)
                                TO '/home/db-buildings-composite/output/pad_address_reformat.csv'
                                DELIMITER ',' CSV HEADER;"
