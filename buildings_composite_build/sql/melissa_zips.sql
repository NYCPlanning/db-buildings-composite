WITH _zips AS (
        SELECT bin, city, zip,
            row_number() OVER (PARTITION BY bin ORDER BY bin, zip) AS row_number
            FROM dcp_melissa
            WHERE bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        )
        , _zips_count AS (
            SELECT bin, COUNT(distinct zip) AS zip_count
                FROM dcp_melissa
                WHERE bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
                GROUP BY bin
        )
UPDATE dcp_buildings_composite c
SET bin = a.bin,
    city = a.city,
    zip = a.zip,
    multzip = CASE
	                WHEN b.zip_count > 1 THEN '1'
	                ELSE ''
                END
FROM _zips a, _zips_count b
WHERE a.bin = b.bin
AND a.bin = c.bin
AND a.row_number = 1;