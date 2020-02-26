UPDATE dcp_buildings_composite a 
SET usps_addr = b.usps_addr
FROM dcp_melissa_formatted."2020" b
WHERE a.bin = b.bin;