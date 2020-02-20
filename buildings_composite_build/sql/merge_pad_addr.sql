UPDATE dcp_buildings_composite a 
SET pad_addr = b.pad_addr
FROM dcp_pad_formatted."2020" b
WHERE a.bin = b.bin;