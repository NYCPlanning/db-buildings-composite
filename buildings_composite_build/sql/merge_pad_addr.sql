INSERT INTO dcp_buildings_composite (pad_addr)    
SELECT b.pad_addr AS pad_addr
FROM dcp_buildings_composite a, dcp_pad_formatted."2020" b
WHERE a.bin = b.bin;