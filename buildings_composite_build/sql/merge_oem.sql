UPDATE dcp_buildings_composite a 
SET complex_type = b.complex_type,
complex = b.complex,
oem_name = b.oem_name,
mas_constrtype = b.constrtype
FROM em_buildings_composite b
WHERE a.bin = b.bin;