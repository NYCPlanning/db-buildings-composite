INSERT INTO dcp_buildings_composite (complex_type,
                                    complex,
                                    oem_name,
                                    mas_constrtype)    
SELECT b.complex_type AS complex_type,
        b.complex AS complex,
        b.oem_name AS oem_name,
        b.constrtype AS mas_constrtype
FROM dcp_buildings_composite a, em_buildings_composite b
WHERE a.bin = b.bin;