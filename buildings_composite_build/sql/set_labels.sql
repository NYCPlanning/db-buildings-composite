UPDATE dcp_buildings_composite a
SET label = CASE WHEN usps_addr is null THEN pad_addr ELSE usps_addr END;