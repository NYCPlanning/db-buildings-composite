DROP INDEX IF EXISTS dcp_pluto.idx_pl_bbl;
        CREATE INDEX idx_pl_bbl ON dcp_pluto."19v2" (bbl);

DROP INDEX IF EXISTS doitt_buildingfootprints.idx_bf_mpluto_bbl;
        CREATE INDEX idx_bf_mpluto_bbl ON doitt_buildingfootprints."2019/11/29" (mpluto_bbl);

DROP INDEX IF EXISTS doitt_buildingfootprints.idx_bf_bin;
        CREATE INDEX idx_bf_bin ON doitt_buildingfootprints."2019/11/29" (bin);

SELECT f.mpluto_bbl AS bbl,
        f.bin AS bin,
        f.doitt_id AS doitt_id,
        p.cd AS cd,
        p.bldgclass AS bldgclass,
        p.landuse AS landuse,
        p.ownername AS ownername,
        p.ownertype AS ownertype,
        p.numbldgs AS numbldgs,
        p.numfloors AS numfloors,
        p.lotarea AS lotarea,
        p.unitsres AS unitsres,
        p.unitstotal AS unitstotal,
        p.bsmtcode AS bsmtcode,
        p.proxcode AS proxcode,
        p.lottype AS lottype,
        p.yearbuilt AS yearbuilt,
        p.yearalter1 AS yearalter1,
        p.yearalter2 AS yearalter2,
        p.borocode AS borocode,
        f.heightroof AS height_roof,
        f.feat_code AS feature_code,
        f.groundelev AS ground_elevation,
        f.lststatype AS last_status_type,
        p.borocode || p.ct2010 || p.cb2010 as bctcb,
        ST_AsText(f.the_geom) AS shape
        FROM doitt_buildingfootprints.latest f
        LEFT JOIN dcp_pluto.latest p
        ON f.mpluto_bbl = p.bbl
        WHERE f.bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        AND f.mpluto_bbl IS NOT NULL;