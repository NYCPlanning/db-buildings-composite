from sqlalchemy import create_engine
import pandas as pd
import os
import csv
import numpy as np
import gc

if __name__ == '__main__':
    # Connect to postgres db
    recipe_engine = create_engine(os.environ['RECIPE_ENGINE'])
    edm_data = create_engine(os.environ['EDM_DATA'])
    #engine = create_engine(os.environ['BUILD_ENGINE'])

    create_pluto_index_sql = '''
        DROP INDEX IF EXISTS dcp_pluto.idx_pl_bbl;
        CREATE INDEX idx_pl_bbl ON dcp_pluto."19v2" (bbl);
    '''

    create_bf_index_sql = '''
        DROP INDEX IF EXISTS doitt_buildingfootprints.idx_bf_mpluto_bbl;
        CREATE INDEX idx_bf_mpluto_bbl ON doitt_buildingfootprints."2019/11/29" (mpluto_bbl);
    '''

    create_bf_index_bin_sql = '''
        DROP INDEX IF EXISTS doitt_buildingfootprints.idx_bf_bin;
        CREATE INDEX idx_bf_bin ON doitt_buildingfootprints."2019/11/29" (bin);
    '''

    # Join building footprints to PLUTO and return selected fields.
    import_sql = '''SELECT f.mpluto_bbl AS bbl,
        f.mpluto_bbl AS bbl_text,
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
        f.lststatype AS last_status_type,
        p.borocode || p.ct2010 || p.cb2010 as bctcb,
        f.feat_code AS feature_code,
        f.groundelev AS ground_elevation,
        '' as complex_type,
        '' as complex,
        '' as oem_name,
        '' as label,
        '' as mas_constrtype,
        ST_AsText(f.the_geom) AS shape
        FROM doitt_buildingfootprints.latest f
        LEFT JOIN dcp_pluto.latest p
        ON f.mpluto_bbl = p.bbl
        WHERE f.bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        AND f.mpluto_bbl IS NOT NULL
        LIMIT 10000;
    '''

    melissa_sql = '''WITH _zips AS (
        SELECT bin, city, zip,
            row_number() OVER (PARTITION BY bin ORDER BY bin, zip) AS row_number
            FROM dcp_melissa."2019/09/25"
            WHERE bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        )

        , _zips_count AS (
            SELECT bin, COUNT(distinct zip) AS zip_count
                FROM dcp_melissa."2019/09/25"
                WHERE bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
                GROUP BY bin
        )

        SELECT a.bin AS bin,
            a.city AS city,
            a.zip AS zip,
        CASE
	       WHEN b.zip_count > 1 THEN '1'
	       ELSE ''
        END AS multzip
        FROM _zips a, _zips_count b
        WHERE a.bin = b.bin
        AND a.row_number = 1;
    '''

    print('Creating BBL index on PLUTO')
    recipe_engine.execute(create_pluto_index_sql)
    print('Creating MPLUTO_BBL index on footprints')
    recipe_engine.execute(create_bf_index_sql)
    print('Creating BIN index on footprints')
    recipe_engine.execute(create_bf_index_bin_sql)

    print('Getting footprints/PLUTO information...')
    df = pd.read_sql(import_sql, recipe_engine)

    if df.empty:
        print('No records read from footprints / PLUTO join')
        exit()
    else:
        print(df.head())

    print('Getting zip code information from Melissa...')
    df_melissa_zip = pd.read_sql(melissa_sql, edm_data)

    if df_melissa_zip.empty:
        print('No records returned by Melissa zip code query')
        exit()
    else:
        df_melissa_zip['bin'] = df_melissa_zip['bin'].astype(str)
        print(df_melissa_zip.head())

    print('Getting PAD address strings...')
    df_pad = pd.read_csv('output/pad_output.csv')

    if df_pad.empty:
        print('No records read from PAD')
        exit()
    else:
        df_pad['bin'] = df_pad['bin'].astype(str)
        print(df_pad.head())

    print('Getting Melissa address strings...')
    df_melissa = pd.read_csv('output/melissa_output.csv')

    if df_melissa.empty:
        print('No records read from Melissa')
        exit()
    else:
        df_melissa['bin'] = df_melissa['bin'].astype(str)
        print(df_melissa.head())

    print('Getting OEM address strings...')
    df_oem_bc = pd.read_csv('input/oem_bc.csv', 
        usecols=['BIN', 'COMPLEX_TYPE', 'COMPLEX', 'OEM_NAME', 'ConstrType'],
        dtype={
            'BIN': 'str',
            'COMPLEX_TYPE': 'str',
            'COMPLEX': 'str',
            'OEM_NAME': 'str',
            'ConstrType': 'str'
    })

    if df_oem_bc.empty:
        print('No records read from OEM Building Composite')
        exit()
    else:
        print(df_oem_bc.head())

    df['label'] = ''

    print('Merging footprints/PLUTO with PAD data...')
    with_pad = pd.merge(df,
            df_pad[['bin', 'pad_addr']],
            on='bin',
            how='left')

    del df, df_pad
    gc.collect()

    print(with_pad.head())

    print('Merging footprints/PLUTO/PAD data with Melissa USPS address...')
    with_melissa = pd.merge(with_pad,
            df_melissa[['bin', 'usps_addr']],
            on='bin',
            how='left')

    del with_pad, df_melissa
    gc.collect()

    print(with_melissa.head())

    print('Merging footprints/PLUTO/PAD/Melissa data with Melissa zip code info...')
    with_melissa_zip = pd.merge(with_melissa,
            df_melissa_zip[['bin', 'city', 'zip', 'multzip']],
            on='bin',
            how='left')

    del with_melissa
    gc.collect()

    print(with_melissa_zip.head())

    print('Merging footprints/PLUTO/PAD/Melissa data with OEM Buildings Composite info...')
    df_final = pd.merge(with_melissa_zip,
            df_oem_bc[['BIN', 'COMPLEX_TYPE', 'COMPLEX', 'OEM_NAME', 'ConstrType']],
            right_on='BIN',
            left_on='bin',
            how='left')

    del with_melissa_zip, df_oem_bc
    gc.collect()

    print(df_final.head())

    print('Setting label and initializing Nan values...')
    #with_melissa_zip = with_melissa_zip.replace(np.nan, '', regex=True)
    df_final.loc[df_final['usps_addr'].isnull(), 'label'] = df_final['pad_addr']
    df_final.loc[df_final['label'] != df_final['pad_addr'], 'label'] = df_final['usps_addr']

    print('Dumping output to CSV...')
    #fieldnames = ['bbl', 'bbl_text', 'bin', 'doitt_id', 'cd', 'bldgclass', 'landuse',
    #'ownername', 'ownertype', 'numbldgs', 'numfloors', 'lotarea', 'unitsres', 'unitstotal',
    #'bsmtcode', 'proxcode', 'lottype', 'yearbuilt', 'yearalter1', 'yearalter2', 'borocode',
    #'height_roof', 'last_status_type', 'shape', 'complex_type', 'complex', 'oem_name', 'label',
    #'pad_addr', 'usps_addr', 'city', 'zip', 'multzip', 'mas_constrtype', 'bctcb',
    #'feature_code', 'ground_elevation']
    #df_reorder = with_melissa_zip[fieldnames] # rearrange column here
    df_final.to_csv('output/buildings_composite.csv', index=False)
