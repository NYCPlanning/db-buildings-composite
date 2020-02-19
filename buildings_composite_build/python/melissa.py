from helper.engines import recipe_engine, edm_engine, build_engine
from helper.sort_addresses import hnum_sort_and_list, sort_and_list
from helper.exporter import exporter
import pandas as pd
import numpy as np
import re
import os

if __name__ == '__main__':
    # Import table of unique BIN-street combos
    import_sql = '''SELECT borough_code AS boro,
        substring(bbl,2,5) AS block,
        substring(bbl,7,4) AS lot,
        bin,
        f1_normalized_hn AS hnum,
        f1_normalized_sn AS stname
        FROM dcp_melissa."2019/09/25"
        WHERE f1_grc in ('00', '01')
        AND bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        AND borough_code BETWEEN '1' AND '5'
        GROUP BY boro, block, lot, bin, f1_normalized_sn, f1_normalized_hn
        ORDER BY boro, block, lot, bin, stname, hnum;'''

    df = pd.read_sql(import_sql, edm_engine)
    df = df.replace([None], '')

    # Strip extra spaces from house numbers & street names
    df['hnum'] = df['hnum'].str.strip()
    df['stname'] = df['stname'].str.strip()
    df['stname'] = df['stname'].str.replace('\s+',' ', regex=True)

    # Create full address field
    df_by_street = df.groupby(['boro','block','lot','bin','stname']).apply(hnum_sort_and_list).reset_index()
    df_by_street = df_by_street.rename(columns={0:'hnums'})
    df_by_street.loc[df_by_street['hnums'] != '', 'full_address'] = df_by_street['hnums'] + ' ' + df_by_street['stname']
    df_by_street.loc[df_by_street['hnums'] == '', 'full_address'] = df_by_street['stname']
    print("Created full addresses: \n", df_by_street.head())

    df_by_bin = df_by_street.groupby('bin').apply(sort_and_list).reset_index().rename(columns={0:'usps_addr'})
    print("Created BIN-specific strings: \n", df_by_bin.head())

    # Export formatted Melissa to postgres
    output_table = "dcp_melissa_formatted.\"2020\""
    DDL={"boro":"text",
        "block":"text",
        "lot":"text",
        "bin":"text",
        "usps_addr":"text"}

    print("Exporting data...")
    exporter(df=df_by_bin, 
            output_table=output_table, 
            con=build_engine, 
            DDL=DDL)


