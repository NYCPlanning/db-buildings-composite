from helper.engines import recipe_engine, edm_engine, build_engine
from helper.sort_addresses import hnum_sort_and_list, sort_and_list, apply_parallel
import pandas as pd
from multiprocessing import Pool, cpu_count
import numpy as np
import re
import os

def load_bin_chunk(bin):
    # Get Melissa data from EDM for a unique, valid bin
    import_sql = f'''SELECT borough_code AS boro,
        substring(bbl,2,5) AS block,
        substring(bbl,7,4) AS lot,
        bin,
        f1_normalized_hn AS hnum,
        f1_normalized_sn AS stname
        FROM dcp_melissa."2019/09/25"
        WHERE f1_grc in ('00', '01')
        AND bin = '{bin}';'''
    df = pd.read_sql(import_sql, edm_engine)
    df = df.replace([None], '')

    # Strip extra spaces from house numbers
    df['hnum'] = df['hnum'].str.strip()
    df['stname'] = df['stname'].str.strip()
    
    return df

def sort_single_bin(df):
    # Create full address field
    df_by_street = df.groupby(['boro','block','lot','bin','stname']).apply(hnum_sort_and_list).reset_index()
    df_by_street = df_by_street.rename(columns={0:'hnums'})
    df_by_street.loc[df_by_street['hnums'] != '', 'full_address'] = df_by_street['hnums'] + ' ' + df_by_street['stname']
    df_by_street.loc[df_by_street['hnums'] == '', 'full_address'] = df_by_street['stname']
    

    # Sort full addresses
    df_by_bin = df_by_street.groupby(['boro','block','lot','bin']).apply(sort_and_list).reset_index()
    df_by_bin = df_by_bin.rename(columns={0:'usps_addr'})
    print(df_by_bin.head())
    return df_by_bin

def load_and_sort(bin):
    raw_df = load_bin_chunk(bin)
    sorted_df = sort_single_bin(raw_df)
    return sorted_df


if __name__ == '__main__':
    # Import table of unique BIN-street combos
    import_sql = '''SELECT DISTINCT bin
        FROM dcp_melissa."2019/09/25"
        WHERE f1_grc in ('00', '01')
        AND bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        AND borough_code BETWEEN '1' AND '5';'''
    unique_df = pd.read_sql(import_sql, edm_engine)
    unique_records = unique_df['bin'].tolist()
    print("Loaded unique BINS: ", len(unique_records))

    # Load and process BINs in parallel
    print('Loading and sorting BINs...')
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(load_and_sort, unique_records, 10000)

    results_df = pd.DataFrame(it)
    print(results_df.head())

    results_df.to_csv('output/melissa_output.csv')

