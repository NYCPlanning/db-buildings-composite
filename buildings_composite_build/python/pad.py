from helper.engines import recipe_engine, edm_engine, build_engine
from helper.sort_addresses import hnum_sort_and_list, sort_and_list
from helper.exporter import exporter
import pandas as pd
import re
import os

def hnum_sort_and_list(street_df):
    # Sort number addresses, join, and append street name
    sorted_hnum = street_df.sort_values('hnum')
    hnum_list = sorted_hnum['hnum'].unique().tolist()
    address_string = ','.join(hnum_list)
    
    return address_string

def sort_and_list(bin_df):
    # Temporarily modify street name field before alphabetizing
    bin_df.loc[bin_df['hnums'] == '', 'stname'] = 'ZZZ' + bin_df['stname']
    sorted_bin = bin_df.sort_values('stname')
    
    # Remove modifications and join
    sorted_bin['stname'] = sorted_bin['stname'].str.replace('ZZZ','')
    address_list = sorted_bin['full_address'].unique().tolist()
    address_string = '::'.join(address_list)
    
    return address_string

if __name__ == '__main__':
    # Get the PAD records from recipes, filtering out "million" BINs.
    import_sql = '''SELECT boro, block, lot, bin, lhnd, hhnd, stname
        FROM dcp_pad.latest
        WHERE bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        ORDER BY boro, block, lot, bin, stname, lhnd;'''
    df = pd.read_sql(import_sql, recipe_engine)

    # Strip extra spaces from house numbers
    df['lhnd'] = df['lhnd'].str.strip()
    df['hhnd'] = df['hhnd'].str.strip()
    df['stname'] = df['stname'].str.strip()

    # Concatenate house num ranges
    df.loc[df['hhnd'] == df['lhnd'], 'hnum'] = df['lhnd']
    df.loc[df['hhnd'] != df['lhnd'], 'hnum'] = df['lhnd'] + " - " + df['hhnd']
    df.loc[df['hhnd'] == '', 'hnum'] = df['lhnd']
    print("Created house numbers: ", df.head())

    # Create full address field
    df_by_street = df.groupby(['boro','block','lot','bin','stname']).apply(hnum_sort_and_list).reset_index().rename(columns={0:'hnums'})
    df_by_street.loc[df_by_street['hnums'] != '', 'full_address'] = df_by_street['hnums'] + ' ' + df_by_street['stname']
    df_by_street.loc[df_by_street['hnums'] == '', 'full_address'] = df_by_street['stname']
    print("Created full addresses: ", df_by_street.head())

    df_by_bin = df_by_street.groupby(['boro','block','lot','bin']).apply(sort_and_list).reset_index().rename(columns={0:'pad_addr'})
    print("Created BIN-specific strings: ", df_by_bin.head())

    # Export formatted PAD to postgres
    
    output_table = "dcp_pad_formatted.\"2020\""
    DDL={"boro":"text",
        "block":"text",
        "lot":"text",
        "bin":"text",
        "pad_addr":"text"}

    print("Exporting data...")
    exporter(df=df_by_bin, 
            output_table=output_table, 
            con=build_engine, 
            DDL=DDL)
