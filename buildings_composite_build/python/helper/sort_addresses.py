import pandas as pd
from multiprocessing import Pool, cpu_count

def apply_parallel(df_grouped, func):
    with Pool(cpu_count()) as p:
        sorted_group = p.map(func, [group for name, group in df_grouped])
    return pandas.concat(sorted_group)

def hnum_sort_and_list(street_df):
    if street_df.shape[0] == 1:
        return street_df['hnum'][0]
    else:
        # Sort number addresses, join, and append street name
        sorted_hnum = street_df.sort_values('hnum')
        hnum_list = sorted_hnum['hnum'].unique().tolist()
        address_string = ','.join(hnum_list)   
        return address_string

def sort_and_list(bin_df):
    if bin_df.shape[0] == 1:
        return bin_df['full_address'][0]
    else:
        # Temporarily modify street name field before alphabetizing
        bin_df.loc[bin_df['hnums'] == '', 'stname'] = 'ZZZ' + bin_df['stname']
        sorted_bin = bin_df.sort_values('stname')
        
        # Remove modifications and join
        sorted_bin['stname'] = sorted_bin['stname'].str.replace('ZZZ','')
        address_list = sorted_bin['full_address'].unique().tolist()
        address_string = '::'.join(address_list)
        return address_string