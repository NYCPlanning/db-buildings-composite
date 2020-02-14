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
    grouped = df.groupby(['boro','block','lot','bin','stname'])
    print(len(df_by_street))
    df_by_street = grouped.apply(hnum_sort_and_list)
    df_by_street = df_by_street.reset_index()
    df_by_street = df_by_street.rename(columns={0:'hnums'})
    print(df_by_street.head())
    df_by_street.loc[df_by_street['hnums'] != '', 'full_address'] = df_by_street['hnums'] + ' ' + df_by_street['stname']
    df_by_street.loc[df_by_street['hnums'] == '', 'full_address'] = df_by_street['stname']

    # Sort full addresses
    df_by_bin = df_by_street.groupby(['boro','block','lot','bin']).apply(sort_and_list)
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




    '''
    # Initialize work areas

    boro_hold = df['boro'].iloc[0]
    block_hold = df['block'].iloc[0]
    lot_hold = df['lot'].iloc[0]
    bin_hold = df['bin'].iloc[0]
    stname_hold = df['stname'].iloc[0]
    no_street_number = []
    street_numbers = []
    addresses = []
    c1 = ","
    c2 = '::'
    input_ctr = 0
    output_ctr = 0
    bin_ctr = 1

    # Open the output CSV, loop through the data and roll up addresses for each BIN

    with open('output/melissa_output.csv', mode='w') as melissa_output:
        melissa_output_writer = csv.writer(melissa_output, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        melissa_output_writer.writerow(["boro", "block", "lot", "bin", "usps_addr"])

        for index, row in df.iterrows():

            input_ctr += 1
            if input_ctr % 10000 == 0:
                print('Records processed {}'.format(input_ctr))

            if row['bin'] != bin_hold:

            # Write the record for the last BIN to the output dataframe

                if len(street_numbers) > 0:
                    if bin_ctr == 1:
                        stname_hold = '({})'.format(stname_hold.strip())
                    #addresses.append(c1.join(sorted(street_numbers)) + " " + stname_hold.strip())
                    addresses.append(c1.join(sorted(street_numbers)) + " " + " ".join(stname_hold.split()))

                if len(addresses) == 0 and len(no_street_number) == 1:
                    no_street_number[0] = '({})'.format(no_street_number[0])

                street_string = c2.join([*addresses, *no_street_number])

                melissa_output_writer.writerow([boro_hold, block_hold, lot_hold, bin_hold, street_string])

                output_ctr += 1
                bin_ctr = 0

                # Now start rolling up addresses for the new BIN

                boro_hold = row['boro']
                block_hold = row['block']
                lot_hold = row['lot']
                bin_hold = row['bin']
                stname_hold = row['stname']
                no_street_number = []
                street_numbers = []
                addresses = []
                street_string = ''
                bin_ctr += 1

                street_numbers, no_street_number = build_address_string(row, street_numbers, no_street_number)

            else:

                # BIN is the same. But if the street number has changed, format the old one by combining
                # the street numbers with the name, and reinitialize the street numbers list.

                if row['stname'] != stname_hold:

                    if len(street_numbers) > 0:
                        #addresses.append(c1.join(sorted(street_numbers)) + " " + stname_hold.strip())
                        addresses.append(c1.join(sorted(street_numbers)) + " " + " ".join(stname_hold.split()))
                        street_numbers = []

                    stname_hold = row['stname']
                    bin_ctr += 1

                street_numbers, no_street_number = build_address_string(row, street_numbers, no_street_number)

        # Process the last record

        if len(street_numbers) > 0:
            if bin_ctr == 1:
                stname_hold = '({})'.format(stname_hold.strip())
                #addresses.append(c1.join(sorted(street_numbers)) + " " + stname_hold.strip())
                addresses.append(c1.join(sorted(street_numbers)) + " " + " ".join(stname_hold.split()))

        street_string = c2.join([*addresses, *no_street_number])

        melissa_output_writer.writerow([boro_hold, block_hold, lot_hold, bin_hold, street_string])
        output_ctr += 1

        print('Records read and processed {}'.format(input_ctr))
        print('Records written {}'.format(output_ctr))
'''