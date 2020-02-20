from sqlalchemy import create_engine
from helper.engines import build_engine, psycopg2_connect
import psycopg2
import pandas as pd
import os
import io
import csv

def build_address_string(row, street_numbers, no_street_number):
    # lhnd and hhnd come in with extra spaces, hence the stripping
    if row['hnum'] is None:
        hnum = row['hnum']
    else:
        hnum = row['hnum'].strip()

    stname = row['stname'].strip()

    if hnum is None or len(hnum) == 0:
        no_street_number.append(stname)
    else:
        street_numbers.append(hnum)

    return street_numbers, no_street_number

if __name__ == '__main__':
    # Connect to postgres db
    edm_engine = create_engine(os.environ['EDM_DATA'])
    engine = create_engine(os.environ['BUILD_ENGINE'])
   
    # Get the PAD records from the database. Filter out "million" BINs.
    # Grouping because there are dups on this table.

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

    if df.empty:
        print('No records on Melissa table read')
        exit()
    else:
        print(df.head())

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
    if not os.path.exists('output'):
        os.makedirs('output')

    with open('output/melissa_output.csv', mode='w') as melissa_output:
        melissa_output_writer = csv.writer(melissa_output, delimiter='$')
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
    
    with open('output/melissa_output.csv', mode='r') as melissa_output:
        output_table = "dcp_melissa_formatted.\"2020\""
        DDL={"boro":"text",
            "block":"text",
            "lot":"text",
            "bin":"text",
            "usps_addr":"text"}

        # Parse output table
        schema = output_table.split('.')[0]
        version = output_table.split('.')[1].replace('"', '')
        con = build_engine

        # psycopg2 connections
        db_connection = psycopg2_connect(con.url)
        db_cursor = db_connection.cursor()
        str_buffer = io.StringIO() 

        columns = [i.strip('"') for i in DDL.keys()]
        column_definitions = ','.join([f'"{key}" {value}' for key,value in DDL.items()])
        print(column_definitions)
        
        # Create table
        create = f'''
        CREATE SCHEMA IF NOT EXISTS {schema};
        DROP TABLE IF EXISTS {output_table};
        CREATE TABLE {output_table} (
            {column_definitions}
        );
        '''

        con.connect().execute(create)
        con.dispose()

        os.system(f'\necho "exporting table {output_table} ..."')

        # export
        db_cursor.copy_from(melissa_output, output_table, sep='$', null='')
        db_cursor.connection.commit()

        str_buffer.close()
        db_cursor.close()
        db_connection.close()
