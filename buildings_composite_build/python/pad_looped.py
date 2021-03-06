from sqlalchemy import create_engine
from helper.engines import build_engine, psycopg2_connect
import psycopg2
import pandas as pd
import os
import io
import csv

def build_address_string(row, street_numbers, no_street_number):
    # lhnd and hhnd come in with extra spaces, hence the stripping
    lhnd = row['lhnd'].strip()
    hhnd = row['hhnd'].strip()
    stname = row['stname'].strip()

    if len(lhnd) == 0 and len(hhnd) == 0:
        #no_street_number.append(stname)
        no_street_number.append(" ".join(stname.split()))
    else:
        if lhnd == hhnd:
            street_numbers.append(lhnd)
        else:
            if len(hhnd) == 0:
                street_numbers.append(lhnd)
            else:
                street_numbers.append(lhnd + " - " + hhnd)

    return street_numbers, no_street_number

if __name__ == '__main__':
    # Connect to postgres db
    recipe_engine = create_engine(os.environ['RECIPE_ENGINE'])
    engine = create_engine(os.environ['BUILD_ENGINE'])

    
    # Get the PAD records from the database. Filter out "million" BINs.

    import_sql = '''SELECT boro, block, lot, bin, lhnd, hhnd, stname
        FROM dcp_pad.latest
        WHERE bin NOT IN ('1000000', '2000000', '3000000', '4000000', '5000000')
        ORDER BY boro, block, lot, bin, stname, lhnd;'''

    df = pd.read_sql(import_sql, recipe_engine)

    if df.empty:
        print('No records on PAD table read')
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

    with open('output/pad_output.csv', mode='w') as pad_output:
        pad_output_writer = csv.writer(pad_output, delimiter='$')
        pad_output_writer.writerow(["boro", "block", "lot", "bin", "pad_addr"])

        for index, row in df.iterrows():

            input_ctr += 1
            if input_ctr % 10000 == 0:
                print('Records processed {}'.format(input_ctr))

            if row['bin'] != bin_hold:

            # Write the record for the last BIN to the output dataframe

                if len(street_numbers) > 0:
                    if bin_ctr == 1 and len(street_numbers) == 1:
                        stname_hold = '({})'.format(stname_hold.strip())
                    if bin_ctr == 1 and len(street_numbers) > 1:
                        stname_hold = stname_hold.strip()
                    #addresses.append(c1.join(sorted(street_numbers)) + " " + stname_hold.strip())
                    addresses.append(c1.join(sorted(street_numbers)) + " " + " ".join(stname_hold.split()))

                if len(addresses) == 0 and len(no_street_number) == 1:
                    no_street_number[0] = '({})'.format(no_street_number[0])

                street_string = c2.join([*addresses, *no_street_number])

                pad_output_writer.writerow([boro_hold, block_hold, lot_hold, bin_hold, street_string])

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

        pad_output_writer.writerow([boro_hold, block_hold, lot_hold, bin_hold, street_string])
        output_ctr += 1

        print('Records read and processed {}'.format(input_ctr))
        print('Records written {}'.format(output_ctr))
    
    with open('output/pad_output.csv', mode='r') as pad_output:
        output_table = "dcp_pad_formatted.\"2020\""
        DDL={"boro":"text",
            "block":"text",
            "lot":"text",
            "bin":"text",
            "pad_addr":"text"}

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
        db_cursor.copy_from(pad_output, output_table, sep='$', null='')
        db_cursor.connection.commit()

        str_buffer.close()
        db_cursor.close()
        db_connection.close()