from sqlalchemy import create_engine
import pandas as pd
import os
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

    with open('output/pad_output.csv', mode='w') as pad_output:
        pad_output_writer = csv.writer(pad_output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

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
