
from cook import Importer
import os
import pandas as pd

RECIPE_ENGINE = os.environ.get('RECIPE_ENGINE', '')
BUILD_ENGINE=os.environ.get('BUILD_ENGINE', '')
EDM_DATA = os.environ.get('EDM_DATA', '')


def ETL():
    # Import data from recipes:

    recipe_importer = Importer(RECIPE_ENGINE, BUILD_ENGINE)
    # Import OEM Building Composite
    recipe_importer.import_table(schema_name='em_buildings_composite', version="2020/02/19")
    # Import DOITT building footprints
    recipe_importer.import_table(schema_name='doitt_buildingfootprints', version="2019/11/29")
    # Import DCP Pluto
    recipe_importer.import_table(schema_name='dcp_pluto', version="19v2")

    # Import data from EDM:

    edm_importer = Importer(EDM_DATA, BUILD_ENGINE)
    # Import Melissa
    edm_importer.import_table(schema_name='dcp_melissa', version="2019/09/25")

    # Address look-ups for Melissa and PAD are already in BUILD



def old_building_composite():
    importer = Importer(EDM_DATA, BUILD_ENGINE)
    importer.import_table(schema_name='dcp_building_composite', version="")

if __name__ == "__main__":
    ETL()
    #old_building_composite()