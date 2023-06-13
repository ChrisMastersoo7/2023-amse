import pytest
import unittest.mock as mock
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from pipeline import *
import sqlite3

@pytest.mark.system_test
def test_sql():
    assert os.path.exists("./project/data/hamburg.sqlite") == True
    assert os.path.exists("./project/data/tirol.sqlite") == True

@pytest.fixture
def input_test_pipline():
   input_test_pipline = Pipeline('test') 
   return input_test_pipline

@pytest.mark.component_test
def test_constructor():
    test_pipeline = Pipeline('test')
    assert os.path.isdir("./project/data/tmp") == True
    assert os.path.isdir("./project/data/tmp/test") == True
    
@pytest.mark.component_test
def test_create_database(input_test_pipline):
    test_df = pd.DataFrame([[1, 2, 3], [3, 4, 5]], columns=['a', 'b', 'c'])
    input_test_pipline.create_database(test_df)
    assert os.path.exists("./project/data/test.sqlite") == True
    conn = sqlite3.connect('./project/data/test.sqlite')
    result = pd.read_sql_query("SELECT * FROM test", conn)
    conn.close()
    
    assert_frame_equal(result, test_df)

@pytest.mark.mock_test
def test_mock_database(input_test_pipline):
    test_df = pd.DataFrame([[1, 2, 3], [3, 4, 5]], columns=['a', 'b', 'c'])
    
    with mock.patch.object(test_df, "to_sql") as to_sql_mock:
        input_test_pipline.create_database(test_df)
        to_sql_mock.assert_called_once()
        
@pytest.mark.integration_test
def test_extract_zip(input_test_pipline):
    test_zip_url = "https://gis.tirol.gv.at/ogd/sport_freizeit/TW_BikeTrailTirol_Einzeletappen.zip"
    input_test_pipline.extract_zip(test_zip_url)
    test_df = input_test_pipline.convert_from_gpx_to_df()
    assert os.path.exists("./project/data/tmp/tirol/TW_BikeTrailTirol_Einzeletappen.zip") == True
    assert test_df is not None

@pytest.mark.integration_test
def test_extract_from_GeoJSON_to_df(input_test_pipline):
    test_geoJSON_url = "https://api.hamburg.de/datasets/v1/freizeitrouten/collections/freizeitroute1/items?bulk=true&f=json"
    test_df = input_test_pipline.extract_from_GeoJSON_to_df(test_geoJSON_url)
    assert os.path.exists("./project/data/tmp/hamburg/freizeitroute1.json") == True
    assert test_df is not None

    

