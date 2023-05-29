import pytest
import os

def test_sql():
    assert os.path.exists("hamburg.sqlite") == False
    assert os.path.exists("tirol.sqlite") == False