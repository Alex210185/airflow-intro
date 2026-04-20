import pandas as pd
from extraction.validate import validate_customers, validate_items

def test_validate_customers():
    df = pd.DataFrame({"State": ["CA", "NY", "CA"]})
    valid, invalid = validate_customers(df)
    assert len(valid) == 2
    assert len(invalid) == 1

def test_validate_items():
    df = pd.DataFrame({"CustomerID": [1, None, 2]})
    valid, invalid = validate_items(df)
    assert len(valid) == 2
    assert len(invalid) == 1
