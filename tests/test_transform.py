import pytest
import pandas as pd
from etl.transform import Transformer

@pytest.fixture
def transformer():
    return Transformer(max_title_len=10)

def test_clean_title(transformer):
    assert transformer.clean_title("  hello world  ") == "Hello worl"    # Truncated to 10
    assert transformer.clean_title("") == ""
    assert transformer.clean_title(None) == ""
    assert transformer.clean_title("test") == "Test"

def test_clean_body(transformer):
    assert transformer.clean_body("  test  body \n\n\nwords   ") == "test body words"
    assert transformer.clean_body("") == ""
    assert transformer.clean_body(None) == ""

def test_add_derived_fields(transformer):
    df = pd.DataFrame({"title": ["Test"], "body": ["hello world!"]})
    df2 = transformer.add_derived_fields(df.copy())
    assert "title_length" in df2.columns
    assert "body_word_count" in df2.columns
    assert df2["title_length"].iloc[0] == 4
    assert df2["body_word_count"].iloc[0] == 2

def test_filter_valid(transformer):
    df = pd.DataFrame({
        "id": [1, 2, 2],
        "title": ["Good", "", "Duplicate"],
        "body": ["Body", "Valid", "Body dup"]
    })
    df2 = transformer.filter_valid(df)
    # Only id=1 is unique after filtering invalids, so only row 0 should remain
    assert len(df2) == 2
    assert set(df2['id']) == {1, 2}

def test_transform_pipeline(transformer):
    records = [
        {"userId": 1, "id": 1, "title": "  hello world!    ", "body": "a  b   c"},
        {"userId": 1, "id": 2, "title": "   ", "body": "no title"},   # invalid title
        {"userId": 2, "id": 1, "title": "duplicate", "body": "dup"},  # duplicate id
    ]
    df = transformer.transform(records)
    # Both records with id=1 are duplicates (even after cleaning), so both are dropped. 
    # id=2's title is invalid, dropped.
    assert len(df) == 0