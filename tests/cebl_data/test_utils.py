import pandas as pd

from cebl_data.utils import clean_strings, tidy_whitespace, unescape


def test_unescape_decodes_entities():
    assert unescape("D&#039;Andre") == "D'Andre"


def test_unescape_decodes_double_escaped_entities():
    assert unescape("Tre&amp;#039;darius") == "Tre'darius"


def test_unescape_passes_non_strings_through():
    assert unescape(7) == 7
    assert unescape(None) is None


def test_clean_strings_nulls_empty_and_whitespace():
    frame = pd.DataFrame({"name": ["Koby", "", " "], "points": [10, 5, 3]})
    cleaned = clean_strings(frame)
    assert cleaned["name"].isna().tolist() == [False, True, True]
    assert cleaned["points"].tolist() == [10, 5, 3]


def test_clean_strings_unescapes_string_columns():
    frame = pd.DataFrame({"name": ["D&#039;Andre"]})
    assert clean_strings(frame)["name"].iloc[0] == "D'Andre"


def test_tidy_whitespace_collapses_and_trims():
    assert tidy_whitespace("Chris  Buccella") == "Chris Buccella"
    assert tidy_whitespace(" Tony Turnbull ") == "Tony Turnbull"


def test_tidy_whitespace_passes_non_strings_through():
    assert tidy_whitespace(7) == 7
    assert tidy_whitespace(None) is None


def test_clean_strings_tidies_whitespace_in_string_columns():
    frame = pd.DataFrame({"name": ["Chris  Buccella", "Derek Brown Jr. "]})
    assert clean_strings(frame)["name"].tolist() == [
        "Chris Buccella",
        "Derek Brown Jr.",
    ]
