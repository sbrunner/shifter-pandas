# https://data.worldbank.org/indicator/NY.GDP.MKTP.KD

import pandas as pd

from shifter_pandas.wikidata_ import PROPERTY_ISO_3166_1_ALPHA_2
from shifter_pandas.worldbank import WorldbankDatasource


def test_wikidata() -> None:
    shifter_ds = WorldbankDatasource("tests/API_NY.GDP.MKTP.KD_DS2_en_csv_v2_3630701.zip")
    data_field = shifter_ds.datasource(
        wikidata_id=True,
        wikidata_name=True,
        wikidata_type=True,
        wikidata_properties=[PROPERTY_ISO_3166_1_ALPHA_2],
    ).query("CountryName in ['OECD members', 'Switzerland', 'World', 'European Union', 'Euro area']")
    assert list(data_field.columns) == [
        "Year",
        "Value",
        "CountryName",
        "CountryCode",
        "IndicatorName",
        "IndicatorCode",
        "WikidataType",
        "WikidataId",
        "WikidataName",
        "WikidataIso3166_1Alpha_2Code",
    ]
    assert {x for x in data_field.WikidataType if pd.notna(x)} == {"country", "World"}
    assert any(pd.isna(x) for x in data_field.WikidataType)
    assert {x for x in data_field.WikidataId if pd.notna(x)} == {"Q16502", "Q30", "Q39"}
    assert any(pd.isna(x) for x in data_field.WikidataId)
    assert {x for x in data_field.WikidataName if pd.notna(x)} == {
        "United States of America",
        "world",
        "Switzerland",
    }
    assert any(pd.isna(x) for x in data_field.WikidataName)
    assert {x for x in data_field.WikidataIso3166_1Alpha_2Code if pd.notna(x)} == {"US", "CH"}
    assert any(pd.isna(x) for x in data_field.WikidataIso3166_1Alpha_2Code)
