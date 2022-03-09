# https://data.worldbank.org/indicator/NY.GDP.MKTP.KD

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
    assert set(data_field.WikidataType) == {None, "country", "World"}
    assert set(data_field.WikidataId) == {None, "Q16502", "Q30", "Q39"}
    assert set(data_field.WikidataName) == {None, "United States of America", "world", "Switzerland"}
    assert set(data_field.WikidataIso3166_1Alpha_2Code) == {None, "US", "CH"}
