# https://data.worldbank.org/indicator/NY.GDP.MKTP.KD
from shifter_pandas.worldbank import WorldbankDatasource


def test_worldbank() -> None:
    shifter_ds = WorldbankDatasource("tests/API_NY.GDP.MKTP.KD_DS2_en_csv_v2_3630701.zip")
    data_field = shifter_ds.datasource()
    assert list(data_field.columns) == [
        "Year",
        "Value",
        "CountryName",
        "CountryCode",
        "IndicatorName",
        "IndicatorCode",
    ]
    assert set(data_field.Year) == set(range(1960, 2021))
