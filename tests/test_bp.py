"""
Tests of BP Datasource.
"""


from shifter_pandas.bp import UNITS_ENERGY, BPDatasource


def test_bp() -> None:
    """
    Tests of BP Datasource.
    """

    shifter_ds = BPDatasource("tests/bp-stats-review-2021-all-data.xlsx")
    metadata = shifter_ds.metadata()
    assert metadata[1]["regions"][:5] == [
        {"index": 5, "label": "Canada"},
        {"index": 6, "label": "Mexico"},
        {"index": 7, "label": "US"},
        {"index": 8, "label": "Total North America"},
        {"index": 10, "label": "Argentina"},
    ]
    del metadata[1]["regions"]
    assert metadata[1]["years"][:5] == [
        {"index": 2, "label": 1965},
        {"index": 3, "label": 1966},
        {"index": 4, "label": 1967},
        {"index": 5, "label": 1968},
        {"index": 6, "label": 1969},
    ]
    del metadata[1]["years"]

    assert metadata[3]["regions"][:2] == [
        {"index": 6, "label": "Mexico"},
        {"index": 7, "label": "US"},
    ]
    del metadata[3]["regions"]
    assert metadata[3]["years"][:2] == [
        {"index": 2, "label": 1975},
        {"index": 3, "label": 1980},
    ]
    del metadata[3]["years"]

    assert metadata == [
        {"index": 0, "supported": False, "type": "Contents"},
        {
            "index": 1,
            "label": "Primary Energy Consumption",
            "row_index": 3,
            "supported": True,
            "type": "Primary Energy Consumption",
            "unit": {
                "iso": "J",
                "iso_factor": 1000000000000000000,
                "iso_postfix": "",
                "normalized": "exajoules",
                "original": "Exajoules",
            },
        },
        {"index": 2, "supported": False, "type": "Cobalt and Lithium - Prices"},
        {
            "index": 3,
            "label": "Geothermal Capacity",
            "row_index": 4,
            "supported": True,
            "type": "Geothermal Capacity",
            "unit": {
                "iso": "W",
                "iso_factor": 1000000,
                "iso_postfix": "",
                "normalized": "megawatts",
                "original": "Megawatts",
            },
        },
        {"index": 4, "supported": False, "type": "Approximate conversion factors"},
    ]

    assert shifter_ds.to_iso_unit == {
        "lb": {"unit": "tonnes", "factor": 2204.62},
        "short tons": {"unit": "tonnes", "factor": 1.1023},
        "barrels": {"unit": "m³", "factor": 6.2898},
        "litres": {"unit": "m³", "factor": 1000},
        "kilocalorie": {"unit": "J", "factor": 0.0041841004184100415},
        "kcal": {"unit": "J", "factor": 0.0041841004184100415},
        "calorie": {"unit": "J", "factor": 4.184100418410042},
        "cal": {"unit": "J", "factor": 4.184100418410042},
        "Btu": {"unit": "J", "factor": 0.0010548523206751054},
        "barrel of oil equivalent": {"unit": "j", "factor": 6119000},
        "kilowatt-hour": {"unit": "J", "factor": 3.6},
        "kilowatt-hours": {"unit": "J", "factor": 3.6},
        "kWh": {"unit": "J", "factor": 3.6},
        "watt-hour": {"unit": "J", "factor": 3600},
        "watt-hours": {"unit": "J", "factor": 3600},
        "Wh": {"unit": "J", "factor": 3600},
        "cubic meters": {"unit": "m³", "factor": 1},
        "meters": {"unit": "m", "factor": 1},
        "joules": {"unit": "J", "factor": 1},
        "cubic feets": {"unit": "m³", "factor": 35.3146667},
        "cubic meter": {"unit": "m³", "factor": 1},
        "meter": {"unit": "m", "factor": 1},
        "joule": {"unit": "J", "factor": 1},
        "cubic feet": {"unit": "m³", "factor": 35.3146667},
        "watts": {"unit": "W", "factor": 1},
        "watt": {"unit": "W", "factor": 1},
        "us gallons": {"unit": "m³", "factor": 0.14969724},
        "gallons": {"unit": "m³", "factor": 0.14969724},
    }

    data_frame = shifter_ds.datasource(regions_filter=["Total World"], years_factor=20)
    assert set(data_frame.Year) == {1980, 2000, 2020}
    assert set(data_frame.Region) == {"Total World"}
    assert set(data_frame.TypeUnit) == {"Geothermal Capacity [W]", "Primary Energy Consumption [J]"}
    assert set(data_frame.Type) == {"Geothermal Capacity", "Primary Energy Consumption"}
    assert set(data_frame.Unit) == {"W", "J"}

    data_frame = shifter_ds.datasource(regions_filter=["Total World", "Switzerland"], years_factor=50)
    assert set(data_frame.Region) == {"Total World", "Switzerland"}

    data_frame = shifter_ds.datasource(
        regions_filter=["Total World", "Switzerland"], years_filter=[1935, 1945]
    )
    assert set(data_frame.Year) == set()

    data_frame = shifter_ds.datasource(
        regions_filter=["Total World", "Switzerland"], years_filter=[1985, 1993]
    )
    assert set(data_frame.Year) == {1985, 1993}

    data_frame = shifter_ds.datasource(types_filter=["Geothermal Capacity"], years_factor=50)
    assert set(data_frame.Type) == {"Geothermal Capacity"}

    data_frame = shifter_ds.datasource(units_filter=UNITS_ENERGY, years_factor=50)
    assert set(data_frame.Type) == {"Primary Energy Consumption"}
