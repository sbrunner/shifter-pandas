# https://www.bfs.admin.ch/bfs/fr/home/statistiques/catalogues-banques-donnees/donnees.assetdetail.18904904.html

from shifter_pandas.ofs import OFSDatasource


def test_ofs():
    shifter_ds = OFSDatasource(
        "https://www.pxweb.bfs.admin.ch/api/v1/fr/px-x-0204000000_106/px-x-0204000000_106.px",
    )

    data_field = shifter_ds.datasource(
        {
            "query": [
                {
                    "code": "Wirtschaft und Haushalte",
                    "selection": {"filter": "item", "values": ["1"]},
                },
            ],
            "response": {"format": "json-stat"},
        },
    )
    assert list(data_field.columns) == [
        "values",
        "Unité de mesure",
        "Économie et ménages",
        "Agent énergétique",
        "Année",
    ]
    data_field.rename(columns={"Année": "Year"}, inplace=True)
    assert set(data_field.Year) == {str(e) for e in range(2000, 2024)}
