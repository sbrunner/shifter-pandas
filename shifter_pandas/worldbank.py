"""Datasource builder for data from World Bank."""

import csv
import io
import os
import re
from typing import Any, Optional
from zipfile import ZipFile

import pandas as pd

from shifter_pandas import standardize_property
from shifter_pandas.wikidata_ import WikidataDatasource


class WorldbankDatasource:
    """Datasource builder for data from World Bank."""

    def __init__(self, zip_filename: str) -> None:
        """Initialize the datasource builder."""
        self.wdds = WikidataDatasource()
        self.wdds.set_alias("World", "WLD", "Q16502", "World")
        with ZipFile(zip_filename) as myzip:
            with myzip.open(os.path.splitext(os.path.basename(zip_filename))[0] + ".csv") as csvfile:
                self.table = list(
                    csv.reader(io.TextIOWrapper(csvfile, encoding=None), delimiter=",", quotechar='"')
                )

    def datasource(
        self,
        wikidata_id: bool = False,
        wikidata_name: bool = False,
        wikidata_type: bool = False,
        wikidata_properties: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Get the Datasource as DataFrame."""

        if wikidata_properties is None:
            wikidata_properties = []
        wikidata = wikidata_id or wikidata_name or wikidata_properties

        year_re = re.compile(r"^[0-9]{4}$")
        headers = [
            (e[0], standardize_property(e[1]))
            for e in enumerate(self.table[4])
            if e[1] and year_re.match(e[1]) is None
        ]
        years = [(e[0], int(e[1])) for e in enumerate(self.table[4]) if year_re.match(e[1]) is not None]

        data: dict[str, list[Any]] = {"Year": [], "Value": []}
        for index_y, header in headers:
            data[header] = []
        for row in self.table[5:]:
            for index_y, year in years:
                value = row[index_y]
                if value:
                    country_name = None
                    for index_y2, header in headers:
                        data[header].append(row[index_y2])
                        if header == "CountryCode":
                            country_name = row[index_y2]
                    data["Year"].append(year)
                    data["Value"].append(float(row[index_y]))

                    if wikidata:
                        assert country_name is not None
                        element_id = self.wdds.get_region(country_name)
                        if wikidata_type:
                            data.setdefault("WikidataType", []).append(
                                element_id["type"] if element_id else None
                            )
                        item = self.wdds.get_item(
                            element_id["id"] if element_id else None,
                            with_name=wikidata_name,
                            with_id=wikidata_id,
                            properties=wikidata_properties,
                            prefix="Wikidata",
                        )
                        for key, value in item.items():
                            data.setdefault(key, []).append(value)
        return pd.DataFrame(data)
