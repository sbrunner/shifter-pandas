"""Datasource builder for data from World Bank."""

import csv
import io
import re
from typing import Any, Dict, List, Optional, cast
from zipfile import ZipFile

import pandas as pd

from shifter_pandas import standardize_property
from shifter_pandas.wikidata_ import ELEMENT_COUNTRY, WikidataDatasource


class WorldbankDatasource:
    """Datasource builder for data from World Bank."""

    def __init__(self, zip_filename: str) -> None:
        """Initialize the datasource builder."""
        self.wdds = WikidataDatasource()
        with ZipFile(zip_filename) as myzip:
            with myzip.open(zip_filename[:-4] + ".csv") as csvfile:
                self.table = list(
                    csv.reader(io.TextIOWrapper(csvfile, encoding=None), delimiter=",", quotechar='"')
                )

    def datasource(
        self,
        wikidata_id: bool = False,
        wikidata_name: bool = False,
        wikidata_properties: Optional[List[str]] = None,
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

        data: Dict[str, List[Any]] = {"Year": [], "Value": []}
        for index_y, header in headers:
            data[header] = []
        for row in self.table[5:]:

            for index_y, year in years:
                value = row[index_y]
                if value:
                    country_name = None
                    for index_y2, header in headers:
                        data[header].append(row[index_y2])
                        if header == "CountryName":
                            country_name = row[index_y2]
                    data["Year"].append(year)
                    data["Value"].append(float(row[index_y]))

                    if wikidata:
                        element_ids = self.wdds.get_from_alias(ELEMENT_COUNTRY, cast(str, country_name))
                        item = (
                            self.wdds.get_item(
                                element_ids[0]["id"],
                                with_name=wikidata_name,
                                with_id=wikidata_id,
                                properties=wikidata_properties,
                                prefix="Wikidata",
                            )
                            if element_ids
                            else {}
                        )

                        if wikidata_id:
                            data.setdefault("WikidataId").append(item.get("WikidataId"))
                        if wikidata_name:
                            data.setdefault("WikidataName").append(item.get("Name"))
                        for wikidata_property in wikidata_properties:
                            wikidata_property_name = self.wdds.get_property_name(wikidata_property)
                            data.setdefault(wikidata_property_name).append(item.get(wikidata_property_name))

        return pd.DataFrame(data)
