"""Datasource builder for data from the swiss Office Federal of Statistics."""

from typing import Any, Optional, cast

import pandas as pd
import requests

from shifter_pandas.wikidata_ import ELEMENT_CANTON_CH, WikidataDatasource


# https://www.bfs.admin.ch/bfs/fr/home/services/recherche/api/api-pxweb.html
class OFSDatasource:
    """Datasource builder for data from the swiss Office Federal of Statistics."""

    def __init__(self, url: str) -> None:
        """Initialize the datasource builder."""
        self.url = url
        self.wdds = WikidataDatasource()

    def metadata(self) -> dict[str, Any]:
        """Get the metadata."""
        response = requests.get(self.url, timeout=120)
        if not response.ok:
            print(f"Error on query {self.url}: {response.status_code}")
            print(response.text)
            response.raise_for_status()
        return cast(dict[str, Any], response.json())

    def datasource(
        self,
        query: dict[str, Any],
        wikidata_dimension: Optional[str] = None,
        wikidata_id: bool = False,
        wikidata_name: bool = False,
        wikidata_properties: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Get the Datasource as DataFrame."""

        if wikidata_properties is None:
            wikidata_properties = []
        wikidata = wikidata_id or wikidata_name or wikidata_properties

        response = requests.post(self.url, json=query, timeout=120)
        if not response.ok:
            print(f"Error on query {self.url}: {response.status_code}")
            print(response.text)
            response.raise_for_status()

        json = response.json()

        values = {"values": json["dataset"]["value"]}
        length = 1
        total_length = len(json["dataset"]["value"])
        for dimension_id in json["dataset"]["dimension"]["id"]:
            dimension = json["dataset"]["dimension"][dimension_id]

            current_length = len(dimension["category"]["index"])
            number = int(total_length / (length * current_length))

            dimension_value = list(json["dataset"]["value"])
            for index_x in range(length):
                for index_y, value in enumerate(dimension["category"]["label"].values()):
                    for index_z in range(number):
                        dimension_value[
                            index_x * current_length * number + index_y * number + index_z
                        ] = value

            values[dimension["label"]] = dimension_value

            length *= current_length

        if wikidata and wikidata_dimension:

            def _get_values(canton: str) -> dict[str, Any]:
                element_ids = self.wdds.get_from_alias(ELEMENT_CANTON_CH, canton)

                element = {}
                if element_ids:
                    element.update(
                        self.wdds.get_item(
                            element_ids[0]["id"],
                            with_name=wikidata_name,
                            properties=wikidata_properties,
                            with_id=wikidata_id,
                            prefix="Wikidata",
                        )
                    )

                return element

            data = [_get_values(canton) for canton in values[wikidata_dimension]]

            if wikidata_id:
                values["WikidataId"] = [item.get("WikidataId") for item in data]
            if wikidata_name:
                values["WikidataName"] = [item.get("Name") for item in data]
            for wikidata_property in wikidata_properties:
                wikidata_property_name = self.wdds.get_property_name(wikidata_property)
                values[wikidata_property_name] = [item.get(wikidata_property_name) for item in data]

        return pd.DataFrame(values)
