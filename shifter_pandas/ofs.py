"""Datasource builder for data from the swiss Office Federal of Statistics."""

from typing import Any, Dict, cast

import pandas as pd
import requests


# https://www.bfs.admin.ch/bfs/fr/home/services/recherche/api/api-pxweb.html
class OFSDatasource:
    """Datasource builder for data from the swiss Office Federal of Statistics."""

    def __init__(self, url: str) -> None:
        """Initialize the datasource builder."""
        self.url = url

    def metadata(self) -> Dict[str, Any]:
        """Get the metadata."""
        response = requests.get(self.url)
        if not response.ok:
            print(f"Error on query {self.url}: {response.status_code}")
            print(response.text)
            response.raise_for_status()
        return cast(Dict[str, Any], response.json())

    def datasource(self, query: Dict[str, Any]) -> pd.DataFrame:
        """Get the Datasource ad DataFrame."""
        response = requests.post(self.url, json=query)
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

        return pd.DataFrame(values)
