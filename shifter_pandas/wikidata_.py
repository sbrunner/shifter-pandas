"""Datasource builder for data from WikiData."""

import json
import os
import shutil
from typing import Any, Dict, List, Optional, cast

import pandas as pd
import requests
from wikidata.client import Client
import wikidata.entity
import wikidata.quantity

from shifter_pandas import standardize_property

ELEMENT_COUNTRY = "Q6256"
ELEMENT_CONTINENT = "Q5107"
ELEMENT_CANTON_CH = "Q23058"
PROPERTY_INSTANCE_OF = "P31"
PROPERTY_ISO_3166_1_ALPHA_2 = "P297"
PROPERTY_ISO_3166_1_ALPHA_3 = "P298"
PROPERTY_ISO_3166_1_NUMERIC = "P299"
PROPERTY_ISO_3166_2 = "P300"
PROPERTY_ISO_3166_2 = "P300"
PROPERTY_POPULATION = "P1082"


class WikidataDatasource:
    """Datasource builder for data from WikiData."""

    def __init__(self, endpoint_url: str = "https://query.wikidata.org/sparql") -> None:
        """
        Initialize the WikidataDatasource.
        """
        self.endpoint_url = endpoint_url
        self.headers = {"User-Agent": "shifter_pandas - stephane.brunner@gmail.com"}

        if os.path.exists("wikidata-cache.json"):
            with open("wikidata-cache.json", encoding="utf-8") as file:
                self.cache = json.load(file)
        else:
            self.cache = {}
        self.memory_cache: Dict[str, wikidata.entity.Entity] = {}

        self.client = Client()

    def _save_cache(self) -> None:
        with open("wikidata-cache.json.new", "w", encoding="utf-8") as file:
            file.write(json.dumps(self.cache, indent=2))
        shutil.move("wikidata-cache.json.new", "wikidata-cache.json")

    def run_query(self, query: str) -> Dict[str, Any]:
        """
        Run a SPARQL query against the Wikidata SPARQL endpoint.

        Query can be build here: https://query.wikidata.org/querybuilder/
        """
        payload = {
            "query": query,
            "format": "json",
        }
        response = requests.get(self.endpoint_url, params=payload, headers=self.headers)
        if not response.ok:
            print(f"Error on query {self.endpoint_url}: {response.status_code}")
            print(response.text)
            print(query)
            response.raise_for_status()
        return cast(Dict[str, Any], response.json())

    def get_property_name(self, property_id: str) -> str:
        """Get the name of a property."""

        if property_id not in self.cache.get("properties", {}):
            self.cache.setdefault("properties", {})[property_id] = str(
                self._get_item_obj(cast(wikidata.entity.EntityId, property_id)).label
            )
            self._save_cache()
        return cast(str, self.cache["properties"][property_id])

    def set_alias(self, instance_of: str, code: str, item_id: str, label: str = "", lang: str = "en") -> None:
        """
        Set an alias in the cache to prevent unwanted match.
        """
        self.cache.setdefault("fromAlias", {}).setdefault(lang, {}).setdefault(instance_of, {})[code] = [
            {
                "id": item_id,
                "url": f"http://www.wikidata.org/entity/{item_id}",
                "label": label,
            }
        ]

    def get_from_alias(
        self, instance_of: str, code: str, lang: str = "en", limit: int = 10
    ) -> List[Dict[str, str]]:
        """
        Get the items id from an alias.
        """
        if code not in self.cache.get("fromAlias", {}).get(lang, {}).get(instance_of, {}):
            items = [
                {
                    "id": item["item"]["value"].split("/")[-1],
                    "url": item["item"]["value"],
                    "label": item["itemLabel"]["value"],
                }
                for item in self.run_query(
                    f"""
        SELECT DISTINCT ?item ?itemLabel WHERE {{
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
  {{
    SELECT DISTINCT ?item WHERE {{
      ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
      ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
      ?item skos:altLabel ?alias.
      ?item wdt:{PROPERTY_POPULATION} ?population.
      FILTER(CONTAINS(?alias, "{code}"@en))
    }}
    ORDER BY DESC(?population)
    LIMIT {limit}
  }}
        }}"""
                )["results"]["bindings"]
            ]

            if not items:
                items = [
                    {
                        "id": item["item"]["value"].split("/")[-1],
                        "url": item["item"]["value"],
                        "label": item["itemLabel"]["value"],
                    }
                    for item in self.run_query(
                        f"""
            SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
    {{
        SELECT DISTINCT ?item WHERE {{
        ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
        ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
        ?item skos:altLabel ?alias.
        FILTER(CONTAINS(?alias, "{code}"@en))
        }}
        LIMIT {limit}
    }}
            }}"""
                    )["results"]["bindings"]
                ]

                def _get_id(item: Dict[str, Any]) -> str:
                    return cast(str, item["id"])

                items = sorted(items, key=_get_id)
            self.cache.setdefault("fromAlias", {}).setdefault(lang, {}).setdefault(instance_of, {})[
                code
            ] = items

            self.cache["fromAlias"][lang][instance_of][code].sort(key=lambda x: int(x["id"][1:]))
            self._save_cache()
        return cast(List[Dict[str, str]], self.cache["fromAlias"][lang][instance_of][code])

    def _get_item_obj(self, item_id: wikidata.entity.EntityId) -> wikidata.entity.Entity:
        if item_id not in self.memory_cache:
            self.memory_cache[item_id] = self.client.get(item_id, load=True)
        return self.memory_cache[item_id]

    def get_item(
        self,
        item_id: str,
        properties: Optional[List[str]] = None,
        with_id: bool = True,
        with_name: bool = True,
        with_description: bool = False,
        prefix: str = "",
    ) -> Dict[str, Any]:
        """
        Get the item with the given item_id as a JSON object.
        """

        if properties is None:
            properties = []

        json_item = self.cache.get("items", {}).get(item_id, {})
        item = None
        dirty_cache = False
        if not json_item:
            item = self._get_item_obj(cast(wikidata.entity.EntityId, item_id))
            assert item.label is not None
            json_item["name"] = str(item.label)
            json_item["description"] = str(item.description)
            self.cache.setdefault("items", {})[item_id] = json_item
            dirty_cache = True

        result = {}
        if with_id:
            result[f"{prefix}Id"] = item_id
        if with_name:
            result[f"{prefix}Name"] = json_item["name"]
        if with_description:
            result[f"{prefix}Description"] = json_item["description"]

        for property_id in properties:
            property_name = self.get_property_name(property_id)
            if property_name not in json_item:
                if item is None:
                    item = self._get_item_obj(cast(wikidata.entity.EntityId, item_id))
                property_value = item.get(self._get_item_obj(cast(wikidata.entity.EntityId, property_id)))
                if isinstance(property_value, wikidata.quantity.Quantity):
                    # TODO: handle amount, units, lower_bound, upper_bound
                    property_value = property_value.amount
                json_item[property_name] = property_value
                dirty_cache = True
            result[prefix + standardize_property(property_name)] = json_item[property_name]

        if dirty_cache:
            self._save_cache()

        return result

    def datasource(
        self,
        instance_of: str,
        lang: str = "en",
        with_id: bool = False,
        with_description: bool = False,
        with_name: bool = True,
        properties: Optional[List[str]] = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Get the Datasource as DataFrame."""

        if properties is None:
            properties = []

        ids = [
            item["item"]["value"].split("/")[-1]
            for item in self.run_query(
                f"""
        SELECT DISTINCT ?item ?itemLabel WHERE {{
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }}
  {{
    SELECT DISTINCT ?item WHERE {{
      ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
      ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
    }}
    LIMIT {limit}
  }}
        }}"""
            )["results"]["bindings"]
        ]
        values: Dict[str, List[Any]] = {}
        for element_id in ids:
            item = self.get_item(
                element_id,
                properties=properties,
                with_name=with_name,
                with_description=with_description,
                with_id=with_id,
            )
            for key, value in item.items():
                values.setdefault(key, []).append(value)
        return pd.DataFrame(values)
