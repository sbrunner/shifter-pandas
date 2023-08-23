"""Datasource builder for data from WikiData."""

import json
import os
import shutil
from typing import Any, Optional, cast

import pandas as pd
import requests
import wikidata.entity
import wikidata.quantity
from wikidata.client import Client

from shifter_pandas import standardize_property

ELEMENT_COUNTRY = "Q6256"
ELEMENT_CONTINENT = "Q5107"
ELEMENT_CANTON_CH = "Q23058"
ELEMENT_SUBCONTINENT = "Q855697"
ELEMENT_GEOPOLITICAL_REGION = "Q82794"
ELEMENT_SUBREGION = "Q7631958"
ELEMENT_ELECTORAL_DISTRICT = "Q192611"
ELEMENT_POLITICAL_TERRITORY_ENTITY = "Q1048835"
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

        if os.path.exists(os.environ.get("WIKIDATA_CACHE_FILE", ".wikidata-cache.json")):
            with open(
                os.environ.get("WIKIDATA_CACHE_FILE", ".wikidata-cache.json"), encoding="utf-8"
            ) as file:
                self.cache = json.load(file)
        else:
            self.cache = {}
        self.memory_cache: dict[str, wikidata.entity.Entity] = {}

        self.custom_aliases: dict[str, dict[str, dict[str, str]]] = {}

        self.client = Client()

    def _save_cache(self) -> None:
        with open(".wikidata-cache.json.new", "w", encoding="utf-8") as file:
            file.write(json.dumps(self.cache, indent=2))
        shutil.move(".wikidata-cache.json.new", os.environ.get("WIKIDATA_CACHE_FILE", ".wikidata-cache.json"))

    def run_query(self, query: str) -> dict[str, Any]:
        """
        Run a SPARQL query against the Wikidata SPARQL endpoint.

        Query can be build here: https://query.wikidata.org/querybuilder/
        """
        payload = {
            "query": query,
            "format": "json",
        }
        response = requests.get(self.endpoint_url, params=payload, headers=self.headers, timeout=120)
        if not response.ok:
            print(f"Error on query {self.endpoint_url}: {response.status_code}")
            print(response.text)
            print(query)
            response.raise_for_status()
        return cast(dict[str, Any], response.json())

    def get_property_name(self, property_id: str) -> str:
        """Get the name of a property."""

        if property_id not in self.cache.get("properties", {}):
            self.cache.setdefault("properties", {})[property_id] = str(
                self._get_item_obj(cast(wikidata.entity.EntityId, property_id)).label
            )
            self._save_cache()
        return cast(str, self.cache["properties"][property_id])

    def set_alias(self, instance_of: str, name: str, item_id: str, label: str = "") -> None:
        """
        Set an alias in the cache to prevent unwanted match.
        """
        self.custom_aliases.setdefault("name", {})[name] = {
            "id": item_id,
            "url": f"http://www.wikidata.org/entity/{item_id}",
            "label": label,
            "type": instance_of,
        }

    def set_alias_code(self, instance_of: str, code: str, item_id: str, label: str = "") -> None:
        """
        Set an alias in the cache to prevent unwanted match.
        """
        self.custom_aliases.setdefault("code", {})[code] = {
            "id": item_id,
            "url": f"http://www.wikidata.org/entity/{item_id}",
            "label": label,
            "instance_of": instance_of,
        }

    def get_from_alias(
        self, instance_of: str, code: str, lang: str = "en", limit: int = 10
    ) -> list[dict[str, str]]:
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
    FILTER(CONTAINS(?alias, "{code}"@en))
    }}
    LIMIT {limit}
}}
        }}"""
                )["results"]["bindings"]
            ]

            def _get_id(item: dict[str, Any]) -> str:
                return cast(str, item["id"])

            items = sorted(items, key=_get_id)
            self.cache.setdefault("fromAlias", {}).setdefault(lang, {}).setdefault(instance_of, {})[
                code
            ] = items

            self.cache["fromAlias"][lang][instance_of][code].sort(key=lambda x: int(x["id"][1:]))
            self._save_cache()
        return cast(list[dict[str, str]], self.cache["fromAlias"][lang][instance_of][code])

    def _get_item_obj(self, item_id: wikidata.entity.EntityId) -> wikidata.entity.Entity:
        if item_id not in self.memory_cache:
            self.memory_cache[item_id] = self.client.get(item_id, load=True)
        return self.memory_cache[item_id]

    def get_item(
        self,
        item_id: Optional[str],
        properties: Optional[list[str]] = None,
        with_id: bool = False,
        with_name: bool = True,
        with_description: bool = False,
        prefix: str = "",
    ) -> dict[str, Any]:
        """
        Get the item with the given item_id as a JSON object.
        """

        if properties is None:
            properties = []

        json_item = self.cache.get("items", {}).get(item_id, {}) if item_id else {}
        item = None
        dirty_cache = False
        if not json_item and item_id:
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
            result[f"{prefix}Name"] = json_item.get("name")
        if with_description:
            result[f"{prefix}Description"] = json_item.get("description")

        for property_id in properties:
            property_name = self.get_property_name(property_id)
            if property_name not in json_item and item_id:
                if item is None:
                    item = self._get_item_obj(cast(wikidata.entity.EntityId, item_id))
                property_value = item.get(self._get_item_obj(cast(wikidata.entity.EntityId, property_id)))
                if isinstance(property_value, wikidata.quantity.Quantity):
                    # TODO: handle amount, units, lower_bound, upper_bound
                    property_value = property_value.amount
                json_item[property_name] = property_value
                dirty_cache = True
            result[prefix + standardize_property(property_name)] = json_item.get(property_name)

        if dirty_cache:
            self._save_cache()

        return result

    def get_region(self, region: Optional[str], code: Optional[str] = None) -> Optional[dict[str, str]]:
        """Get the region informations."""

        none_match = False
        if code in self.custom_aliases.get("code", {}):
            if self.custom_aliases["code"][code] is None:
                none_match = True
            else:
                return self.custom_aliases["code"][code]
        if region in self.custom_aliases.get("name", {}):
            if self.custom_aliases["name"][region] is None:
                none_match = True
            else:
                return self.custom_aliases["name"][region]

        if code in self.cache.get("regions", {}).get("code", {}):
            if self.cache["regions"]["code"][code] is None:
                none_match = True
            else:
                return cast(dict[str, str], self.cache["regions"]["code"][code])
        if region in self.cache.get("regions", {}).get("name", {}):
            if self.cache["regions"]["name"][region] is None:
                none_match = True
            else:
                return cast(dict[str, str], self.cache["regions"]["name"][region])

        if none_match:
            return None

        categories = [
            (ELEMENT_CONTINENT, "continent"),
            (ELEMENT_COUNTRY, "country"),
        ]
        lang = "en"
        limit = 10

        for instance_of, type_value in categories:
            code_property = None
            if code and len(code) == 2:
                code_property = PROPERTY_ISO_3166_1_ALPHA_2
            if code and len(code) == 3:
                code_property = PROPERTY_ISO_3166_1_ALPHA_3

            if code_property:
                items = [
                    {
                        "id": item["item"]["value"].split("/")[-1],
                        "url": item["item"]["value"],
                        "label": item["itemLabel"]["value"],
                        "type": type_value,
                    }
                    for item in self.run_query(
                        f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item WHERE {{
            ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
            ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
            ?item p:{code_property} ?code.
            ?code (ps:{code_property}) "{code}"
        }}
        LIMIT {limit}
    }}
}}"""
                    )["results"]["bindings"]
                ]
                if items:
                    self.cache.setdefault("regions", {}).setdefault("code", {})[code] = items[0]
                    self._save_cache()
                    return items[0]

        code_property = None
        if code and len(code) == 2:
            code_property = PROPERTY_ISO_3166_1_ALPHA_2
        if code and len(code) == 3:
            code_property = PROPERTY_ISO_3166_1_ALPHA_3

        if code_property:
            items = [
                {
                    "id": item["item"]["value"].split("/")[-1],
                    "url": item["item"]["value"],
                    "label": item["itemLabel"]["value"],
                    "type": "other",
                }
                for item in self.run_query(
                    f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item WHERE {{
            ?item p:{code_property} ?code.
            ?code (ps:{code_property}) "{code}"
        }}
        LIMIT {limit}
    }}
}}"""
                )["results"]["bindings"]
            ]
            if items:
                self.cache.setdefault("regions", {}).setdefault("code", {})[code] = items[0]
                self._save_cache()
                return items[0]

        if not region:
            if code:
                self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
            self._save_cache()
            return None

        for instance_of, type_value in categories:
            code_property = None
            if len(region) == 2:
                code_property = PROPERTY_ISO_3166_1_ALPHA_2
            if len(region) == 3:
                code_property = PROPERTY_ISO_3166_1_ALPHA_3

            if code_property:
                items = [
                    {
                        "id": item["item"]["value"].split("/")[-1],
                        "url": item["item"]["value"],
                        "label": item["itemLabel"]["value"],
                        "type": type_value,
                    }
                    for item in self.run_query(
                        f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item WHERE {{
            ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
            ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
            ?item p:{code_property} ?code.
            ?code (ps:{code_property}) "{region}"
        }}
        LIMIT {limit}
    }}
}}"""
                    )["results"]["bindings"]
                ]
                if items:
                    if code:
                        self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
                    self.cache.setdefault("regions", {}).setdefault("name", {})[region] = items[0]
                    self._save_cache()
                    return items[0]

        code_property = None
        if len(region) == 2:
            code_property = PROPERTY_ISO_3166_1_ALPHA_2
        if len(region) == 3:
            code_property = PROPERTY_ISO_3166_1_ALPHA_3

        if code_property:
            items = [
                {
                    "id": item["item"]["value"].split("/")[-1],
                    "url": item["item"]["value"],
                    "label": item["itemLabel"]["value"],
                    "type": "other",
                }
                for item in self.run_query(
                    f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item WHERE {{
            ?item p:{code_property} ?code.
            ?code (ps:{code_property}) "{region}"
        }}
        LIMIT {limit}
    }}
}}"""
                )["results"]["bindings"]
            ]
            if items:
                if code:
                    self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
                self.cache.setdefault("regions", {}).setdefault("name", {})[region] = items[0]
                self._save_cache()
                return items[0]

        categories = [
            (ELEMENT_CONTINENT, "continent"),
            (ELEMENT_COUNTRY, "country"),
            (ELEMENT_SUBCONTINENT, "subcontinent"),
            (ELEMENT_GEOPOLITICAL_REGION, "geographic region"),
            (ELEMENT_SUBREGION, "subregion"),
            (ELEMENT_ELECTORAL_DISTRICT, "electoral district"),
            (ELEMENT_POLITICAL_TERRITORY_ENTITY, "political territorial entity"),
        ]

        for instance_of, type_value in categories:
            items = [
                {
                    "id": item["item"]["value"].split("/")[-1],
                    "url": item["item"]["value"],
                    "label": item["itemLabel"]["value"],
                    "type": type_value,
                }
                for item in self.run_query(
                    f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item WHERE {{
            ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
            ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
            ?item rdfs:label ?label.
            FILTER(LCASE(?label) = "{region.lower()}"@{lang})
        }}
        LIMIT {limit}
    }}
}}"""
                )["results"]["bindings"]
            ]
            if items:
                if code:
                    self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
                self.cache.setdefault("regions", {}).setdefault("name", {})[region] = items[0]
                self._save_cache()
                return items[0]

        for instance_of, type_value in categories:
            items = [
                {
                    "id": item["item"]["value"].split("/")[-1],
                    "url": item["item"]["value"],
                    "label": item["itemLabel"]["value"],
                    "type": type_value,
                }
                for item in self.run_query(
                    f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item ?population WHERE {{
            ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
            ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
            ?item skos:altLabel ?alias.
            ?item wdt:{PROPERTY_POPULATION} ?population.
            FILTER(CONTAINS(?alias, "{region}"@{lang}))
        }}
        LIMIT {limit}
    }}
}}
ORDER BY DESC(?population)"""
                )["results"]["bindings"]
            ]
            if items:
                if code:
                    self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
                self.cache.setdefault("regions", {}).setdefault("name", {})[region] = items[0]
                self._save_cache()
                return items[0]

            items = [
                {
                    "id": item["item"]["value"].split("/")[-1],
                    "url": item["item"]["value"],
                    "label": item["itemLabel"]["value"],
                    "type": type_value,
                }
                for item in self.run_query(
                    f"""
SELECT DISTINCT ?item ?itemLabel WHERE {{
    SERVICE wikibase:label {{ bd:serviceParam wikibase:language "{lang}". }} {{
        SELECT DISTINCT ?item WHERE {{
            ?item p:{PROPERTY_INSTANCE_OF} ?statement0.
            ?statement0 (ps:{PROPERTY_INSTANCE_OF}) wd:{instance_of}.
            ?item skos:altLabel ?alias.
            FILTER(CONTAINS(?alias, "{region}"@{lang}))
        }}
        LIMIT {limit}
    }}
}}"""
                )["results"]["bindings"]
            ]

            def _get_id(item: dict[str, Any]) -> str:
                return cast(str, item["id"])

            items = sorted(items, key=_get_id)
            if items:
                if code:
                    self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
                self.cache.setdefault("regions", {}).setdefault("name", {})[region] = items[0]
                self._save_cache()
                return items[0]

        if code:
            self.cache.setdefault("regions", {}).setdefault("code", {})[code] = None
        self.cache.setdefault("regions", {}).setdefault("name", {})[region] = None
        self._save_cache()
        return None

    def datasource(
        self,
        instance_of: str,
        lang: str = "en",
        with_id: bool = False,
        with_description: bool = False,
        with_name: bool = True,
        properties: Optional[list[str]] = None,
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
        values: dict[str, list[Any]] = {}
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

    # For Our World in Data
    def datasource_code(
        self,
        wikidata_id: bool = False,
        wikidata_name: bool = False,
        wikidata_type: bool = False,
        wikidata_properties: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Get the Datasource as DataFrame with the codes for Our World in Data."""
        codes = {
            "ABW",
            "AFG",
            "AGO",
            "AIA",
            "ALB",
            "AND",
            "ARE",
            "ARG",
            "ARM",
            "ATG",
            "AUS",
            "AUT",
            "AZE",
            "BDI",
            "BEL",
            "BEN",
            "BES",
            "BFA",
            "BGD",
            "BGR",
            "BHR",
            "BHS",
            "BIH",
            "BLR",
            "BLZ",
            "BMU",
            "BOL",
            "BRA",
            "BRB",
            "BRN",
            "BTN",
            "BWA",
            "CAF",
            "CAN",
            "CHE",
            "CHL",
            "CHN",
            "CIV",
            "CMR",
            "COD",
            "COG",
            "COK",
            "COL",
            "COM",
            "CPV",
            "CRI",
            "CUB",
            "CUW",
            "CYM",
            "CYP",
            "CZE",
            "DEU",
            "DJI",
            "DMA",
            "DNK",
            "DOM",
            "DZA",
            "ECU",
            "EGY",
            "ERI",
            "ESP",
            "EST",
            "ETH",
            "FIN",
            "FJI",
            "FLK",
            "FRA",
            "FRO",
            "FSM",
            "GAB",
            "GBR",
            "GEO",
            "GGY",
            "GHA",
            "GIB",
            "GIN",
            "GMB",
            "GNB",
            "GNQ",
            "GRC",
            "GRD",
            "GRL",
            "GTM",
            "GUY",
            "HKG",
            "HND",
            "HRV",
            "HTI",
            "HUN",
            "IDN",
            "IMN",
            "IND",
            "IRL",
            "IRN",
            "IRQ",
            "ISL",
            "ISR",
            "ITA",
            "JAM",
            "JEY",
            "JOR",
            "JPN",
            "KAZ",
            "KEN",
            "KGZ",
            "KHM",
            "KIR",
            "KNA",
            "KOR",
            "KWT",
            "LAO",
            "LBN",
            "LBR",
            "LBY",
            "LCA",
            "LIE",
            "LKA",
            "LSO",
            "LTU",
            "LUX",
            "LVA",
            "MAC",
            "MAR",
            "MCO",
            "MDA",
            "MDG",
            "MDV",
            "MEX",
            "MHL",
            "MKD",
            "MLI",
            "MLT",
            "MMR",
            "MNE",
            "MNG",
            "MOZ",
            "MRT",
            "MSR",
            "MUS",
            "MWI",
            "MYS",
            "NAM",
            "NCL",
            "NER",
            "NGA",
            "NIC",
            "NIU",
            "NLD",
            "NOR",
            "NPL",
            "NRU",
            "NZL",
            "OMN",
            "OWID_AFR",
            "OWID_ASI",
            "OWID_CYN",
            "OWID_EUN",
            "OWID_EUR",
            "OWID_HIC",
            "OWID_INT",
            "OWID_KOS",
            "OWID_LIC",
            "OWID_LMC",
            "OWID_NAM",
            "OWID_OCE",
            "OWID_SAM",
            "OWID_UMC",
            "OWID_WRL",
            "PAK",
            "PAN",
            "PCN",
            "PER",
            "PHL",
            "PLW",
            "PNG",
            "POL",
            "PRT",
            "PRY",
            "PSE",
            "PYF",
            "QAT",
            "ROU",
            "RUS",
            "RWA",
            "SAU",
            "SDN",
            "SEN",
            "SGP",
            "SHN",
            "SLB",
            "SLE",
            "SLV",
            "SMR",
            "SOM",
            "SPM",
            "SRB",
            "SSD",
            "STP",
            "SUR",
            "SVK",
            "SVN",
            "SWE",
            "SWZ",
            "SXM",
            "SYC",
            "SYR",
            "TCA",
            "TCD",
            "TGO",
            "THA",
            "TJK",
            "TKL",
            "TKM",
            "TLS",
            "TON",
            "TTO",
            "TUN",
            "TUR",
            "TUV",
            "TWN",
            "TZA",
            "UGA",
            "UKR",
            "URY",
            "USA",
            "UZB",
            "VAT",
            "VCT",
            "VEN",
            "VGB",
            "VNM",
            "VUT",
            "WLF",
            "WSM",
            "YEM",
            "ZAF",
            "ZMB",
            "ZWE",
        }

        data: dict[str, list[Any]] = {}
        for code in codes:
            element_id = self.get_region(None, code)
            if element_id:
                data.setdefault("Code", []).append(code)

                if wikidata_type:
                    data.setdefault("WikidataType", []).append(element_id["type"] if element_id else None)
                item = self.get_item(
                    element_id["id"],
                    with_name=wikidata_name,
                    with_id=wikidata_id,
                    properties=wikidata_properties,
                    prefix="Wikidata",
                )
                for key, value in item.items():
                    data.setdefault(key, []).append(value)
        return pd.DataFrame(data)
