"""Datasource builder for data from British Petroleum."""

from typing import Any, Dict, List, Optional

import openpyxl
import pandas as pd


class BPDatasource:
    """Datasource builder for data from British Petroleum."""

    def __init__(self, file_name: str) -> None:
        """Initialize the datasource builder."""
        self.xlsx = openpyxl.load_workbook(file_name)

    def metadata(self) -> List[Dict[str, Any]]:
        """Get the metadata."""
        metadata: List[Dict[str, Any]] = []
        for index, value in enumerate(self.xlsx.sheetnames):
            if isinstance(self.xlsx.worksheets[index].cell(3, 2).value, int):
                # Year
                years = []
                index = 2
                while True:
                    value = self.xlsx.worksheets[index].cell(3, index).value
                    if self.xlsx.worksheets[index].cell(2, index).value is not None:
                        break
                    years.append({"label": value, "index": index})
                    index += 1

                # Country
                regions = []
                index = 5
                while True:
                    value = self.xlsx.worksheets[index].cell(index, 1).value
                    if value is not None and value == "Switzerland":
                        regions.append({"label": value, "index": index})
                    if value == "Total World":
                        break
                    index += 1
                metadata.append(
                    {
                        "label": value,
                        "index": index,
                        "unit": self.xlsx.worksheets[index].cell(3, 1).value,
                        "years": years,
                        "refions": regions,
                    }
                )

        return metadata

    def datasource(
        self,
        types_filter: Optional[List[str]] = None,
        regions_filter: Optional[List[str]] = None,
        years_filter: Optional[List[str]] = None,
        years_factor: Optional[int] = None,
        coherent_units: bool = True,
    ) -> pd.DataFrame:
        """Get the Datasource ad DataFrame."""
        data_frame = pd.DataFrame(columns=["value", "type", "unit", "type unit", "year", "region"])
        for type_ in self.metadata():
            type_index = type_["index"]
            type_label = type_["label"]
            years = type_["years"]
            regions = type_["regions"]

            if type_label.endswith(" - TWh"):
                type_label = type_label[:-6]
            if type_label.endswith(" - EJ"):
                type_label = type_label[:-5]
            if type_label.endswith(" - PJ"):
                type_label = type_label[:-5]

            if types_filter is not None and type_label not in types_filter:
                continue

            for year in years:
                if years_filter is not None and year["label"] not in years_filter:
                    continue
                if years_factor is not None and year["label"] % years_factor != 0:
                    continue
                for region in regions:
                    if regions_filter is not None and region["label"] not in regions_filter:
                        continue
                    value = self.xlsx.worksheets[type_index].cell(region["index"], year["index"]).value
                    if not isinstance(value, int) and not isinstance(value, float):
                        continue

                    unit = self.xlsx.worksheets[type_index].cell(3, 1).value
                    if coherent_units:
                        if unit == "Terawatt-hours":
                            value = value * 3.6
                            unit = "Petajoules"
                        if unit == "Exajoules":
                            value = value * 1000
                            unit = "Petajoules"
                        if unit == "Exajoules (input-equivalent)":
                            value = value * 1000
                            unit = "Petajoules (input-equivalent)"

                    data_frame = data_frame.append(
                        {
                            "value": value,
                            "year": year["label"],
                            "region": region["label"],
                            "type": type_label,
                            "unit": unit,
                            "type unit": f"{type_label} [{unit}]",
                        },
                        ignore_index=True,
                    )

        return data_frame
