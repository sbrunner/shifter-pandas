# Convert some data into Panda DataFrames

## British Petroleum (BP)

It parse sheet like `Primary Energy Consumption` (not like `Primary Energy - Cons by fuel`).

Open: http://www.bp.com/statisticalreview
or https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html

Download `Statistical Review of World Energy – all data`.

Use:

```python
from shifter_pandas.bp import UNITS_ENERGY, BPDatasource

shifter_ds = BPDatasource("bp-stats-review-2021-all-data.xlsx")

df = shifter_ds.datasource(units_filter=UNITS_ENERGY, regions_filter=["Switzerland"])
df
```

## Swiss Office Federal of Statistics (OFS)

From https://www.bfs.admin.ch/bfs/fr/home/services/recherche/stat-tab-donnees-interactives.html
create a stat table.

Click on `À propos du tableau`

Click on `Rendez ce tableau disponible dans votre application`

Use:

```python
from shifter_pandas.ofs import OFSDatasource

shifter_ds = OFSDatasource("<URL>")

df = shifter_ds.datasource(<Requête Json>)
df
```

And replace `<URL>` and `<Requête Json>` with the content of the fields of the OFS web page.

### Interesting sources

- [Parc de motocycles par caractéristiques techniques et émissions](https://www.pxweb.bfs.admin.ch/pxweb/fr/px-x-1103020100_165/-/px-x-1103020100_165.px/)
- [Bilan démographique selon l'âge et le canton](https://www.pxweb.bfs.admin.ch/pxweb/fr/px-x-0102020000_104/-/px-x-0102020000_104.px/)

## Our World in Data

Select a publication.

Click `Download`.

Click `Full data (CSV)`.

Use:

```python
import pandas as pd
from shifter_pandas.wikidata_ import WikidataDatasource

df_owid = pd.read_csv("<file name>")
wdds = WikidataDatasource()
df_wd = wdds.datasource_code(wikidata_id=True, wikidata_name=True, wikidata_type=True)
df = pd.merge(df_owid, df_wd, how="inner", left_on='iso_code', right_on='Code')
df
```

### Interesting sources

- [GDP, 1820 to 2018](https://ourworldindata.org/grapher/gdp-world-regions-stacked-area)
- [Population, 1800 to 2021](https://ourworldindata.org/grapher/population-since-1800)

## World Bank

Open https://data.worldbank.org/

Find a chart

In `Download` click `CSV`

Use:

```python
from shifter_pandas.worldbank import wbDatasource

df = wbDatasource("<file name>")
df
```

### Interesting sources

- [GDP (current US$)](https://data.worldbank.org/indicator/NY.GDP.MKTP.CD)
- [GDP (constant 2015 US$)](https://data.worldbank.org/indicator/NY.GDP.MKTP.KD)

## Wikidata

By providing the `wikidata_*` parameters, you can ass some data from WikiData.

Careful, the WikiData is relatively slow then the first time you run it il will be slow.
We use a cache to make it fast the next times.

You can also get the country list with population and ISO 2 code with:

```python
from shifter_pandas.wikidata_ import (
    ELEMENT_COUNTRY,
    PROPERTY_ISO_3166_1_ALPHA_2,
    PROPERTY_POPULATION,
    WikidataDatasource,
)

shifter_ds = WikidataDatasource()
df = shifter_ds.datasource(
    instance_of=ELEMENT_COUNTRY,
    with_id=True,
    with_name=True,
    properties=[PROPERTY_ISO_3166_1_ALPHA_2, PROPERTY_POPULATION],
    limit=1000,
)
df
```

## Contributing

Install the pre-commit hooks:

```bash
pip install pre-commit
pre-commit install --allow-missing-config
```
