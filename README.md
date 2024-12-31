This repo generates a distance matrix and a data set of trips from New York City taxi data. 

To run this, install [poetry](https://python-poetry.org/) and run `poetry install`, then run `poetry run snakemake all -j1`.

This will download the [OSM graph](https://www.kaggle.com/datasets/crailtap/street-network-of-new-york-in-graphml) of NYC and [TLC taxi trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) from September 2024.

