rule all:
    input:
        "data/taxi_zones.shp",
        "data/yellow_tripdata_2024-09.parquet",
        "data/manhatten.graphml"
    output:
        "raw/manhattan-trips.parquet",
        "raw/manhattan-distances.npy",
        "raw/manhattan-nodes.parquet"
    shell:
        "python graph.py"


rule zones_zip:
    output:
        "data/taxi_zones.zip"
    shell:
        "curl https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip > {output}"


rule zones:
    input:
        "data/taxi_zones.zip"
    output:
        "data/taxi_zones.shp"
    shell:
        "unzip {input} -d data"


rule trips:
    output:
        "data/yellow_tripdata_2024-09.parquet"
    shell:
        "curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-09.parquet > {output}"


rule graph:
    input:
        "data/manhatten.graphml"
    shell:
        "kaggle datasets download crailtap/street-network-of-new-york-in-graphml -p data --unzip --force"
