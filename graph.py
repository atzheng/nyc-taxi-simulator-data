import numpy as np
import networkx as nx
from tqdm import tqdm, trange
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

tqdm.pandas()
np.random.seed(0)

# Graph taken from
# https://www.kaggle.com/datasets/crailtap/street-network-of-new-york-in-graphml

G = nx.read_graphml("data/manhatten.graphml")

# Get node-level data
# -------------------------------------------------------------------------
nodes_df = pd.DataFrame.from_dict(
    {
        "node_id": node_id,
        "idx": i,
        "lng": node_data["x"],
        "lat": node_data["y"],
        "osmid": node_data["osmid"],
    }
    for i, (node_id, node_data) in tqdm(enumerate(G.nodes(data=True)))
).set_index("node_id")


# Compute travel time matrix, in seconds.
# -------------------------------------------------------------------------
# "length" is in meters, assume cars can drive 9 m/s

speed = 9  # m / s
for u, v, data in G.edges(data=True):
    data["weight"] = float(data["length"]) / speed

n = len(G.nodes)
distances = np.ones((n, n)) * np.inf

for src, dests in tqdm(nx.shortest_path_length(G, weight="weight"), total=n):
    for dest, dist in dests.items():
        distances[nodes_df.loc[src, "idx"], nodes_df.loc[dest, "idx"]] = dist

# Remove unreachable nodes
is_invalid = ((distances == np.inf).mean(axis=1) > 0.9) | (
    (distances == np.inf).mean(axis=0) > 0.9
)

# Redefine node indices to align with new distance matrix
distances = distances[~is_invalid][:, ~is_invalid]
nodes_df = nodes_df.sort_values("idx")
nodes_df["is_invalid"] = is_invalid
nodes_df = nodes_df[~is_invalid]
nodes_df["idx"] = range(len(nodes_df))

# Spot check that indexing is correct
assert (
    nx.shortest_path_length(
        G,
        source=nodes_df.iloc[34].osmid,
        target=nodes_df.iloc[4132].osmid,
        weight="weight",
    )
    == distances[34, 4132]
)


# Convert taxi data to lat lngs
# -------------------------------------------------------------------------
taxi = (
    pd.read_parquet("data/yellow_tripdata_2024-09.parquet").reset_index()
    # .head(1000)
)

# Load the shapefile as a GeoDataFrame
shapefile_path = "data/taxi_zones.shp"
polygons_gdf = gpd.read_file(shapefile_path).to_crs(epsg=4326)

# Convert the DataFrame to a GeoDataFrame
# Make sure your coordinate reference systems (CRS) match. You may need to set an appropriate CRS.
nodes_gdf = gpd.GeoDataFrame(
    nodes_df, geometry=gpd.points_from_xy(nodes_df.lng, nodes_df.lat)
)
nodes_gdf.crs = polygons_gdf.crs  # Ensure CRS match

# Use a spatial join to find which polygon each point is in
nodes_with_zones = gpd.sjoin(
    nodes_gdf, polygons_gdf, how="inner", predicate="within"
).set_index("LocationID")

valid_zones = nodes_with_zones.index.unique()


# Merge the taxi data with the nodes, and assign a random node within the
# dropoff and pickup zone for each trip. Note that we are not considering
# trips that start or end outside Manhattan (not even airport trips)
taxi["pickup_osmid"] = taxi.progress_apply(
    lambda row: (
        nodes_with_zones.loc[row.PULocationID, "osmid"].sample(n=1).values[0]
        if row.PULocationID in valid_zones
        else None
    ),
    axis=1,
)

taxi["dropoff_osmid"] = taxi.progress_apply(
    lambda row: (
        nodes_with_zones.loc[row.DOLocationID, "osmid"].sample(n=1).values[0]
        if row.DOLocationID in valid_zones
        else None
    ),
    axis=1,
)

taxi["t"] = (
    taxi.tpep_pickup_datetime - pd.Timestamp("1970-01-01")
) // pd.Timedelta("1s")


taxi_df = (
    taxi.merge(nodes_df[["idx"]], left_on="pickup_osmid", right_index=True)
    .rename(columns={"idx": "pickup_idx"})
    .merge(nodes_df[["idx"]], left_on="dropoff_osmid", right_index=True)
    .rename(columns={"idx": "dropoff_idx"})
)


# Save all outputs
# -------------------------------------------------------------------------
np.save("raw/manhattan-distances.npy", distances)
nodes_df.to_parquet("raw/manhattan-nodes.parquet")
cols = [
    "t",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_osmid",
    "dropoff_osmid",
    "pickup_idx",
    "dropoff_idx",
    "fare_amount",
]
taxi_df[cols].to_parquet("raw/manhattan-trips.parquet", index=False)
