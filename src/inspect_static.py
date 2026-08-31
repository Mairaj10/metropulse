import pandas as pd

routes = pd.read_csv("data/raw/static/gtfs_subway/routes.txt")

target_routes = ["A", "C", "E"]

ace_routes = routes[routes["route_id"].isin(target_routes)]

print(ace_routes[["route_id", "route_long_name"]])