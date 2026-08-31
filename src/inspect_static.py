import pandas as pd

routes = pd.read_csv("data/raw/static/gtfs_subway/routes.txt")

print(routes.head())