import json

import geopandas as gpd
import plotly.express as px
from geojson import Feature, FeatureCollection
from h3 import h3
from shapely.geometry import Polygon


def add_polygon(row):
    points = h3.h3_to_geo_boundary(row["h3_cell"], geo_json=True)
    return Polygon(points)


def hexagons_dataframe_to_geojson(df_hex, hex_id_field, geometry_field, value_field, file_output=None):
    feature_collection = FeatureCollection(
        [
            Feature(geometry=row[geometry_field], id=row[hex_id_field], properties={"value": row[value_field]})
            for i, row in df_hex.iterrows()
        ]
    )

    if not file_output:
        return feature_collection

    with open(file_output, "w") as f:
        json.dump(feature_collection, f)


with open("local_storage.json") as f:
    elevations = json.load(f)

data = {"h3_cell": [row[0] for row in elevations], "elevation": [row[1] for row in elevations]}
df = gpd.GeoDataFrame(data=data)

df["geometry"] = df.apply(add_polygon, axis=1)


feature_collection = hexagons_dataframe_to_geojson(
    df,
    hex_id_field="h3_cell",
    geometry_field="geometry",
    value_field="elevation",
)

figure = px.choropleth_mapbox(
    df,
    geojson=feature_collection,
    locations="h3_cell",
    color="elevation",
    color_continuous_scale="Viridis",
    range_color=(0, df["elevation"].mean()),
    mapbox_style="carto-positron",
    zoom=7,
    center={"lat": 65.469211, "lon": -136.713865},
    opacity=0.7,
    labels={"elevation": "Elevation above sea level [m]"},
)

figure.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
figure.show()
