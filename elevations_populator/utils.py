import geopandas as gpd
import plotly.express as px
from geojson import Feature, FeatureCollection
from h3 import h3
from shapely.geometry import Polygon


def plot_elevations(elevations, center, color_continuous_scale="Viridis", opacity=0.2, zoom=7):
    """Plot a colour-scaled elevation map from (h3_cell, elevation) pairs. The colour scale ranges from the minimum
    elevation to the maximum elevation in the given data.

    :param list(string, float) elevations: (h3_cell, elevation) pairs
    :param str center: the h3 cell to center the map on
    :param str color_continuous_scale: the name of the Plotly color continuous scale to use
    :param float opacity: the opacity of the coloured h3 cells. An opacity of 1 makes the map invisible under the cells whereas an opacity of 0 makes the cells invisible above the map.
    :param int zoom: the zoom level to default when showing the map
    :return plotly.graph_objs.Figure: the colour-scaled elevation map
    """
    df = gpd.GeoDataFrame(data={"h3_cell": [row[0] for row in elevations], "elevation": [row[1] for row in elevations]})
    df["geometry"] = df.apply(_create_polygon, axis=1)

    feature_collection = _hexagons_dataframe_to_geojson(
        df,
        hex_id_field="h3_cell",
        geometry_field="geometry",
        value_field="elevation",
    )

    center_latitude, center_longitude = h3.h3_to_geo(center)

    figure = px.choropleth_mapbox(
        df,
        geojson=feature_collection,
        locations="h3_cell",
        color="elevation",
        color_continuous_scale=color_continuous_scale,
        range_color=(df["elevation"].min(), df["elevation"].max()),
        mapbox_style="carto-positron",
        zoom=zoom,
        center={"lat": center_latitude, "lon": center_longitude},
        opacity=opacity,
        labels={"elevation": "Elevation above sea level [m]"},
    )

    figure.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return figure


def _create_polygon(row):
    """Create a Shapely polygon from the boundary of the h3 cell in the row.

    :param pandas.Series row: the row of the dataframe to use when creating the polygon
    :return shapely.geometry.polygon.Polygon:
    """
    points = h3.h3_to_geo_boundary(row["h3_cell"], geo_json=True)
    return Polygon(points)


def _hexagons_dataframe_to_geojson(df_hex, hex_id_field, geometry_field, value_field):
    return FeatureCollection(
        [
            Feature(geometry=row[geometry_field], id=row[hex_id_field], properties={"value": row[value_field]})
            for i, row in df_hex.iterrows()
        ]
    )
