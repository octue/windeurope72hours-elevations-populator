import geopandas as gpd
import plotly.express as px
from geojson import Feature, FeatureCollection
from h3.api.basic_int import h3_to_geo, h3_to_geo_boundary
from shapely.geometry import Polygon


def plot_elevations(elevations, center, color_continuous_scale="Viridis", opacity=0.2, zoom=7):
    """Plot a colour-scaled elevation map from (h3_cell, elevation) pairs. The colour scale ranges from the minimum
    elevation to the maximum elevation in the given data.

    :param list(int, float) elevations: (h3_cell, elevation) pairs
    :param int center: the h3 cell to center the map on
    :param str color_continuous_scale: the name of the Plotly color continuous scale to use
    :param float opacity: the opacity of the coloured h3 cells. An opacity of 1 makes the map invisible under the cells whereas an opacity of 0 makes the cells invisible above the map.
    :param int zoom: the zoom level to default when showing the map
    :return plotly.graph_objs.Figure: the colour-scaled elevation map
    """
    df = gpd.GeoDataFrame(data={"h3_cell": [row[0] for row in elevations], "elevation": [row[1] for row in elevations]})
    df["geometry"] = df.apply(_create_polygon, axis=1)

    geojson_feature_collection = FeatureCollection(
        [
            Feature(geometry=row["geometry"], id=row["h3_cell"], properties={"value": row["elevation"]})
            for i, row in df.iterrows()
        ]
    )

    center_latitude, center_longitude = h3_to_geo(center)

    figure = px.choropleth_mapbox(
        df,
        geojson=geojson_feature_collection,
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
    points = h3_to_geo_boundary(row["h3_cell"], geo_json=True)
    return Polygon(points)
