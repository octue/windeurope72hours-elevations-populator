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
    cells_column = []
    elevations_column = []
    polygons_column = []

    for row in elevations:
        cells_column.append(row[0])
        elevations_column.append(row[1])
        polygons_column.append(Polygon(h3_to_geo_boundary(row[0], geo_json=True)))

    df = gpd.GeoDataFrame(
        data={
            "h3_cell": cells_column,
            "elevation": elevations_column,
            "geometry": polygons_column,
        }
    )

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
