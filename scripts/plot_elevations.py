import json

from elevations_populator.utils import plot_elevations


with open("local_storage.json") as f:
    elevations = json.load(f)

figure = plot_elevations(elevations, center=elevations[0][0], zoom=15, opacity=1)
figure.show()
