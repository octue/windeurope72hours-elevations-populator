import json

from elevations_populator.utils import plot_elevations


with open("local_storage.json") as f:
    elevations = json.load(f)

figure = plot_elevations(elevations, center=635452880562529343, zoom=15)
figure.show()
