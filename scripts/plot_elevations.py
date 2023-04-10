import json

from elevations_populator.utils import plot_elevations


with open("scripts/local_storage.json") as f:
    elevations = json.load(f)

figure = plot_elevations(elevations, center="8d19507316da43f", zoom=15)
figure.show()
