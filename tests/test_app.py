import os.path
import unittest

from octue import Runner

from elevations_populator.app import App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))


class TestApp(unittest.TestCase):
    def test_app(self):
        """Test that the app runs when given valid input."""
        runner = Runner(app_src=App, twine=os.path.join(REPOSITORY_ROOT, "twine.json"))
        analysis = runner.run(input_values={"h3_cells": [0]})
        self.assertIsNone(analysis.output_values)
