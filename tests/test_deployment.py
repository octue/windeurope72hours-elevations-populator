import os
import unittest
from unittest import TestCase

from octue.resources import Child


@unittest.skipUnless(
    condition=os.getenv("RUN_CLOUD_RUN_DEPLOYMENT_TEST", "0").lower() == "1",
    reason="'RUN_CLOUD_RUN_DEPLOYMENT_TEST' environment variable is False or not present.",
)
class TestCloudRunDeployment(TestCase):
    # This is the service ID of the example service deployed to Google Cloud Run.
    child = Child(
        id="octue/elevations-populator:0-2-3",
        backend={"name": "GCPPubSubBackend", "project_name": "windeurope72-private"},
    )

    def test_cloud_run_deployment(self):
        """Test that the Google Cloud Run example deployment works, providing a service that can be asked questions and
        send responses.
        """
        answer = self.child.ask(input_values={"h3_cells": [630949280935159295]})

        # Check the output values.
        self.assertIsNone(answer["output_values"])
