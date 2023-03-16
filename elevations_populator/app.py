import logging

logger = logging.getLogger(__name__)


class App:
    def __init__(self, analysis):
        self.analysis = analysis

    def run(self):
        logger.info("The elevations service has started.")
