class ElevationsPopulatorException(Exception):
    pass


class DataUnavailable(ElevationsPopulatorException):
    """Raise if data is unavailable."""
