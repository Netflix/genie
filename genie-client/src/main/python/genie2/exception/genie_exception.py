__author__ = 'amsharma'


class GenieException(Exception):
    """Exception class for errors for this module."""
    # Call the base class constructor with the parameters it needs
    def __init__(self, message, ecode=None):
        Exception.__init__(self, message)
        self.ecode = ecode