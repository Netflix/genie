"""
genie.exceptions

This module implements the set of Genie exceptions.

"""


import logging


logger = logging.getLogger('com.netflix.genie.exceptions')


class GenieError(Exception):
    """Base exception for Genie."""
    pass

class GenieAdapterError(GenieError):
    """Error when trying to construct a JSON payload for a job."""
    pass


class GenieConfigError(GenieError):
    """A Genie config error occurred."""
    pass


class GenieConfigSectionError(GenieConfigError):
    """Error when getting a section from a Genie config which does not exist."""
    pass


class GenieConfigOptionError(GenieConfigError):
    """Error when getting an option from a Genie config section which does not
    exist."""
    pass


class GenieHTTPError(GenieError):
    """Error when sending a request to the server."""

    def __init__(self, response):
        super(GenieHTTPError, self) \
            .__init__('{}: {}, {}'.format(response.status_code,
                                          response.reason,
                                          response.text))
        self.response = response


class GenieJobError(GenieError):
    """A Genie job error occurred."""
    pass
