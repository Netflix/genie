"""
genie.exceptions

This module implements the set of Genie exceptions.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging


logger = logging.getLogger('com.netflix.genie.exceptions')


class GenieError(Exception):
    """Base exception for Genie."""
    pass


class GenieAdapterError(GenieError):
    """Error when trying to construct a JSON payload for a job."""
    pass


class GenieAttachmentError(GenieError):
    """Error when trying to add an attachment for a job."""
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
        super(GenieHTTPError, self).__init__('{}: {}, {}' \
                                                 .format(response.status_code,
                                                         response.reason,
                                                         response.content))
        self.response = response


class GenieJobError(GenieError):
    """A Genie job error occurred."""
    pass


class GenieJobNotFoundError(GenieError):
    """Error when job is not found."""
    pass


class GenieLogNotFoundError(GenieError):
    """Error when a log is not found."""
    pass


class JobTimeoutError(GenieError):
    """Error when a job runs longer than a specified timeout."""
    pass
