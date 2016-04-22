"""
genie.utils

This module contains utility functions.
"""


from __future__ import absolute_import, print_function, unicode_literals

import logging
import os
import pkg_resources
import six
import socket
import uuid

try:
    import eureq as requests
except ImportError:
    import requests

from .exceptions import GenieHTTPError


logger = logging.getLogger('com.netflix.genie.utils')


USER_AGENT_HEADER = {
    'user-agent': '/'.join([
        socket.getfqdn(),
        'nflx-genie-client',
        pkg_resources.get_distribution('nflx-genie-client').version
    ])
}


def call(url, method='get', headers=None, *args, **kwargs):
    """
    Wrap HTTP request calls to the Genie server.

    The request header will be updated to include 'user-agent'. If headers are
    passed in with 'user-agent', it will be overwritten.
    """

    headers = USER_AGENT_HEADER if headers is None \
        else dict(headers, **USER_AGENT_HEADER)
    resp = requests.request(method, url=url, headers=headers, *args, **kwargs)

    if not resp.ok:
        raise GenieHTTPError(resp)

    return resp


def is_file(path):
    """Checks if path is to an existing file."""

    return os.path.isfile(path)


def is_str(string):
    """Checks if arg is of string type."""

    return isinstance(string, six.string_types)


def str_to_list(string, delimiter=','):
    """
    Given a string arg, return a list split by delimiter. If the arg is not a
    string, returns the arg.

    Example:
        >>> str_to_list('a, b, c')
        ['a', 'b', 'c']
        >>> str_to_list(None)
        None

    Args:
        string (str): The string to convert to a list.
        delimiter (str, optional): The delimiter to use when splitting the string.

    Returns:
        list: A list of the string split by delimiter.
    """

    if string is not None and is_str(string):
        return [i.strip() for i in string.split(delimiter)]
    return string


def uuid_str():
    """
    Return a unique id as a string.

    Example:
        >>> uuid_str()
        '1234-abcd-4321'

    Returns:
        str: A unique id.
    """

    return str(uuid.uuid1())
