"""
genie.utils

This module contains utility functions.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import logging
import pkg_resources
import six
import socket
import time
import uuid

from functools import wraps

import requests

from .auth import AuthHandler

from requests.exceptions import Timeout, ConnectionError
from .exceptions import GenieHTTPError


logger = logging.getLogger('com.netflix.pygenie.utils')


USER_AGENT_HEADER = {
    'user-agent': '/'.join([
        socket.getfqdn(),
        'nflx-genie-client',
        pkg_resources.get_distribution('nflx-genie-client').version
    ])
}


class DotDict(dict):
    """
    Allow dictionary keys to be retrieved as attribtues. Used for the genie2 to
    genie 3 migration.
    """
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


def call(url, method='get', headers=None, raise_not_status=None,
         none_on_404=False, auth_handler=None, retry_status_codes=None,
         attempts=4, backoff=3,
         *args, **kwargs):
    """
    Wrap HTTP request calls to the Genie server.

    The request header will be updated to include 'user-agent'. If headers are
    passed in with 'user-agent', it will be overwritten.

    Args:
        method (str): the HTTP method to make
        headers (dict): headers to pass in during the request
        raise_not_status (int): raise GenieHTTPError if this status is not
            returned by genie.
        none_on_404 (bool): return None if a 404 if returned instead of raising
            GenieHTTPError.
    """

    assert isinstance(retry_status_codes, (list, int)) \
            or retry_status_codes is None, \
        'retry_status_codes should be an int or list of ints'

    retry_status_codes = retry_status_codes or list()
    if isinstance(retry_status_codes, int):
        retry_status_codes = [retry_status_codes]
    retry_status_codes = set(retry_status_codes + [503])

    auth_handler = auth_handler or AuthHandler()

    headers = USER_AGENT_HEADER if headers is None \
        else dict(headers, **USER_AGENT_HEADER)

    logger.debug('"%s %s"', method.upper(), url)
    logger.debug('headers: %s', headers)

    errors = list()
    for i in range(attempts):
        try:
            resp = requests.request(method,
                                    url=url,
                                    headers=headers,
                                    auth=auth_handler.auth,
                                    *args,
                                    **kwargs)
        except (ConnectionError, Timeout, socket.timeout) as err:
            errors.append(err)
            resp = None

        if i < attempts - 1 and (resp is None or resp.status_code in retry_status_codes):
            msg = ''
            if resp is not None:
                msg = '-> {}: {}'.format(resp.status_code, resp.text)
            logger.warning('attempt %s %s', i + 1, msg)
            time.sleep(i * backoff)
        else:
            break

    if resp is not None:
        # Allow us to return None if we receive a 404
        if resp.status_code == 404 and none_on_404:
            return None
        if not resp.ok:
            raise GenieHTTPError(resp)
        # Raise GenieHTTPError if a particular status code was not returned
        if raise_not_status and resp.status_code != raise_not_status:
            raise GenieHTTPError(resp)
        return resp
    elif len(errors) > 0:
        raise errors[-1]


def convert_to_unicode(value):
    """Convert value to unicode."""

    if is_str(value) and not isinstance(value, unicode):
        return value.decode('utf-8')

    return value


def unicodify(func):
    """
    Decorator to convert all string args and kwargs to unicode.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wraps func."""

        args = tuple(convert_to_unicode(i) for i in args)
        kwargs = {convert_to_unicode(key): convert_to_unicode(value) \
            for key, value in kwargs.items()}

        return func(*args, **kwargs)

    return wrapper


def dttm_to_epoch(date_str, frmt='%Y-%m-%dT%H:%M:%SZ'):
    """Convert a date string to epoch seconds."""

    return int((datetime.datetime.strptime(date_str, frmt) -
                datetime.datetime(1970, 1, 1)).total_seconds())


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

    return unicode(uuid.uuid1())
