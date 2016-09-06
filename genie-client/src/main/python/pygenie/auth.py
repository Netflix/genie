"""
genie.auth

This module implements handling authentication for HTTP requests.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import requests

from .conf import GenieConf


logger = logging.getLogger('com.netflix.genie.auth')


class AuthHandler(object):
    """
    Handles creating authentication objects to use for HTTP requests.

    If an auth object's path is specifed in the configuration using
    the "genie.auth" option, the handler will create an instance of the auth
    object.

    The auth object must implement 2 methods (see HTTPBasicGenieAuth below for
    an example):
    1. __init__() must accept a GenieConf object as a kwarg, and options specified
        in the "genie_auth" section will be passed as kwargs.
        Example: cls.__init__(self, conf=None, **kwargs)
    2. __call__() must accept a request object as an arg.
        Example: cls.__call__(self, req)

    The auth instance will be passed to request calls via the "auth" keyword
    argument.

    For example, if the following is defined in the configuration:
    [genie]
    auth='mylib.mymod.MyAuth'

    The AuthHandler will create an instance of 'mylib.mymod.MyAuth'. This instance
    will then be passed to every request using the 'auth' kwarg. The auth object
    has access to the GenieConf in case additional configurations for
    authentication need to be set, they can be set/accessed via the configuration.

    For more details on auth object implementation:
    https://github.com/kennethreitz/requests/blob/master/requests/auth.py
    """

    def __init__(self, conf=None):
        self._conf = conf or GenieConf()

        self.auth_name = self._conf.get('genie.auth')
        self.auth = None

        if self.auth_name:
            mod_name, cls_name = self.auth_name.rsplit('.', 1)
            logger.debug('loading module: %s', mod_name)
            mod = __import__(mod_name, fromlist=[cls_name])
            try:
                cls = getattr(mod, cls_name)
                logger.debug('setting auth to: %s', cls)
                self.auth = cls(conf=self._conf, **self._conf.genie_auth.to_dict())
            except AttributeError:
                logger.warning('%s not found in module %s (auth set to None).',
                               cls_name,
                               mod_name)


class HTTPBasicGenieAuth(requests.auth.HTTPBasicAuth):
    """
    Example authentication object using basic HTTP autentication with username and
    password.
    """

    def __init__(self, conf=None, **kwargs):
        self._conf = conf or GenieConf()

        # access settings for authentication via configuration
        self.username = self._conf.get('genie_auth.username')
        self.password = self._conf.get('genie_auth.password')

        super(HTTPBasicGenieAuth, self).__init__(self.username, self.password)

    def __call__(self, r):
        # this doesn't need to be implemented since it just call's super
        # but here for example.
        return super(HTTPBasicGenieAuth, self).__call__(r)
