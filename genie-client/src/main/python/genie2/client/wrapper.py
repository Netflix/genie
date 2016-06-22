"""
  Copyright 2015 Netflix, Inc.

     Licensed under the Apache License, Version 2.0 (the "License");
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

     Unless required by applicable law or agreed to in writing, software
     distributed under the License is distributed on an "AS IS" BASIS,
     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     See the License for the specific language governing permissions and
     limitations under the License.
"""
import logging
import threading
import time
import urllib2
import genie2.client.api_client
import genie2.client.V2Api

__author__ = 'jcistaro'

DEBUG = logging.getLogger(__name__).debug
ERROR = logging.getLogger(__name__).error
INFO = logging.getLogger(__name__).info
WARN = logging.getLogger(__name__).warn


class Genie2(object):  # pylint: disable=R0903
    """Wrapper for Genie V2Api"""

    def __init__(self, service_base_url, retry_policy=None):
        # pylint: disable=R0913
        self._lock = threading.RLock()
        self._service_base_url = service_base_url
        self._cached_url = None
        self._retry_policy = retry_policy if retry_policy is not None else \
            RetryPolicy(tries=1)

    def __getattr__(self, name):  # pylint: disable=R0912
        if name.startswith('__'):
            raise AttributeError
        parts = name.split('_')
        if len(parts) == 0:
            camel = ''
        else:
            camel = parts[0]
            for part in parts[1:]:
                camel += part.capitalize()

        def wrap(*args, **kwargs):
            """Wrap the function"""
            # pylint: disable=R0912
            if 'retry_policy' in kwargs:
                retry_policy = kwargs['retry_policy']
                del kwargs['retry_policy']
            else:
                retry_policy = self._retry_policy
            delay = retry_policy.delay
            for remaining in range(retry_policy.tries - 1, -1, -1):
                try:
                    service_base_url = self.get_service_base_url()
                    client = genie2.client.api_client.ApiClient(
                        api_server=service_base_url)
                    api = genie2.client.V2Api.V2Api(client)
                    if not hasattr(api, camel):
                        raise AttributeError(
                            ('genie2.client.V2Api.V2Api object has no ' +
                             "attribute '%s' to be wrapped by '%s'" %
                             (camel, name)))
                    return getattr(api, camel)(*args, **kwargs)
                except AttributeError:
                    raise
                except urllib2.HTTPError as http_error:
                    DEBUG(http_error)
                    if http_error.code == 404 and retry_policy.none_on_404 is True:
                        return None
                    if http_error.code in range(500, 600):
                        self.reset_service_base_url()
                    if remaining < 1 or http_error.code in \
                            retry_policy.no_retry_http_codes:
                        raise Exception(
                            ('V2Api call to %s failed due to ' +
                             'error code=[%s] because=[%s]') %
                            (camel, str(http_error.code), http_error.read())
                        )
                except urllib2.URLError as url_error:
                    DEBUG(url_error)
                    self.reset_service_base_url()
                    if remaining < 1:
                        raise Exception(
                            'V2Api call to %s failed with reason=[%s]' %
                            (camel, url_error.reason))
                except Exception as error:  # pylint: disable=W0703
                    DEBUG(error)
                    self.reset_service_base_url()
                    if remaining < 1:
                        raise
                time.sleep(delay)  # wait...
                delay *= retry_policy.backoff  # make future wait longer

        return wrap

    def get_service_base_url(self):
        """Get service base url."""
        with self._lock:
            if not hasattr(self._service_base_url, '__call__'):
                return self._service_base_url
            else:
                if self._cached_url is None:
                    self._cached_url = self._service_base_url()
                return self._cached_url

    def reset_service_base_url(self):
        """Get service base url."""
        with self._lock:
            if hasattr(self._service_base_url, '__call__'):
                self._cached_url = None


class RetryPolicy(object):
    """Retry Policy."""
    # pylint: disable=R0903

    tries = property(lambda self: getattr(self, '_tries'))
    delay = property(lambda self: getattr(self, '_delay'))
    backoff = property(lambda self: getattr(self, '_backoff'))
    no_retry_http_codes = property(
        lambda self: getattr(self, '_no_retry_http_codes'))
    none_on_404 = property(lambda self: getattr(self, '_none_on_404'))

    def __init__(self, tries=6, delay=3, backoff=2, no_retry_http_codes=None,
                 none_on_404=False):
        # pylint: disable=R0913
        if not isinstance(backoff, (int, long)) or backoff <= 1:
            raise ValueError('backoff must be an integer greater than 1')
        if not isinstance(tries, (int, long)) or tries <= 0:
            raise ValueError('tries must an integer greater than 0')
        if not isinstance(delay, (int, long)) or delay <= 0:
            raise ValueError("delay must be greater than 0")
        self._tries = tries
        self._delay = delay
        self._backoff = backoff
        self._no_retry_http_codes = no_retry_http_codes \
            if no_retry_http_codes is not None else []
        self._none_on_404 = none_on_404
