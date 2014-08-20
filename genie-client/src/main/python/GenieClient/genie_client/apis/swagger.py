# !/usr/bin/env python
"""Wordnik.com's Swagger generic API client. This client handles the client-
server communication, and is invariant across implementations. Specifics of
the methods and models for each application are generated from the Swagger
templates."""

import re
import urllib
import urllib2
import json
import datetime
import dateutil.parser

from genie_client.models import *

class ApiClient:
    """Generic API client for Swagger client library builds"""

    def __init__(self, api_key=None, api_server=None):
        if api_key is None:
            raise Exception('You must pass an apiKey when instantiating the '
                            'APIClient')
        self.api_key = api_key
        self.api_server = api_server
        self.cookie = None

    def callApi(self, resource_path, method, query_params, post_data,
                header_params=None):

        url = self.api_server + resource_path
        headers = {}
        if header_params:
            for param, value in header_params.iteritems():
                headers[param] = value

        headers['api_key'] = self.api_key
        headers['Accept'] = 'application/json'

        if self.cookie:
            headers['Cookie'] = self.cookie

        data = None

        if query_params:
            # Need to remove None values, these should not be sent
            sent_query_params = {}
            for param, value in query_params.items():
                if value is not None:
                    sent_query_params[param] = value
            url = url + '?' + urllib.urlencode(sent_query_params)

        if method in ['GET']:

            # Options to add statements later on and for compatibility
            pass

        elif method in ['POST', 'PUT', 'DELETE']:

            if post_data:
                headers['Content-Type'] = 'application/json'
                data = self.sanitizeForSerialization(post_data)
                data = json.dumps(data)
                print data

        else:
            raise Exception('Method ' + method + ' is not recognized.')

        request = MethodRequest(method=method, url=url, headers=headers,
                                data=data)

        # Make the request
        response = urllib2.urlopen(request)
        if 'Set-Cookie' in response.headers:
            self.cookie = response.headers['Set-Cookie']
        string = response.read()

        try:
            data = json.loads(string)
        except ValueError:  # PUT requests don't return anything
            data = None

        return data

    def toPathValue(self, obj):
        """Convert a string or object to a path-friendly value
        Args:
            obj -- object or string value
        Returns:
            string -- quoted value
        """
        if type(obj) == list:
            return urllib.quote(','.join(obj))
        else:
            return urllib.quote(str(obj))

    def sanitizeForSerialization(self, obj):
        """Dump an object into JSON for POSTing."""

        if type(obj) == type(None):
            return None
        elif type(obj) in [str, int, long, float, bool, unicode]:
            return obj
        elif type(obj) == list:
            return [self.sanitizeForSerialization(sub_obj) for sub_obj in obj]
        elif type(obj) == set:
            return [self.sanitizeForSerialization(sub_obj) for sub_obj in obj]
        elif type(obj) == datetime.datetime:
            # Genie server always handles things in GMT
            return obj.isoformat()
        else:
            if type(obj) == dict:
                obj_dict = obj
            else:
                obj_dict = obj.__dict__
            return {key: self.sanitizeForSerialization(val)
                    for (key, val) in obj_dict.iteritems()
                    if key != 'swaggerTypes' and not key[0:2] == '__'}

    def deserialize(self, obj, obj_class):
        """Derialize a JSON string into an object.

        Args:
            obj -- string or object to be deserialized
            objClass -- class literal for deserialzied object, or string
                of class name
        Returns:
            object -- deserialized object"""

        if type(obj) == type(None):
            return None

        # Have to accept objClass as string or actual type. Type could be a
        # native Python type, or one of the model classes.
        if type(obj_class) == str:
            if 'Array[' in obj_class:
                match = re.match('Array\[(.*)\]', obj_class)
                sub_class = match.group(1)
                return [self.deserialize(sub_obj, sub_class) for sub_obj in obj]
            if 'Set[' in obj_class:
                match = re.match('Set\[(.*)\]', obj_class)
                sub_class = match.group(1)
                return [self.deserialize(sub_obj, sub_class) for sub_obj in obj]

            if obj_class in ['int', 'float', 'long', 'dict', 'list', 'str', 'bool', 'datetime']:
                obj_class = eval(obj_class)
            else:  # not a native type, must be model class
                obj_class = eval(obj_class + '.' + obj_class)

        if obj_class in [int, long, float, dict, list, str, bool]:
            return obj_class(obj)
        elif obj_class == datetime:
            return dateutil.parser.parse(obj)

        instance = obj_class()

        for attr, attrType in instance.swaggerTypes.iteritems():
            if obj is not None and attr in obj and type(obj) in [list, dict]:
                value = obj[attr]
                if attrType in ['str', 'int', 'long', 'float', 'bool']:
                    attrType = eval(attrType)
                    try:
                        value = attrType(value)
                    except UnicodeEncodeError:
                        value = unicode(value)
                    except TypeError:
                        value = value
                    setattr(instance, attr, value)
                elif attrType == 'datetime':
                    setattr(instance, attr, dateutil.parser.parse(value))
                elif 'list[' in attrType:
                    match = re.match('list\[(.*)\]', attrType)
                    sub_class = match.group(1)
                    subValues = []
                    if not value:
                        setattr(instance, attr, None)
                    else:
                        for subValue in value:
                            subValues.append(self.deserialize(subValue,
                                                              sub_class))
                    setattr(instance, attr, subValues)
                elif 'Set[' in attrType:
                    match = re.match('Set\[(.*)\]', attrType)
                    sub_class = match.group(1)
                    subValues = []
                    if not value:
                        setattr(instance, attr, None)
                    else:
                        for subValue in value:
                            subValues.append(self.deserialize(subValue,
                                                              sub_class))
                    setattr(instance, attr, subValues)
                elif 'List[' in attrType:
                    match = re.match('List\[(.*)\]', attrType)
                    sub_class = match.group(1)
                    subValues = []
                    if not value:
                        setattr(instance, attr, None)
                    else:
                        for subValue in value:
                            subValues.append(self.deserialize(subValue,
                                                              sub_class))
                    setattr(instance, attr, subValues)
                elif 'Array[' in attrType:
                    match = re.match('Array\[(.*)\]', attrType)
                    sub_class = match.group(1)
                    subValues = []
                    if not value:
                        setattr(instance, attr, None)
                    else:
                        for subValue in value:
                            subValues.append(self.deserialize(subValue,
                                                              sub_class))
                    setattr(instance, attr, subValues)
                else:
                    setattr(instance, attr, self.deserialize(value,
                                                             obj_class))

        return instance


class MethodRequest(urllib2.Request):
    def __init__(self, *args, **kwargs):
        """Construct a MethodRequest. Usage is the same as for
        `urllib2.Request` except it also takes an optional `method`
        keyword argument. If supplied, `method` will be used instead of
        the default."""

        if 'method' in kwargs:
            self.method = kwargs.pop('method')
        return urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return getattr(self, 'method', urllib2.Request.get_method(self))


