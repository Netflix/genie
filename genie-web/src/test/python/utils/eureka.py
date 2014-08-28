# #
#
# Copyright 2014 Netflix, Inc.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#
##

import os
import json
import urllib2
import random


class EurekaClient(object):
    # Users can either set the EUREKA_URL environment variable
    # or pass it as a parameter (which takes precedence)
    def __init__(self, eureka_url=None):
        self._eureka_url = eureka_url
        if self._eureka_url is None or len(self._eureka_url) == 0:
            self._eureka_url = os.getenv('EUREKA_URL')

    # If appName is not set, then use NETFLIX_APP env, and finally default to 'genie'
    def get_instances(self, app_name=None, status='UP'):
        # if appName is not set, try to infer it from the environment - else default to 'genie'
        if app_name is None or len(app_name) == 0:
            app_name = os.getenv("NETFLIX_APP")
        if app_name is None or len(app_name) == 0:
            app_name = 'genie2'

        # ensure that we can find Eureka
        if self._eureka_url is None or len(self._eureka_url) == 0:
            raise RuntimeError("EUREKA_URL is not provided via env or constructor")

        # get the response from Eureka
        rest_url = self._eureka_url + '/' + app_name
        req = urllib2.Request(url=rest_url)
        req.add_header('Accept', 'application/json')
        assert req.get_method() == 'GET'
        response = urllib2.urlopen(req)

        # parse the json response
        json_str = response.read()
        json_obj = json.loads(json_str)

        # get a list of all instances
        instances = json_obj['application']['instance']

        # check to see if this is a list or a singleton
        is_list = isinstance(instances, list)

        # filter out the instances as specified by status
        instances_list = []
        if is_list:
            for i in instances:
                if i['status'] == status:
                    instances_list.append(i)
        else:
            # singleton instance
            if instances['status'] == status:
                instances_list.append(instances)

        # ensure that we have at least 1 instance that is UP
        assert len(instances_list) > 0, \
            "No " + app_name + " instances found that were " + status + " on " + self._eureka_url

        # return each one
        return instances_list

    # If the SERVICE_BASE_URL environment variable is set, return it - e.g. http://localhost:7001
    # Else use Eureka to find an instance that is UP   
    def get_service_base_url(self, app_name=None):
        service_url = os.getenv('SERVICE_BASE_URL')
        if service_url is not None and len(service_url) != 0:
            print "Returning SERVICE_BASE_URL provided by environment variable:", service_url
            print
            return service_url
        else:
            print "Getting UP instance from Eureka: " + self._eureka_url
            print

        instances_up = self.get_instances(app_name, status='UP')

        # pick a random one
        instance = instances_up[random.randrange(0, len(instances_up), 1)]

        service_url = 'http://' + instance['hostName'] + ':' + instance['port']['$']
        return service_url

    # Use Eureka to find instances that are OUT_OF_SERVICE
    def get_oos_instances(self, app_name=None):
        return self.get_instances(app_name, status='OUT_OF_SERVICE')

    # Use Eureka to find instances that are UP
    def get_up_instances(self, app_name=None):
        return self.get_instances(app_name, status='UP')


if __name__ == "__main__":
    client = EurekaClient()

    print "Getting base URL for Genie Service from Eureka:"
    print client.get_service_base_url()
    print

    print "Getting list of all Genie OOS instances"
    print client.get_oos_instances()
