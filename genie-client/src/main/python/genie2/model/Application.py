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


class Application:

    def __init__(self):
        self.swaggerTypes = {
            'status': 'str',
            'jars': 'Set[str]',
            'envPropFile': 'str',
            'tags': 'Set[str]',
            'configs': 'Set[str]',
            'name': 'str',
            'user': 'str',
            'version': 'str',
            'id': 'str',
            'created': 'datetime',
            'updated': 'datetime'
        }

        #The current status of this application
        self.status = None  # str
        #Any jars needed to run this application
        self.jars = None  # Set[str]
        #Users can specify a property file location with environment variables
        self.envPropFile = None  # str
        #Reference to all the tags associated with this application.
        self.tags = None  # Set[str]
        #All the configuration files needed for this application
        self.configs = None  # Set[str]
        #Name of this entity
        self.name = None  # str
        #User who created this entity
        self.user = None  # str
        #Version number for this entity
        self.version = None  # str
        #id
        self.id = None  # str
        #created
        self.created = None  # datetime
        #updated
        self.updated = None  # datetime
