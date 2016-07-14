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

__author__ = 'amsharma'


class GenieException(Exception):
    """Exception class for errors for this module."""
    # Call the base class constructor with the parameters it needs
    def __init__(self, message, ecode=None):
        Exception.__init__(self, message)
        self.ecode = ecode