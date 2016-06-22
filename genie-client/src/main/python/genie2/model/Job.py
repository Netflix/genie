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


class Job:

    def __init__(self):
        self.swaggerTypes = {
            'hostName': 'str',
            'status': 'str',
            'applicationId': 'str',
            'group': 'str',
            'statusMsg': 'str',
            'attachments': 'Set[FileAttachment]',
            'fileDependencies': 'str',
            'envPropFile': 'str',
            'commandArgs': 'str',
            'email': 'str',
            'outputURI': 'str',
            'executionClusterName': 'str',
            'executionClusterId': 'str',
            'commandName': 'str',
            'commandId': 'str',
            'finished': 'datetime',
            'clientHost': 'str',
            'exitCode': 'int',
            'archiveLocation': 'str',
            'applicationName': 'str',
            'description': 'str',
            'forwarded': 'bool',
            'processHandle': 'int',
            'killURI': 'str',
            'disableLogArchival': 'bool',
            'tags': 'Set',
            'clusterCriterias': 'list[ClusterCriteria]',
            'commandCriteria': 'Set[str]',
            'started': 'datetime',
            'name': 'str',
            'user': 'str',
            'version': 'str',
            'id': 'str',
            'created': 'datetime',
            'updated': 'datetime'
        }

        #The host where the job is being run.
        self.hostName = None  # str
        #The current status of the job.
        self.status = None  # str
        #Id of the application that this job should use to run.
        self.applicationId = None  # str
        #group name of the user who submitted this job
        self.group = None  # str
        #A status message about the job.
        self.statusMsg = None  # str
        #Attachments sent as a part of job request.
        self.attachments = None  # Set[FileAttachment]
        #Dependent files for this job to run.
        self.fileDependencies = None  # str
        #Path to a shell file which is sourced before job is run.
        self.envPropFile = None  # str
        #Command line arguments for the job
        self.commandArgs = None  # str
        #Email address to send notifications to on job completion.
        self.email = None  # str
        #The URI where to find job output.
        self.outputURI = None  # str
        #Name of the cluster where the job is run
        self.executionClusterName = None  # str
        #Id of the cluster where the job is run
        self.executionClusterId = None  # str
        #Name of the command that this job should run.
        self.commandName = None  # str
        #Id of the command that this job should run.
        self.commandId = None  # str
        #The end time of the job.
        self.finished = None  # long
        #The host of the client submitting the job.
        self.clientHost = None  # str
        #The exit code of the job.
        self.exitCode = None  # int
        #Where the logs were archived in S3.
        self.archiveLocation = None  # str
        #Name of the application that this job should use to run.
        self.applicationName = None  # str
        #Description specified for the job
        self.description = None  # str
        #Whether this job was forwared or not.
        self.forwarded = None  # bool
        #The process handle.
        self.processHandle = None  # int
        #The URI to use to kill the job.
        self.killURI = None  # str
        #Boolean variable to decide whether job should be archived after it finishes.
        self.disableLogArchival = None  # bool
        #Reference to all the tags associated with this job.
        self.tags = None  # Set
        #List of criteria containing tags to use to pick a cluster to run this job
        self.clusterCriterias = None  # list[ClusterCriteria]
        #List of criteria containing tags to use to pick a command to run this job
        self.commandCriteria = None  # Set[str]
        #The start time of the job.
        self.started = None  # long
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
