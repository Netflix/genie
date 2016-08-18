"""
genie.jobs.adapter.genie_2

This module implements Genie 2 adapter.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import base64
import json
import logging
import os

from functools import wraps
from multipledispatch import dispatch

from ..utils import (call,
                     is_str)

from ..jobs.utils import (is_attachment,
                          is_file)

from .genie_x import (GenieBaseAdapter,
                      substitute)

from ..exceptions import (GenieAttachmentError,
                          GenieHTTPError,
                          GenieJobError,
                          GenieJobNotFoundError,
                          GenieLogNotFoundError)

#jobs
from ..jobs.core import GenieJob
from ..jobs.hadoop import HadoopJob
from ..jobs.hive import HiveJob
from ..jobs.pig import PigJob
from ..jobs.presto import PrestoJob
from ..jobs.sqoop import SqoopJob


logger = logging.getLogger('com.netflix.genie.jobs.adapter.genie_2')

dispatch_ns = dict()

JSON_HEADERS = {
    'content-type':'application/json',
    'Accept':'application/json'
}


def assert_script(func):
    """Decorator to enforce script is set on a job."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wraps func."""

        job = args[0]
        if job.get('script') is None:
            raise GenieJobError('cannot run {} without specifying script' \
                                    .format(job.__class__.__name__))

        return func(*args, **kwargs)

    return wrapper


def set_jobname(func):
    """Decorator to update payload with script dependenvy."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wraps func."""

        payload = func(*args, **kwargs)

        job = args[0]
        script = job.get('script')

        # handle job name if not set
        if not job.get('job_name') and script:
            payload['name'] = os.path.basename(script) if is_file(script) \
                else script.replace('\n', ' ')[:40].strip()

        return payload

    return wrapper


def to_attachment(att):
    if is_str(att) and os.path.isfile(att):
        with open(att, 'r') as content_file:
            content = content_file.read()
        return dict(name=os.path.basename(att), data=base64.b64encode(content))
    elif isinstance(att, dict):
        try:
            return dict(name=att['name'], data=base64.b64encode(att['data']))
        except KeyError:
            raise GenieAttachmentError("in-line attachment is missing required keys ('name', 'data') ({})".format(att))
    raise GenieAttachmentError("cannot handle attachment '{}'".format(att))


class Genie2Adapter(GenieBaseAdapter):
    """Genie server 2"""

    JOBS_ENDPOINT = 'genie/v2/jobs'

    def __init__(self, conf=None):
        super(Genie2Adapter, self).__init__(conf=conf)

    def get_log(self, job_id, log, iterator=False, **kwargs):
        url = '{}/{}'.format(self.__url_for_job(job_id), log) \
            .replace('/genie/v2/jobs/', '/genie-jobs/', 1)

        try:
            response = call(method='get', url=url, stream=iterator, **kwargs)
            return response.iter_lines() if iterator else response.text
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                raise GenieLogNotFoundError("log not found at {}".format(url))
            raise

    def __url_for_job(self, job_id):
        return '{}/{}/{}'.format(self._conf.genie.url,
                                 Genie2Adapter.JOBS_ENDPOINT,
                                 job_id)

    @staticmethod
    def construct_base_payload(job):
        """Returns a base payload for the job."""

        # do this first because some jobs will apply some logic (like manipulating
        # dependencies) which need to be done before proceeding with building the
        # payload
        command_args = job.cmd_args

        attachments = list()
        dependencies = list()

        for dep in job.get('dependencies'):
            if is_attachment(dep):
                # attachment is an adhoc script so do parameter substitution
                if isinstance(dep, dict) \
                        and dep.get('data') \
                        and hasattr(job, 'DEFAULT_SCRIPT_NAME') \
                        and dep.get('name') == job.DEFAULT_SCRIPT_NAME:
                    dep['data'] = substitute(dep['data'], job.get('parameters'))
                attachments.append(to_attachment(dep))
            else:
                dependencies.append(dep)

        cluster_tag_mapping = job.get('cluster_tag_mapping')
        clusters = [
            dict(tags=cluster_tag_mapping.get(priority))
            for priority in sorted(cluster_tag_mapping.keys())
        ]

        payload = {
            'attachments': attachments,
            'clusterCriterias': [i for i in clusters if i.get('tags')],
            'commandArgs': job.get('command_arguments') or command_args,
            'commandCriteria': job.get('command_tags'),
            'description': job.get('description'),
            'disableLogArchival': not job.get('archive'),
            'email': job.get('email'),
            'envPropFile': job.get('setup_file'),
            'fileDependencies': dependencies,
            'group': job.get('group'),
            'id': job.get('job_id'),
            'name': job.get('job_name'),
            'tags': job.get('tags'),
            'user': job.get('username'),
            'version': job.get('job_version')
        }

        # command tags not set, use default
        if not payload.get('commandCriteria'):
            payload['commandCriteria'] = job.default_command_tags

        # cluster tags not set, use default
        if not [c.get('tags') for c in payload.get('clusterCriterias', []) \
                if len(c.get('tags') or []) > 0]:
            payload['clusterCriterias'] = [{'tags': job.default_cluster_tags}]

        return payload

    def get(self, job_id, path=None, **kwargs):
        """Get information."""

        url = self.__url_for_job(job_id)
        if path:
            url = '{}/{}'.format(url, path.lstrip('/'))

        try:
            return call(method='get', url=url, **kwargs).json()
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                raise GenieJobNotFoundError("job not found at {}".format(url))
            raise

    def get_info_for_rj(self, job_id, *args, **kwargs):
        """
        Get information for RunningJob object.
        """

        data = self.get(job_id, timeout=10)

        file_dependencies = data.get('fileDependencies')

        return {
            'application_name': data.get('applicationName'),
            'archive_location': data.get('archiveLocation'),
            'attachments': data.get('attachments'),
            'client_host': data.get('clientHost'),
            'cluster_id': data.get('executionClusterId'),
            'cluster_name': data.get('executionClusterName'),
            'command_args': data.get('commandArgs'),
            'command_id': data.get('commandId'),
            'command_name': data.get('commandName'),
            'created': data.get('created'),
            'description': data.get('description'),
            'disable_archive': data.get('disableLogArchival'),
            'email': data.get('email'),
            'file_dependencies': file_dependencies.split(',') \
                if file_dependencies is not None else [],
            'finished': data.get('finished'),
            'group': data.get('group'),
            'id': data.get('id'),
            'job_link': data.get('killURI'),
            'json_link': data.get('killURI'),
            'kill_uri': data.get('killURI'),
            'name': data.get('name'),
            'output_uri': data.get('outputURI'),
            'request_data': dict(),
            'setup_file': data.get('envPropFile'),
            'started': data.get('started'),
            'status': data.get('status'),
            'status_msg': data.get('statusMsg'),
            'tags': data.get('tags'),
            'updated': data.get('updated'),
            'user': data.get('user'),
            'version': data.get('version')
        }


    def get_genie_log(self, job_id, **kwargs):
        """Get a genie log for a job."""

        return self.get_log(job_id, 'cmd.log', **kwargs)

    def get_status(self, job_id):
        """Get job status."""

        return self.get(job_id, path='status', timeout=10).get('status')

    def get_stderr(self, job_id, **kwargs):
        """Get a stderr log for a job."""

        return self.get_log(job_id, 'stderr.log', **kwargs)

    def get_stdout(self, job_id, **kwargs):
        """Get a stdout log for a job."""

        return self.get_log(job_id, 'stdout.log', **kwargs)

    def kill_job(self, job_id=None, kill_uri=None):
        """Kill a job."""

        url = kill_uri if kill_uri is not None else self.__url_for_job(job_id)

        try:
            return call(method='delete', url=url, timeout=10)
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                raise GenieJobNotFoundError("job not found at {}".format(url))
            raise

    def submit_job(self, job):
        """Submit a job execution to the server."""

        payload = {
            key: value for key, value in get_payload(job).iteritems() \
            if value is not None \
                and value != [] \
                and value != {} \
                and value != ''
        }

        if payload.get('fileDependencies'):
            payload['fileDependencies'] = ','.join(payload['fileDependencies'])

        logger.debug('payload to genie 2:')
        logger.debug(json.dumps(payload,
                                sort_keys=True,
                                indent=4,
                                separators=(',', ': ')))
        call(method='post',
             url='{}/{}'.format(job._conf.genie.url, Genie2Adapter.JOBS_ENDPOINT),
             timeout=30,
             data=json.dumps(payload),
             headers=JSON_HEADERS)


@dispatch(GenieJob, namespace=dispatch_ns)
def get_payload(job):
    """Construct payload for GenieJob -> Genie 2."""

    try:
        payload = Genie2Adapter.construct_base_payload(job)
    except GenieJobError:
        raise GenieJobError(
            'trying to run GenieJob without explicitly setting command line arguments (use .command_arguments())')

    if not payload.get('commandCriteria'):
        raise GenieJobError('trying to run GenieJob without specifying command tags')

    return payload


@dispatch((HadoopJob, HiveJob, PigJob, PrestoJob), namespace=dispatch_ns)
@set_jobname
@assert_script
def get_payload(job):
    """Construct payload for jobs -> Genie 2."""

    return Genie2Adapter.construct_base_payload(job)


@dispatch((SqoopJob), namespace=dispatch_ns)
def get_payload(job):
    """Construct payload for SqoopJob -> Genie 2."""

    return Genie2Adapter.construct_base_payload(job)
