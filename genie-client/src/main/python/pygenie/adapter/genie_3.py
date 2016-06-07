"""
genie.jobs.adapter.genie_3

This module implements Genie 3 adapter.

"""


from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import os

from functools import wraps
from multipledispatch import dispatch

from ..auth import AuthHandler
from ..utils import (call,
                     is_str)

from ..jobs.utils import (is_attachment,
                          is_file)

from .genie_x import (GenieBaseAdapter,
                      substitute)

from ..exceptions import (GenieAttachmentError,
                          GenieHTTPError,
                          GenieJobError,
                          GenieJobNotFoundError)

#jobs
from ..jobs.core import GenieJob
from ..jobs.hadoop import HadoopJob
from ..jobs.hive import HiveJob
from ..jobs.pig import PigJob
from ..jobs.presto import PrestoJob


logger = logging.getLogger('com.netflix.genie.jobs.adapter.genie_3')

dispatch_ns = dict()


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
    """Decorator to update job name with script."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wraps func."""

        payload = func(*args, **kwargs)

        job = args[0]
        script = job.get('script')

        # handle job name if not set
        if not job.get('job_name'):
            payload['name'] = os.path.basename(script) if is_file(script) \
                else script.replace('\n', ' ')[:40].strip()

        return payload

    return wrapper


def to_attachment(att):
    if is_str(att) and os.path.isfile(att):
        return (unicode(os.path.basename(att)), open(att, 'rb'))
    elif is_str(att) and os.path.isdir(att):
        _files = list()
        for local_file in [os.path.join(att, d) for d in os.listdir(att)]:
            if os.path.isfile(local_file) and \
                    os.path.getsize(local_file) > 0 and \
                    not os.path.basename(local_file).startswith('.'):
                _files.append(
                    (unicode(os.path.basename(local_file)), open(local_file, 'rb'))
                )
        return _files
    elif isinstance(att, dict):
        try:
            return (unicode(att['name']), unicode(att['data']))
        except KeyError:
            raise GenieAttachmentError(
                "in-line attachment is missing " \
                "required keys ('name', 'data') ({})".format(att))
    raise GenieAttachmentError("cannot handle attachment '{}'".format(att))


class Genie3Adapter(GenieBaseAdapter):
    """Genie server 3"""

    JOBS_ENDPOINT = 'api/v3/jobs'

    def __init__(self, conf=None):
        super(Genie3Adapter, self).__init__(conf=conf)
        self.auth_handler = AuthHandler(conf=conf)

    def __get_log(self, job_id, log, iterator=False, **kwargs):
        url = '{}/output/{}'.format(self.__url_for_job(job_id), log)

        try:
            response = call(method='get',
                            url=url,
                            stream=iterator,
                            auth_handler=self.auth_handler,
                            **kwargs)
            return response.iter_lines() if iterator else response.text
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                raise GenieJobNotFoundError("job id '{}' not found at {}." \
                    .format(job_id, url))
            raise

    def __url_for_job(self, job_id):
        return '{}/{}/{}'.format(self.conf.genie.url,
                                 Genie3Adapter.JOBS_ENDPOINT,
                                 job_id)

    @staticmethod
    def construct_base_payload(job):
        """Returns a base payload for the job."""

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
                att = to_attachment(dep)
                if isinstance(att, list):
                    attachments.extend(att)
                else:
                    attachments.append(att)
            else:
                dependencies.append(dep)

        payload = {
            'applications': job.get('application_ids'),
            'attachments': attachments,
            'clusterCriterias': [
                {'tags': job.get('cluster_tags')}
            ],
            'commandArgs': job.get('command_arguments') or job.cmd_args,
            'commandCriteria': job.get('command_tags'),
            'dependencies': dependencies,
            'description': job.get('description'),
            'disableLogArchival': not job.get('archive'),
            'email': job.get('email'),
            'group': job.get('group'),
            'id': job.get('job_id'),
            'name': job.get('job_name'),
            'setupFile': job.get('setup_file'),
            'tags': job.get('tags'),
            'timeout': job.get('timeout'),
            'user': job.get('username'),
            'version': job.get('job_version')
        }

        # command tags not set, use default
        if not payload.get('commandCriteria'):
            payload['commandCriteria'] = job.default_command_tags

        # cluster tags not set, use default
        if not [c.get('tags') for c in payload.get('clusterCriterias', []) \
                if len(c.get('tags', [])) > 0]:
            payload['clusterCriterias'] = [{'tags': job.default_cluster_tags}]

        return payload

    def get(self, job_id, path=None, if_not_found=None):
        """
        Get information for a job.

        Args:
            job_id (str): The job id.
            path (str, optional): Path to more job information (cluster, requests,
                etc)
            if_not_found (optional): If the job id is to a job that cannot be
                found, if if_not_found is not None will return if_not_found
                instead of raising error.

        Returns:
            json: JSON response data.
        """

        url = self.__url_for_job(job_id)
        if path:
            url = '{}/{}'.format(url, path.lstrip('/'))

        try:
            return call(method='get',
                        url=url,
                        auth_handler=self.auth_handler).json()
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                msg = "job id '{}' not found at {}.".format(job_id, url)
                if if_not_found is not None:
                    logger.warning(msg)
                    return if_not_found
                raise GenieJobNotFoundError(msg)
            raise

    def get_info_for_rj(self, job_id, *args, **kwargs):
        """
        Get information for RunningJob object.
        """

        data = self.get(job_id)
        request_data = self.get(job_id, path='request')
        application_data = self.get(job_id,
                                    path='applications',
                                    if_not_found=list())
        cluster_data = self.get(job_id,
                                path='cluster',
                                if_not_found=dict())
        command_data = self.get(job_id,
                                path='command',
                                if_not_found=dict())
        execution_data = self.get(job_id,
                                  path='execution',
                                  if_not_found=dict())

        link = data.get('_links', {}).get('self', {}).get('href')

        return {
            'application_name': ','.join(a.get('id') for a in application_data),
            'archive_location': data.get('archiveLocation'),
            'attachments': None,
            'client_host': execution_data.get('hostName'),
            'cluster_id': cluster_data.get('id'),
            'cluster_name': cluster_data.get('name'),
            'command_args': data.get('commandArgs'),
            'command_id': command_data.get('id'),
            'command_name': command_data.get('name'),
            'created': data.get('created'),
            'description': data.get('description'),
            'disable_archive': request_data.get('disableLogArchival'),
            'email': request_data.get('email'),
            'file_dependencies': request_data.get('dependencies'),
            'finished': data.get('finished'),
            'group': request_data.get('group'),
            'id': data.get('id'),
            'json_link': link,
            'kill_uri': link,
            'name': data.get('name'),
            'output_uri': data.get('_links', {}).get('output', {}).get('href'),
            'setup_file': request_data.get('setupFile'),
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

        return self.__get_log(job_id, 'genie/logs/genie.log', **kwargs)

    def get_status(self, job_id):
        """Get job status."""

        return self.get(job_id, path='status').get('status')

    def get_stderr(self, job_id, **kwargs):
        """Get a stderr log for a job."""

        return self.__get_log(job_id, 'stderr', **kwargs)

    def get_stdout(self, job_id, **kwargs):
        """Get a stdout log for a job."""

        return self.__get_log(job_id, 'stdout', **kwargs)

    def kill_job(self, job_id=None, kill_uri=None):
        """Kill a job."""

        url = kill_uri if kill_uri is not None else self.__url_for_job(job_id)

        try:
            return call(method='delete',url=url, auth_handler=self.auth_handler)
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                raise GenieJobNotFoundError("job id '{}' not found at {}." \
                    .format(job_id, url))
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

        attachments = payload.get('attachments', [])
        if 'attachments' in payload:
            del payload['attachments']

        files = [('request', ('', json.dumps(payload), 'application/json'))]

        for att in attachments:
            files.append(('attachment', att))
            logger.debug('adding attachment: %s', att)

        try:
            logger.debug('payload to genie 3:')
            logger.debug(json.dumps(payload,
                                    sort_keys=True,
                                    indent=4,
                                    separators=(',', ': ')))
            call(method='post',
                 url='{}/{}'.format(job.conf.genie.url,
                                    Genie3Adapter.JOBS_ENDPOINT),
                 files=files,
                 auth_handler=self.auth_handler)
        except GenieHTTPError as err:
            if err.response.status_code == 409:
                logger.debug("reattaching to job id '%s'", job.get('job_id'))
            else:
                raise


@dispatch(GenieJob, namespace=dispatch_ns)
def get_payload(job):
    """Construct payload for GenieJob -> Genie 3."""

    try:
        payload = Genie3Adapter.construct_base_payload(job)
    except GenieJobError:
        raise GenieJobError('trying to run GenieJob without explicitly setting ' \
                            'command line arguments (use .command_arguments())')

    if not payload.get('commandCriteria'):
        raise GenieJobError('trying to run GenieJob without specifying' \
                            'command tags')

    return payload


@dispatch((HadoopJob, HiveJob, PigJob, PrestoJob), namespace=dispatch_ns)
@set_jobname
@assert_script
def get_payload(job):
    """Construct payload for PigJob -> Genie 3."""

    return Genie3Adapter.construct_base_payload(job)
