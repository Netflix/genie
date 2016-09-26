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
from urlparse import urlparse

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
                          GenieJobNotFoundError,
                          GenieLogNotFoundError)

#jobs
from ..jobs.core import GenieJob
from ..jobs.hadoop import HadoopJob
from ..jobs.hive import HiveJob
from ..jobs.pig import PigJob
from ..jobs.presto import PrestoJob
from ..jobs.sqoop import SqoopJob


logger = logging.getLogger('com.netflix.genie.jobs.adapter.genie_3')

dispatch_ns = dict()


def set_jobname(func):
    """Decorator to update job name with script."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        """Wraps func."""

        payload = func(*args, **kwargs)

        job = args[0]
        script = job.get('script')

        # handle job name if not set
        if not job.get('job_name') and script:
            if is_file(script):
                payload['name'] = os.path.basename(script)
            else:
                payload['name'] = script \
                    .replace('${', '{') \
                    .replace(';', '') \
                    .replace('\n', ' ')[:40] \
                    .strip()

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
            raise GenieAttachmentError("in-line attachment is missing required keys ('name', 'data') ({})".format(att))
    raise GenieAttachmentError("cannot handle attachment '{}'".format(att))


class Genie3Adapter(GenieBaseAdapter):
    """Genie server 3"""

    JOBS_ENDPOINT = 'api/v3/jobs'

    def __init__(self, conf=None):
        super(Genie3Adapter, self).__init__(conf=conf)
        self.auth_handler = AuthHandler(conf=conf)

    def get_log(self, job_id, log, iterator=False, **kwargs):
        url = '{}/output/{}'.format(self.__url_for_job(job_id), log)

        try:
            response = call(method='get',
                            url=url,
                            stream=iterator,
                            auth_handler=self.auth_handler,
                            **kwargs)
            return response.iter_lines() if iterator else response.text
        except GenieHTTPError as err:
            if err.response.status_code in {404, 406}:
                raise GenieLogNotFoundError("log not found at {}".format(url))
            raise

    def __url_for_job(self, job_id):
        return '{}/{}/{}'.format(self._conf.genie.url,
                                 Genie3Adapter.JOBS_ENDPOINT,
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
                att = to_attachment(dep)
                if isinstance(att, list):
                    attachments.extend(att)
                else:
                    attachments.append(att)
            else:
                dependencies.append(dep)

        description = job.get('description')
        if isinstance(description, dict):
            description = json.dumps(description)

        cluster_tag_mapping = job.get('cluster_tag_mapping')
        clusters = [
            dict(tags=cluster_tag_mapping.get(priority))
            for priority in sorted(cluster_tag_mapping.keys())
        ]

        payload = {
            'applications': job.get('application_ids'),
            'attachments': attachments,
            'clusterCriterias': [i for i in clusters if i.get('tags')],
            'commandArgs': job.get('command_arguments') or command_args,
            'commandCriteria': job.get('command_tags') or job.default_command_tags,
            'dependencies': [d for d in dependencies if d not in {'', None}],
            'description': description,
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

        if job.get('genie_cpu'):
            payload['cpu'] = job.get('genie_cpu')
        if job.get('genie_memory'):
            payload['memory'] = job.get('genie_memory')

        return payload

    def get(self, job_id, path=None, if_not_found=None, **kwargs):
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
                        auth_handler=self.auth_handler,
                        retry_status_codes=500,
                        **kwargs) \
                   .json()
        except GenieHTTPError as err:
            if err.response.status_code == 404:
                msg = "job not found at {}".format(url)
                if if_not_found is not None:
                    return if_not_found
                raise GenieJobNotFoundError(msg)
            raise

    def get_info_for_rj(self, job_id, job=False, request=False,
                        applications=False, cluster=False, command=False,
                        execution=False, *args, **kwargs):
        """
        Get information for RunningJob object.
        """

        get_all = all([not job,
                       not request,
                       not applications,
                       not cluster,
                       not command,
                       not execution])

        ret = dict()

        if job or get_all:
            data = self.get(job_id, timeout=30)

            link = data.get('_links', {}).get('self', {}).get('href')
            link_parts = urlparse(link)
            output_link = '{scheme}://{netloc}/output/{job_id}/output' \
                .format(scheme=link_parts.scheme,
                        netloc=link_parts.netloc,
                        job_id=data.get('id'))
            job_link = '{scheme}://{netloc}/jobs?id={job_id}&rowId={job_id}' \
                .format(scheme=link_parts.scheme,
                        netloc=link_parts.netloc,
                        job_id=data.get('id'))

            ret['archive_location'] = data.get('archiveLocation')
            ret['attachments'] = None
            ret['command_args'] = data.get('commandArgs')
            ret['created'] = data.get('created')
            ret['description'] = data.get('description')
            ret['finished'] = data.get('finished')
            ret['id'] = data.get('id')
            ret['job_link'] = job_link
            ret['json_link'] = link
            ret['kill_uri'] = link
            ret['name'] = data.get('name')
            ret['output_uri'] = output_link
            ret['started'] = data.get('started')
            ret['status'] = data.get('status')
            ret['status_msg'] = data.get('statusMsg')
            ret['tags'] = data.get('tags')
            ret['updated'] = data.get('updated')
            ret['user'] = data.get('user')
            ret['version'] = data.get('version')

        if request or get_all:
            request_data = self.get(job_id, path='request', timeout=30)

            ret['disable_archive'] = request_data.get('disableLogArchival')
            ret['email'] = request_data.get('email')
            ret['file_dependencies'] = request_data.get('dependencies')
            ret['group'] = request_data.get('group')
            ret['request_data'] = request_data

        if applications or get_all:
            application_data = self.get(job_id,
                                        path='applications',
                                        if_not_found=list(),
                                        timeout=30)

            ret['application_name'] = ','.join(a.get('id') for a in application_data)

        if cluster or get_all:
            cluster_data = self.get(job_id,
                                    path='cluster',
                                    if_not_found=dict(),
                                    timeout=30)

            ret['cluster_id'] = cluster_data.get('id')
            ret['cluster_name'] = cluster_data.get('name')

        if command or get_all:
            command_data = self.get(job_id,
                                    path='command',
                                    if_not_found=dict(),
                                    timeout=30)

            ret['command_id'] = command_data.get('id')
            ret['command_name'] = command_data.get('name')

        if execution or get_all:
            execution_data = self.get(job_id,
                                      path='execution',
                                      if_not_found=dict(),
                                      timeout=30)

            ret['client_host'] = execution_data.get('hostName')

        return ret

    def get_genie_log(self, job_id, **kwargs):
        """Get a genie log for a job."""

        return self.get_log(job_id, 'genie/logs/genie.log', **kwargs)

    def get_status(self, job_id):
        """Get job status."""

        return self.get(job_id, path='status', timeout=10).get('status')

    def get_stderr(self, job_id, **kwargs):
        """Get a stderr log for a job."""

        return self.get_log(job_id, 'stderr', **kwargs)

    def get_stdout(self, job_id, **kwargs):
        """Get a stdout log for a job."""

        return self.get_log(job_id, 'stdout', **kwargs)

    def kill_job(self, job_id=None, kill_uri=None):
        """Kill a job."""

        url = kill_uri if kill_uri is not None else self.__url_for_job(job_id)

        try:
            return call(method='delete',
                        url=url,
                        timeout=30,
                        auth_handler=self.auth_handler)
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

        attachments = payload.get('attachments', [])
        if 'attachments' in payload:
            del payload['attachments']

        files = [('request', ('', json.dumps(payload), 'application/json'))]

        for att in attachments:
            files.append(('attachment', att))
            logger.debug('adding attachment: %s', att)

        logger.debug('payload to genie 3:')
        logger.debug(json.dumps(payload,
                                sort_keys=True,
                                indent=4,
                                separators=(',', ': ')))

        call(method='post',
             url='{}/{}'.format(job._conf.genie.url, Genie3Adapter.JOBS_ENDPOINT),
             files=files,
             timeout=30,
             auth_handler=self.auth_handler)


@dispatch(GenieJob, namespace=dispatch_ns)
def get_payload(job):
    """Construct payload for GenieJob -> Genie 3."""

    try:
        payload = Genie3Adapter.construct_base_payload(job)
    except GenieJobError:
        raise GenieJobError('trying to run GenieJob without explicitly setting ' \
                            'command line arguments (use .command_arguments())')

    if not payload.get('commandCriteria'):
        raise GenieJobError('trying to run GenieJob without specifying command tags')

    return payload


@dispatch((HadoopJob, HiveJob, PigJob, PrestoJob), namespace=dispatch_ns)
@set_jobname
def get_payload(job):
    """Construct payload for jobs -> Genie 3."""

    return Genie3Adapter.construct_base_payload(job)


@dispatch((SqoopJob), namespace=dispatch_ns)
def get_payload(job):
    """Construct payload for SqoopJob -> Genie 3."""

    return Genie3Adapter.construct_base_payload(job)
