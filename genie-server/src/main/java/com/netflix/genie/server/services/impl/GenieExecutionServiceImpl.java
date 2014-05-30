/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.server.services.impl;

import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.BaseRequest;
import com.netflix.genie.common.messages.BaseResponse;
import com.netflix.genie.common.messages.JobRequest;
import com.netflix.genie.common.messages.JobResponse;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.common.model.Types.JobType;
import com.netflix.genie.common.model.Types.SubprocessStatus;
import com.netflix.genie.server.jobmanager.JobManagerFactory;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.services.ExecutionService;
import com.netflix.genie.server.util.NetUtil;
import com.netflix.niws.client.http.RestClient;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.RollbackException;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Genie Execution Service API that uses a local job
 * launcher (via the job manager implementation), and uses OpenJPA for
 * peristence.
 *
 * @author skrishnan
 * @author bmundlapudi
 * @author amsharma
 * @author tgianos
 */
public class GenieExecutionServiceImpl implements ExecutionService {

    private static final Logger LOG = LoggerFactory
            .getLogger(GenieExecutionServiceImpl.class);

    // instance of the netflix configuration object
    private static final AbstractConfiguration CONF;

    // these can be over-ridden in the properties file
    private static final int SERVER_PORT;
    private static final String JOB_DIR_PREFIX;
    private static final String JOB_RESOURCE_PREFIX;

    // per-instance variables
    private final PersistenceManager<Job> pm;
    private final GenieNodeStatistics stats;

    // initialize static variables
    static {
        CONF = ConfigurationManager.getConfigInstance();
        SERVER_PORT = CONF.getInt("netflix.appinfo.port", 7001);
        JOB_DIR_PREFIX = CONF.getString("netflix.genie.server.job.dir.prefix",
                "genie-jobs");
        JOB_RESOURCE_PREFIX = CONF.getString(
                "netflix.genie.server.job.resource.prefix", "genie/v1/jobs");
    }

    /**
     * Default constructor - initializes persistence manager, and other utility
     * classes.
     */
    public GenieExecutionServiceImpl() {
        pm = new PersistenceManager<Job>();
        stats = GenieNodeStatistics.getInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobResponse submitJob(JobRequest request) {
        LOG.info("called");

        JobResponse response;
        Job jInfo = request.getJobInfo();

        // validate parameters
        try {
            validateJobParams(jInfo);
        } catch (CloudServiceException e) {
            response = new JobResponse(e);
            return response;
        }

        // ensure that job won't overload system
        // synchronize until an entry is created and INIT-ed in DB
        // throttling related parameters
        int maxRunningJobs = CONF.getInt(
                "netflix.genie.server.max.running.jobs", 0);
        int jobForwardThreshold = CONF.getInt(
                "netflix.genie.server.forward.jobs.threshold", 0);
        int maxIdleHostThreshold = CONF.getInt(
                "netflix.genie.server.max.idle.host.threshold", 0);
        int idleHostThresholdDelta = CONF.getInt(
                "netflix.genie.server.idle.host.threshold.delta", 0);
        synchronized (this) {
            try {
                int numRunningJobs = JobCountManager.getNumInstanceJobs();
                LOG.info("Number of running jobs: " + numRunningJobs);

                // find an instance with fewer than (numRunningJobs -
                // idleHostThresholdDelta)
                int idleHostThreshold = numRunningJobs - idleHostThresholdDelta;
                // if numRunningJobs is already >= maxRunningJobs, forward
                // aggressively
                // but cap it at the max
                if ((idleHostThreshold > maxIdleHostThreshold)
                        || (numRunningJobs >= maxRunningJobs)) {
                    idleHostThreshold = maxIdleHostThreshold;
                }

                // check to see if job should be forwarded - only forward it
                // once. the assumption is that jobForwardThreshold < maxRunningJobs
                // (set in properties file)
                if ((numRunningJobs >= jobForwardThreshold)
                        && (!jInfo.isForwarded())) {
                    LOG.info("Number of running jobs greater than forwarding threshold - trying to auto-forward");
                    String idleHost = JobCountManager
                            .getIdleInstance(idleHostThreshold);
                    if (!idleHost.equals(NetUtil.getHostName())) {
                        jInfo.setForwarded(true);
                        stats.incrGenieForwardedJobs();
                        response = forwardJobRequest("http://" + idleHost + ":"
                                + SERVER_PORT + "/" + JOB_RESOURCE_PREFIX, request);
                        return response;
                    } // else, no idle hosts found - run here if capacity exists
                }

                if (numRunningJobs >= maxRunningJobs) {
                    // if we get here, job can't be forwarded to an idle
                    // instance anymore and current node is overloaded
                    response = new JobResponse(
                            new CloudServiceException(
                                    HttpURLConnection.HTTP_UNAVAILABLE,
                                    "Number of running jobs greater than system limit ("
                                    + maxRunningJobs
                                    + ") - try another instance or try again later"));
                    return response;
                }

                // if job can be launched, update the URIs
                buildJobURIs(jInfo);
            } catch (CloudServiceException e) {
                response = new JobResponse(e);
                LOG.error(response.getErrorMsg(), e);
                return response;
            }

            // init state in DB - return if job already exists
            try {
                // TODO add retries to avoid deadlock issue
                pm.createEntity(jInfo);
            } catch (RollbackException e) {
                LOG.error("Can't create entity in the database", e);
                if (e.getCause() instanceof EntityExistsException) {
                    LOG.error(e.getCause().getMessage());
                    // most likely entity already exists - return useful message
                    response = new JobResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "Job already exists for id: " + jInfo.getId()));
                    return response;
                } else {
                    // unknown exception - send it back
                    response = new JobResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            "Received exception: " + e.getCause()));
                    return response;
                }
            }
        } // end synchronize

        // increment number of submitted jobs
        stats.incrGenieJobSubmissions();

        // try to run the job - return success or error
        try {
            JobManagerFactory.getJobManager(jInfo.getJobType()).launch(jInfo);

            // update entity in DB
            jInfo.setUpdateTime(System.currentTimeMillis());
            pm.updateEntity(jInfo);

            // verification
            jInfo = pm.getEntity(jInfo.getId(), Job.class);

            // return successful response
            response = new JobResponse();
            response.setMessage("Successfully launched job: "
                    + jInfo.getId());
            response.setJob(jInfo);
            return response;
        } catch (final CloudServiceException e) {
            LOG.error("Failed to submit job: ", e);
            // update db
            jInfo.setJobStatus(JobStatus.FAILED, e.getMessage());
            jInfo.setUpdateTime(System.currentTimeMillis());
            pm.updateEntity(jInfo);
            // increment counter for failed jobs
            stats.incrGenieFailedJobs();
            // if it is a known exception, handle differently
            if (e instanceof CloudServiceException) {
                response = new JobResponse((CloudServiceException) e);
            } else {
                response = new JobResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            }
            return response;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobResponse getJobInfo(String jobId) {
        LOG.info("called for jobId: " + jobId);

        JobResponse response;
        Job jInfo;
        try {
            jInfo = pm.getEntity(jobId, Job.class);
        } catch (Exception e) {
            LOG.error("Failed to get job info: ", e);
            response = new JobResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        if (jInfo == null) {
            String msg = "Job not found: " + jobId;
            LOG.error(msg);
            response = new JobResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return response;
        } else {
            response = new JobResponse();
            response.setJob(jInfo);
            response.setMessage("Returning job information for: "
                    + jInfo.getId());
            return response;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobResponse getJobs(final String jobId, final String jobName, final String userName, final String jobType,
            final String status, final String clusterName, final String clusterId, final Integer limit, final Integer page) {
        LOG.info("called");

        JobResponse response;

        final StringBuilder builder = new StringBuilder();
        builder.append("Select j");
        builder.append(" FROM Job j");

        final List<String> clauses = new ArrayList<String>();
        if (!StringUtils.isEmpty(jobId)) {
            clauses.add("j.jobID LIKE :jobId");
        }
        if (!StringUtils.isEmpty(jobName)) {
            clauses.add("j.jobName LIKE :jobName");
        }
        if (!StringUtils.isEmpty(userName)) {
            clauses.add("j.userName = :userName");
        }
        if (!StringUtils.isEmpty(jobType)) {
            clauses.add("j.jobType = :jobType");
        }
        if (!StringUtils.isEmpty(status)) {
            clauses.add("j.status = :status");
        }
        if (!StringUtils.isEmpty(clusterName)) {
            clauses.add("j.clusterName = :clusterName");
        }
        if (!StringUtils.isEmpty(clusterId)) {
            clauses.add("j.clusterId = :clusterId");
        }

        if (!clauses.isEmpty()) {
            builder.append(" WHERE");
        }
        boolean addAnd = false;
        for (final String clause : clauses) {
            if (addAnd) {
                builder.append(" AND");
            }
            builder.append(" ").append(clause);
            addAnd = true;
        }

        final EntityManager em = pm.createEntityManager();
        try {
            final Query query = em.createQuery(builder.toString(), Job.class);
            if (!StringUtils.isEmpty(jobId)) {
                query.setParameter("jobId", jobId);
            }
            if (!StringUtils.isEmpty(jobName)) {
                query.setParameter("jobName", jobName);
            }
            if (!StringUtils.isEmpty(userName)) {
                query.setParameter("userName", userName);
            }
            if (!StringUtils.isEmpty(jobType)) {
                try {
                    query.setParameter("jobType", JobType.parse(jobType));
                } catch (final CloudServiceException e) {
                    LOG.error(e.getMessage(), e);
                    response = new JobResponse(e);
                    return response;
                }
            }
            if (!StringUtils.isEmpty(status)) {
                try {
                    query.setParameter("status", JobStatus.parse(status));
                } catch (final CloudServiceException e) {
                    LOG.error(e.getMessage(), e);
                    response = new JobResponse(e);
                    return response;
                }
            }
            if (!StringUtils.isEmpty(clusterName)) {
                query.setParameter("clusterName", clusterName);
            }
            if (!StringUtils.isEmpty(clusterId)) {
                query.setParameter("clusterId", clusterId);
            }

            //Limit the number of results and what page they want if requested
            if (limit != null) {
                query.setMaxResults(limit);
                if (page != null) {
                    query.setFirstResult(limit * page);
                }
            }

            final List<Job> jobs = query.getResultList();
            if (jobs.isEmpty()) {
                //TODO: Why not just throw exception and let Jersey wrappers handle returning errors?
                return new JobResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_NOT_FOUND,
                        "No jobs found for specified criteria"));
            } else {
                response = new JobResponse();
                //TODO: Switch to just using Collections and not native arrays
                response.setJobs(jobs.toArray(new Job[jobs.size()]));
                return response;
            }
        } finally {
            em.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatusResponse getJobStatus(String jobId) {
        LOG.info("called for jobId: " + jobId);

        JobStatusResponse response;

        Job jInfo;
        try {
            jInfo = pm.getEntity(jobId, Job.class);
        } catch (Exception e) {
            LOG.error("Failed to get job results from database: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        if (jInfo == null) {
            String msg = "Job not found: " + jobId;
            LOG.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return response;
        } else {
            response = new JobStatusResponse();
            response.setMessage("Returning status for job: " + jobId);
            response.setStatus(jInfo.getStatus().name());
            return response;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatusResponse killJob(String jobId) {
        LOG.info("called for jobId: " + jobId);

        JobStatusResponse response;

        Job jInfo;
        try {
            jInfo = pm.getEntity(jobId, Job.class);
        } catch (Exception e) {
            LOG.error("Failed to get job results from database: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        // do some basic error handling
        if (jInfo == null) {
            String msg = "Job not found: " + jobId;
            LOG.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return response;
        }

        // check if it is done already
        if (jInfo.getStatus() == JobStatus.SUCCEEDED
                || jInfo.getStatus() == JobStatus.KILLED
                || jInfo.getStatus() == JobStatus.FAILED) {
            // job already exited, return status to user
            response = new JobStatusResponse();
            response.setStatus(jInfo.getStatus().name());
            response.setMessage("Job " + jobId + " is already done");
            return response;
        } else if (jInfo.getStatus() == JobStatus.INIT
                || (jInfo.getProcessHandle() == -1)) {
            // can't kill a job if it is still initializing
            String msg = "Unable to kill job as it is still initializing: " + jobId;
            LOG.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg));
            return response;
        }

        // if we get here, job is still running - and can be killed
        // redirect to the right node if killURI points to a different node
        String killURI = jInfo.getKillURI();
        if (killURI == null) {
            String msg = "Failed to get killURI for jobID: " + jobId;
            LOG.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg));
            return response;
        }
        String localURI;
        try {
            localURI = getEndPoint() + "/" + JOB_RESOURCE_PREFIX + "/" + jobId;
        } catch (CloudServiceException e) {
            LOG.error("Error while retrieving local hostname: "
                    + e.getMessage(), e);
            response = new JobStatusResponse(e);
            return response;
        }
        if (!killURI.equals(localURI)) {
            LOG.debug("forwarding kill request to: " + killURI);
            response = forwardJobKill(killURI);
            return response;
        }

        // if we get here, killURI == localURI, and job should be killed here
        LOG.debug("killing job on same instance: " + jobId);
        try {
            JobManagerFactory.getJobManager(jInfo.getJobType()).kill(jInfo);
        } catch (final CloudServiceException e) {
            LOG.error("Failed to kill job: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Failed to kill job: " + e.getCause()));
            return response;
        }

        jInfo.setJobStatus(JobStatus.KILLED, "Job killed on user request");
        jInfo.setExitCode(SubprocessStatus.JOB_KILLED.code());

        // increment counter for killed jobs
        stats.incrGenieKilledJobs();

        // update final status in DB
        ReentrantReadWriteLock rwl = PersistenceManager.getDbLock();
        try {
            LOG.debug("updating job status to KILLED for: " + jobId);
            // acquire write lock first, and then update status
            // if job status changed between when it was read and now,
            // this thread will simply overwrite it - final state will be KILLED
            rwl.writeLock().lock();
            jInfo.setUpdateTime(System.currentTimeMillis());
            if (!jInfo.isDisableLogArchival()) {
                jInfo.setArchiveLocation(NetUtil.getArchiveURI(jobId));
            }
            pm.updateEntity(jInfo);
        } catch (Exception e) {
            LOG.error("Failed to update job status in database: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));

            // unlock before returning response
            return response;
        } finally {
            if (rwl.writeLock().isHeldByCurrentThread()) {
                rwl.writeLock().unlock();
            }
        }

        // all good - return results
        response = new JobStatusResponse();
        response.setStatus(jInfo.getStatus().name());
        response.setMessage("Successfully killed job: " + jobId);
        return response;
    }

    /*
     * Check if this job has token as id sent from client.
     */
    private void validateJobParams(Job jobInfo)
            throws CloudServiceException {
        LOG.debug("called");

        if (jobInfo == null) {
            String msg = "Missing jobInfo object";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        // Either the commandId or the commandName have to be specified.
        if (StringUtils.isEmpty(jobInfo.getCommandId()) && StringUtils.isEmpty(jobInfo.getCommandName())) {
            String msg = "Either the commandId or the commandName have to be specified";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        // check if userName is valid
        validateNameValuePair("userName", jobInfo.getUserName());

        // check if cmdArgs is valid
        validateNameValuePair("cmdArgs", jobInfo.getCmdArgs());

        // check if jobType is valid
        validateNameValuePair("jobType", jobInfo.getJobType());

        // generate job id, if need be
        if (jobInfo.getId() == null || jobInfo.getId().isEmpty()) {
            UUID uuid = UUID.randomUUID();
            jobInfo.setId(uuid.toString());
        }

        jobInfo.setJobStatus(JobStatus.INIT, "Initializing job");
    }

    private void validateNameValuePair(String name, String value)
            throws CloudServiceException {
        LOG.debug("called");
        String msg;

        // ensure that the value is not null/empty
        if (value == null || value.isEmpty()) {
            msg = "Invalid " + name + " parameter, can't be null or empty";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        // now validate various parameters
        if (name.equals("jobType") && (Types.JobType.parse(value) == null)) {
            msg = "Invalid "
                    + name
                    + ", Invalid Jop type received. Job type should be yarn. Wrong value received: "
                    + value;
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
    }

    private void buildJobURIs(Job ji) throws CloudServiceException {
        ji.setHostName(NetUtil.getHostName());
        ji.setOutputURI(getEndPoint() + "/" + JOB_DIR_PREFIX + "/"
                + ji.getId());
        ji.setKillURI(getEndPoint() + "/" + JOB_RESOURCE_PREFIX + "/"
                + ji.getId());
    }

    private String getEndPoint() throws CloudServiceException {
        return "http://" + NetUtil.getHostName() + ":" + SERVER_PORT;
    }

    private JobStatusResponse forwardJobKill(String killURI) {
        JobStatusResponse response;
        try {
            response = executeRequest(Verb.DELETE, killURI, null,
                    JobStatusResponse.class);
            return response;
        } catch (CloudServiceException e) {
            return new JobStatusResponse(e);
        }
    }

    private JobResponse forwardJobRequest(String hostURI,
            JobRequest request) {
        JobResponse response;
        try {
            response = executeRequest(Verb.POST, hostURI, request,
                    JobResponse.class);
            return response;
        } catch (CloudServiceException e) {
            return new JobResponse(e);
        }
    }

    private <T extends BaseResponse> T executeRequest(Verb method,
            String restURI, BaseRequest request, Class<T> responseClass)
            throws CloudServiceException {
        HttpResponse clientResponse = null;
        T response;
        try {
            RestClient genieClient = (RestClient) ClientFactory
                    .getNamedClient("genie");
            HttpRequest req = HttpRequest.newBuilder()
                    .verb(method).header("Accept", "application/json")
                    .uri(new URI(restURI)).entity(request).build();
            clientResponse = genieClient.execute(req);
            if (clientResponse != null) {
                int status = clientResponse.getStatus();
                LOG.info("Response Status:" + status);
                response = clientResponse.getEntity(responseClass);
                return response;
            } else {
                String msg = "Received null response while auto-forwarding request to Genie instance";
                LOG.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
        } catch (CloudServiceException e) {
            // just raise it rightaway
            throw e;
        } catch (Exception e) {
            String msg = "Error while trying to auto-forward request: "
                    + e.getMessage();
            LOG.error(msg, e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        } finally {
            if (clientResponse != null) {
                // this is really really important
                clientResponse.close();
            }
        }
    }
}
