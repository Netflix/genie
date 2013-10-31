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

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.persistence.EntityExistsException;
import javax.persistence.RollbackException;

import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.client.ClientFactory;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.BaseRequest;
import com.netflix.genie.common.messages.BaseResponse;
import com.netflix.genie.common.messages.JobInfoRequest;
import com.netflix.genie.common.messages.JobInfoResponse;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.Types;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.common.model.Types.SubprocessStatus;
import com.netflix.genie.server.jobmanager.JobManagerFactory;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.persistence.ClauseBuilder;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ExecutionService;
import com.netflix.genie.server.util.NetUtil;
import com.netflix.niws.client.http.HttpClientRequest;
import com.netflix.niws.client.http.HttpClientRequest.Verb;
import com.netflix.niws.client.http.HttpClientResponse;
import com.netflix.niws.client.http.RestClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Implementation of the Genie Execution Service API that uses a local job
 * launcher (via the job manager implementation), and uses OpenJPA for
 * peristence.
 *
 * @author skrishnan
 * @author bmundlapudi
 */
public class GenieExecutionServiceImpl implements ExecutionService {

    private static Logger logger = LoggerFactory
            .getLogger(GenieExecutionServiceImpl.class);

    // instance of the netflix configuration object
    private static AbstractConfiguration conf;

    // these can be over-ridden in the properties file
    private static int serverPort = 7001;
    private static String jobDirPrefix = "genie-jobs";
    private static String jobResourcePrefix = "genie/v0/jobs";

    // per-instance variables
    private PersistenceManager<JobInfoElement> pm;
    private GenieNodeStatistics stats;

    // initialize static variables
    static {
        conf = ConfigurationManager.getConfigInstance();
        serverPort = conf.getInt("netflix.appinfo.port", serverPort);
        jobDirPrefix = conf.getString("netflix.genie.server.job.dir.prefix",
                jobDirPrefix);
        jobResourcePrefix = conf.getString(
                "netflix.genie.server.job.resource.prefix", jobResourcePrefix);
    }

    /**
     * Default constructor - initializes persistence manager, and other utility
     * classes.
     */
    public GenieExecutionServiceImpl() {
        pm = new PersistenceManager<JobInfoElement>();
        stats = GenieNodeStatistics.getInstance();
    }

    /** {@inheritDoc} */
    @Override
    public JobInfoResponse submitJob(JobInfoRequest jir) {
        logger.info("called");

        JobInfoResponse response;
        JobInfoElement jInfo = jir.getJobInfo();

        // validate parameters
        try {
            validateJobParams(jInfo);
        } catch (CloudServiceException e) {
            response = new JobInfoResponse(e);
            return response;
        }

        // ensure that job won't overload system
        // synchronize until an entry is created and INIT-ed in DB
        // throttling related parameters
        int maxRunningJobs = conf.getInt(
                "netflix.genie.server.max.running.jobs", 0);
        int jobForwardThreshold = conf.getInt(
                "netflix.genie.server.forward.jobs.threshold", 0);
        int maxIdleHostThreshold = conf.getInt(
                "netflix.genie.server.max.idle.host.threshold", 0);
        int idleHostThresholdDelta = conf.getInt(
                "netflix.genie.server.idle.host.threshold.delta", 0);
        synchronized (this) {
            try {
                int numRunningJobs = JobCountManager.getNumInstanceJobs();
                logger.info("Number of running jobs: " + numRunningJobs);

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
                    logger.info("Number of running jobs greater than forwarding threshold - trying to auto-forward");
                    String idleHost = JobCountManager
                            .getIdleInstance(idleHostThreshold);
                    if (!idleHost.equals(NetUtil.getHostName())) {
                        jInfo.setForwarded(true);
                        stats.incrGenieForwardedJobs();
                        response = forwardJobRequest("http://" + idleHost + ":"
                                + serverPort + "/" + jobResourcePrefix, jir);
                        return response;
                    } // else, no idle hosts found - run here if capacity exists
                }

                if (numRunningJobs >= maxRunningJobs) {
                    // if we get here, job can't be forwarded to an idle
                    // instance anymore and current node is overloaded
                    response = new JobInfoResponse(
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
                response = new JobInfoResponse(e);
                logger.error(response.getErrorMsg(), e);
                return response;
            }

            // init state in DB - return if job already exists
            try {
                pm.createEntity(jInfo);
            } catch (RollbackException e) {
                logger.error("Can't create entity in the database", e);
                if (e.getCause() instanceof EntityExistsException) {
                    logger.error(e.getCause().getMessage());
                    // most likely entity already exists - return useful message
                    response = new JobInfoResponse(new CloudServiceException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "Job already exists for id: " + jInfo.getJobID()));
                    return response;
                } else {
                    // unknown exception - send it back
                    response = new JobInfoResponse(new CloudServiceException(
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
            jInfo = pm.getEntity(jInfo.getJobID(), JobInfoElement.class);

            // return successful response
            response = new JobInfoResponse();
            response.setMessage("Successfully launched job: "
                    + jInfo.getJobID());
            response.setJob(jInfo);
            return response;
        } catch (Exception e) {
            logger.error("Failed to submit job: ", e);
            // update db
            jInfo.setJobStatus(JobStatus.FAILED, e.getMessage());
            jInfo.setUpdateTime(System.currentTimeMillis());
            pm.updateEntity(jInfo);
            // increment counter for failed jobs
            stats.incrGenieFailedJobs();
            // if it is a known exception, handle differently
            if (e instanceof CloudServiceException) {
                response = new JobInfoResponse((CloudServiceException) e);
            } else {
                response = new JobInfoResponse(new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            }
            return response;
        }
    }

    /** {@inheritDoc} */
    @Override
    public JobInfoResponse getJobInfo(String jobId) {
        logger.info("called for jobId: " + jobId);

        JobInfoResponse response;
        JobInfoElement jInfo;
        try {
            jInfo = pm.getEntity(jobId, JobInfoElement.class);
        } catch (Exception e) {
            logger.error("Failed to get job info: ", e);
            response = new JobInfoResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        if (jInfo == null) {
            String msg = "Job not found: " + jobId;
            logger.error(msg);
            response = new JobInfoResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return response;
        } else {
            response = new JobInfoResponse();
            response.setJob(jInfo);
            response.setMessage("Returning job information for: "
                    + jInfo.getJobID());
            return response;
        }
    }

    /** {@inheritDoc} */
    @Override
    public JobInfoResponse getJobs(String jobID, String jobName, String userName, String jobType,
            String status, String clusterName, String clusterId, Integer limit, Integer page) {
        logger.info("called");

        JobInfoResponse response;
        String table = JobInfoElement.class.getSimpleName();

        ClauseBuilder criteria = null;
        try {
            criteria = new ClauseBuilder(ClauseBuilder.AND);
            if ((jobID != null) && (!jobID.isEmpty())) {
                String query = "jobID like '" + jobID + "'";
                criteria.append(query);
            }
            if ((jobName != null) && (!jobName.isEmpty())) {
                String query = "jobName like '" + jobName + "'";
                criteria.append(query);
            }
            if ((userName != null) && (!userName.isEmpty())) {
                String query = "userName='" + userName + "'";
                criteria.append(query);
            }
            if ((jobType != null) && (!jobType.isEmpty())) {
                if (Types.JobType.parse(jobType) == null) {
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST, "Job type: "
                                    + jobType
                                    + " can only be HADOOP, HIVE or PIG");
                }
                String query = "jobType='" + jobType.toUpperCase() + "'";
                criteria.append(query);
            }
            if ((status != null) && (!status.isEmpty())) {
                if (Types.JobStatus.parse(status) == null) {
                    throw new CloudServiceException(
                            HttpURLConnection.HTTP_BAD_REQUEST,
                            "Unknown job status: " + status);
                }
                String query = "status='" + status.toUpperCase() + "'";
                criteria.append(query);
            }
            if ((clusterName != null) && (!clusterName.isEmpty())) {
                String query = "clusterName='" + clusterName + "'";
                criteria.append(query);
            }
            if ((clusterId != null) && (!clusterId.isEmpty())) {
                String query = "clusterId='" + clusterId + "'";
                criteria.append(query);
            }
        } catch (CloudServiceException e) {
            logger.error(e.getMessage(), e);
            response = new JobInfoResponse(e);
            return response;
        }

        Object[] results;
        try {
            QueryBuilder builder = new QueryBuilder().table(table)
                    .clause(criteria.toString()).limit(limit).page(page);
            results = pm.query(builder);

        } catch (Exception e) {
            logger.error("Failed to get job results from database: ", e);
            response = new JobInfoResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        if (results.length != 0) {
            JobInfoElement[] jobInfos = new JobInfoElement[results.length];
            for (int i = 0; i < results.length; i++) {
                jobInfos[i] = (JobInfoElement) results[i];
            }

            response = new JobInfoResponse();
            response.setJobs(jobInfos);
            response.setMessage("Returning job information for specified criteria");
            return response;
        } else {
            response = new JobInfoResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No jobs found for specified criteria"));
            return response;
        }
    }

    /** {@inheritDoc} */
    @Override
    public JobStatusResponse getJobStatus(String jobId) {
        logger.info("called for jobId: " + jobId);

        JobStatusResponse response;

        JobInfoElement jInfo;
        try {
            jInfo = pm.getEntity(jobId, JobInfoElement.class);
        } catch (Exception e) {
            logger.error("Failed to get job results from database: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        if (jInfo == null) {
            String msg = "Job not found: " + jobId;
            logger.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return response;
        } else {
            response = new JobStatusResponse();
            response.setMessage("Returning status for job: " + jobId);
            response.setStatus(jInfo.getStatus());
            return response;
        }
    }

    /** {@inheritDoc} */
    @Override
    public JobStatusResponse killJob(String jobId) {
        logger.info("called for jobId: " + jobId);

        JobStatusResponse response;

        JobInfoElement jInfo;
        try {
            jInfo = pm.getEntity(jobId, JobInfoElement.class);
        } catch (Exception e) {
            logger.error("Failed to get job results from database: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));
            return response;
        }

        // do some basic error handling
        if (jInfo == null) {
            String msg = "Job not found: " + jobId;
            logger.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_NOT_FOUND, msg));
            return response;
        }

        // check if it is done already
        if (jInfo.getStatus().equalsIgnoreCase("SUCCEEDED")
                || jInfo.getStatus().equalsIgnoreCase("KILLED")
                || jInfo.getStatus().equalsIgnoreCase("FAILED")) {
            // job already exited, return status to user
            response = new JobStatusResponse();
            response.setStatus(jInfo.getStatus());
            response.setMessage("Job " + jobId + " is already done");
            return response;
        } else if (jInfo.getStatus().equalsIgnoreCase("INIT")
                || (jInfo.getProcessHandle() == -1)) {
            // can't kill a job if it is still initializing
            String msg = "Unable to kill job as it is still initializing: " + jobId;
            logger.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg));
            return response;
        }

        // if we get here, job is still running - and can be killed

        // redirect to the right node if killURI points to a different node
        String killURI = jInfo.getKillURI();
        if (killURI == null) {
            String msg = "Failed to get killURI for jobID: " + jobId;
            logger.error(msg);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg));
            return response;
        }
        String localURI;
        try {
            localURI = getEndPoint() + "/" + jobResourcePrefix + "/" + jobId;
        } catch (CloudServiceException e) {
            logger.error("Error while retrieving local hostname: "
                    + e.getMessage(), e);
            response = new JobStatusResponse(e);
            return response;
        }
        if (!killURI.equals(localURI)) {
            logger.debug("forwarding kill request to: " + killURI);
            response = forwardJobKill(killURI);
            return response;
        }

        // if we get here, killURI == localURI, and job should be killed here
        logger.debug("killing job on same instance: " + jobId);
        try {
            JobManagerFactory.getJobManager(jInfo.getJobType()).kill(jInfo);
        } catch (Exception e) {
            logger.error("Failed to kill job: ", e);
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
            logger.debug("updating job status to KILLED for: " + jobId);
            // acquire write lock first, and then update status
            // if job status changed between when it was read and now,
            // this thread will simply overwrite it - final state will be KILLED
            rwl.writeLock().lock();
            jInfo.setUpdateTime(System.currentTimeMillis());
            jInfo.setArchiveLocation(NetUtil.getArchiveURI(jobId));
            pm.updateEntity(jInfo);
            rwl.writeLock().unlock();
        } catch (Exception e) {
            logger.error("Failed to update job status in database: ", e);
            response = new JobStatusResponse(new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()));

            // unlock before returning response
            if (rwl.writeLock().isHeldByCurrentThread()) {
                rwl.writeLock().unlock();
            }
            return response;
        }

        // all good - return results
        response = new JobStatusResponse();
        response.setStatus(jInfo.getStatus());
        response.setMessage("Successfully killed job: " + jobId);
        return response;
    }

    /*
     * Check if this job has token as id sent from client.
     */
    private void validateJobParams(JobInfoElement jobInfo)
            throws CloudServiceException {
        logger.debug("called");

        if (jobInfo == null) {
            String msg = "Missing jobInfo object";
            logger.error(msg);
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

        // check if Hive Params include "-e" or "-f"
        if (Types.JobType.parse(jobInfo.getJobType()) == Types.JobType.HIVE) {
            validateNameValuePair("hiveArgs", jobInfo.getCmdArgs());
        }

        // check if schedule is valid
        validateNameValuePair("schedule", jobInfo.getSchedule());

        // check if configuration is valid for Hive/Pig
        if (Types.JobType.parse(jobInfo.getJobType()) != Types.JobType.HADOOP) {
            validateNameValuePair("configuration", jobInfo.getConfiguration());
        }

        // generate job id, if need be
        if (jobInfo.getJobID() == null || jobInfo.getJobID().isEmpty()) {
            UUID uuid = UUID.randomUUID();
            jobInfo.setJobID(uuid.toString());
        }

        jobInfo.setJobStatus(JobStatus.INIT, "Initializing job");
    }

    private void validateNameValuePair(String name, String value)
            throws CloudServiceException {
        logger.debug("called");
        String msg;

        // ensure that the value is not null/empty
        if (value == null || value.isEmpty()) {
            msg = "Invalid " + name + " parameter, can't be null or empty";
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        // now validate various parameters
        if (name.equals("jobType") && (Types.JobType.parse(value) == null)) {
            msg = "Invalid "
                    + name
                    + ", Valid types are hadoop or hive or pig. Wrong value received: "
                    + value;
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        } else if (name.equals("hiveArgs")) {
            if ((!value.contains("-f")) && (!value.contains("-e"))) {
                msg = "Hive arguments must include either the -e or -f flag";
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_BAD_REQUEST, msg);
            }
        } else if (name.equals("schedule")
                && (Types.Schedule.parse(value) == null)) {
            msg = "Invalid "
                    + name
                    + " type, Valid values are adhoc, sla or bonus. Wrong value received: "
                    + value;
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        } else if (name.equals("configuration")
                && (Types.Configuration.parse(value) == null)) {
            msg = "Invalid "
                    + name
                    + " type, Valid values are prod or test or unittest. Wrong value received: "
                    + value;
            logger.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
    }

    private void buildJobURIs(JobInfoElement ji) throws CloudServiceException {
        ji.setHostName(NetUtil.getHostName());
        ji.setOutputURI(getEndPoint() + "/" + jobDirPrefix + "/"
                + ji.getJobID());
        ji.setKillURI(getEndPoint() + "/" + jobResourcePrefix + "/"
                + ji.getJobID());
    }

    private String getEndPoint() throws CloudServiceException {
        return "http://" + NetUtil.getHostName() + ":" + serverPort;
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

    private JobInfoResponse forwardJobRequest(String hostURI,
            JobInfoRequest request) {
        JobInfoResponse response;
        try {
            response = executeRequest(Verb.POST, hostURI, request,
                    JobInfoResponse.class);
            return response;
        } catch (CloudServiceException e) {
            return new JobInfoResponse(e);
        }
    }

    private <T extends BaseResponse> T executeRequest(Verb method,
            String restURI, BaseRequest request, Class<T> responseClass)
            throws CloudServiceException {
        HttpClientResponse clientResponse = null;
        T response;
        try {
            MultivaluedMapImpl headers = new MultivaluedMapImpl();
            headers.add("Accept", "application/json");

            RestClient genieClient = (RestClient) ClientFactory
                    .getNamedClient("genie");
            HttpClientRequest req = HttpClientRequest.newBuilder()
                    .setVerb(method).setHeaders(headers)
                    .setUri(new URI(restURI)).setEntity(request).build();
            clientResponse = genieClient.execute(req);
            if (clientResponse != null) {
                int status = clientResponse.getStatus();
                logger.info("Response Status:" + status);
                response = clientResponse.getEntity(responseClass);
                return response;
            } else {
                String msg = "Received null response while auto-forwarding request to Genie instance";
                logger.error(msg);
                throw new CloudServiceException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
        } catch (CloudServiceException e) {
            // just raise it rightaway
            throw e;
        } catch (Exception e) {
            String msg = "Error while trying to auto-forward request: "
                    + e.getMessage();
            logger.error(msg, e);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        } finally {
            if (clientResponse != null) {
                clientResponse.releaseResources(); // this is really really
                                                   // important
            }
        }
    }
}
