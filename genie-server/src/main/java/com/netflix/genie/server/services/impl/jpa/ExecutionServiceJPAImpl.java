/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.services.impl.jpa;

import com.netflix.client.ClientFactory;
import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.client.http.HttpResponse;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.common.model.SubProcessStatus;
import com.netflix.genie.server.jobmanager.JobManagerFactory;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.repository.jpa.JobRepository;
import com.netflix.genie.server.repository.jpa.JobSpecs;
import com.netflix.genie.server.services.ExecutionService;
import com.netflix.genie.server.util.NetUtil;
import com.netflix.niws.client.http.RestClient;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityExistsException;
import javax.persistence.EntityManager;
import javax.persistence.LockModeType;
import javax.persistence.PersistenceContext;
import javax.persistence.RollbackException;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

/**
 * Implementation of the Genie Execution Service API that uses a local job
 * launcher (via the job manager implementation), and uses OpenJPA for
 * persistence.
 *
 * @author skrishnan
 * @author bmundlapudi
 * @author amsharma
 * @author tgianos
 */
@Named
@Transactional(rollbackFor = GenieException.class)
public class ExecutionServiceJPAImpl implements ExecutionService {

    private static final Logger LOG = LoggerFactory
            .getLogger(ExecutionServiceJPAImpl.class);

    // instance of the netflix configuration object
    private static final AbstractConfiguration CONF;

    // these can be over-ridden in the properties file
    private static final int SERVER_PORT;
    private static final String JOB_DIR_PREFIX;
    private static final String JOB_RESOURCE_PREFIX;

    // per-instance variables
    private final GenieNodeStatistics stats;
    private final JobRepository jobRepo;
    private final JobCountManager jobCountManager;
    private final JobManagerFactory jobManagerFactory;

    @PersistenceContext
    private EntityManager em;

    // initialize static variables
    static {
        //TODO: Seem to remember something about injecting configuration using governator
        CONF = ConfigurationManager.getConfigInstance();
        SERVER_PORT = CONF.getInt("netflix.appinfo.port", 7001);
        JOB_DIR_PREFIX = CONF.getString("netflix.genie.server.job.dir.prefix",
                "genie-jobs");
        JOB_RESOURCE_PREFIX = CONF.getString(
                "netflix.genie.server.job.resource.prefix", "genie/v2/jobs");
    }

    /**
     * Default constructor - initializes persistence manager, and other utility
     * classes.
     *
     * @param jobRepo The job repository to use.
     * @param stats the GenieNodeStatistics object
     * @param jobCountManager the job count manager to use
     * @param jobManagerFactory The the job manager factory to use
     */
    @Inject
    public ExecutionServiceJPAImpl(
            final JobRepository jobRepo,
            final GenieNodeStatistics stats,
            final JobCountManager jobCountManager,
            final JobManagerFactory jobManagerFactory) {
        this.jobRepo = jobRepo;
        this.stats = stats;
        this.jobCountManager = jobCountManager;
        this.jobManagerFactory = jobManagerFactory;
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Job submitJob(final Job job) throws GenieException {
        LOG.debug("Called");

        if (job == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job entered to run");
        }
        // validate parameters
        job.validate();
        job.setJobStatus(JobStatus.INIT, "Initializing job");
        final Job savedJob;

        // ensure that job won't overload system
        // synchronize until an entry is created and INIT-ed in DB
        // throttling related parameters
        final int maxRunningJobs = CONF.getInt(
                "netflix.genie.server.max.running.jobs", 0);
        final int jobForwardThreshold = CONF.getInt(
                "netflix.genie.server.forward.jobs.threshold", 0);
        final int maxIdleHostThreshold = CONF.getInt(
                "netflix.genie.server.max.idle.host.threshold", 0);
        final int idleHostThresholdDelta = CONF.getInt(
                "netflix.genie.server.idle.host.threshold.delta", 0);
        synchronized (this) {
            final int numRunningJobs = this.jobCountManager.getNumInstanceJobs();
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
            if (numRunningJobs >= jobForwardThreshold && !job.isForwarded()) {
                LOG.info("Number of running jobs greater than forwarding threshold - trying to auto-forward");
                final String idleHost = this.jobCountManager.getIdleInstance(idleHostThreshold);
                if (!idleHost.equals(NetUtil.getHostName())) {
                    job.setForwarded(true);
                    this.stats.incrGenieForwardedJobs();
                    return forwardJobRequest("http://" + idleHost + ":"
                            + SERVER_PORT + "/" + JOB_RESOURCE_PREFIX, job);
                } // else, no idle hosts found - run here if capacity exists
            }

            if (numRunningJobs >= maxRunningJobs) {
                // if we get here, job can't be forwarded to an idle
                // instance anymore and current node is overloaded
                throw new GenieException(
                        HttpURLConnection.HTTP_UNAVAILABLE,
                        "Number of running jobs greater than system limit ("
                        + maxRunningJobs
                        + ") - try another instance or try again later");
            }

            // if job can be launched, update the URIs
            buildJobURIs(job);

            // init state in DB - return if job already exists
            try {
                // TODO add retries to avoid deadlock issue
                savedJob = this.jobRepo.save(job);
            } catch (final RollbackException e) {
                LOG.error("Can't create entity in the database", e);
                if (e.getCause() instanceof EntityExistsException) {
                    // most likely entity already exists - return useful message
                    throw new GenieException(
                            HttpURLConnection.HTTP_CONFLICT,
                            "Job already exists for id: " + job.getId());
                } else {
                    // unknown exception - send it back
                    throw new GenieException(
                            HttpURLConnection.HTTP_INTERNAL_ERROR,
                            e);
                }
            }
        } // end synchronize

        if (savedJob == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to persist job");
        }

        // increment number of submitted jobs
        this.stats.incrGenieJobSubmissions();

        // try to run the job - return success or error
        try {
            this.jobManagerFactory.getJobManager(savedJob).launch();

            // update entity in DB
            savedJob.setUpdated(new Date());
            return savedJob;
        } catch (final GenieException e) {
            LOG.error("Failed to submit job: ", e);
            // update db
            savedJob.setJobStatus(JobStatus.FAILED, e.getMessage());
            // increment counter for failed jobs
            this.stats.incrGenieFailedJobs();
            return savedJob;
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Job getJobInfo(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered unable to retreive job.");
        }
        LOG.debug("called for id: " + id);

        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            return job;
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job exists for id " + id + ". Unable to retrieve.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public List<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final JobStatus status,
            final String clusterName,
            final String clusterId,
            final int limit,
            final int page) throws GenieException {
        LOG.debug("called");
        final PageRequest pageRequest = new PageRequest(
                page < 0 ? 0 : page,
                limit < 0 ? 1024 : limit
        );
        return this.jobRepo.findAll(
                JobSpecs.find(
                        id,
                        jobName,
                        userName,
                        status,
                        clusterName,
                        clusterId),
                pageRequest).getContent();
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public JobStatus getJobStatus(final String id) throws GenieException {
        return getJobInfo(id).getStatus();
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Job killJob(final String id) throws GenieException {
        if (StringUtils.isEmpty(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No id entered unable to kill job.");
        }
        LOG.debug("called for id: " + id);
        final Job job = this.jobRepo.findOne(id);

        // do some basic error handling
        if (job == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job exists for id " + id + ". Unable to kill.");
        }

        // check if it is done already
        if (job.getStatus() == JobStatus.SUCCEEDED
                || job.getStatus() == JobStatus.KILLED
                || job.getStatus() == JobStatus.FAILED) {
            // job already exited, return status to user
            return job;
        } else if (job.getStatus() == JobStatus.INIT
                || (job.getProcessHandle() == -1)) {
            // can't kill a job if it is still initializing
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Unable to kill job as it is still initializing");
        }

        // if we get here, job is still running - and can be killed
        // redirect to the right node if killURI points to a different node
        final String killURI = job.getKillURI();
        if (StringUtils.isEmpty(killURI)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Failed to get killURI for jobID: " + id);
        }
        final String localURI = getEndPoint() + "/" + JOB_RESOURCE_PREFIX + "/" + id;

        if (!killURI.equals(localURI)) {
            LOG.debug("forwarding kill request to: " + killURI);
            return forwardJobKill(killURI);
        }

        // if we get here, killURI == localURI, and job should be killed here
        LOG.debug("killing job on same instance: " + id);
        this.jobManagerFactory.getJobManager(job).kill();

        job.setJobStatus(JobStatus.KILLED, "Job killed on user request");
        job.setExitCode(SubProcessStatus.JOB_KILLED.code());

        // increment counter for killed jobs
        this.stats.incrGenieKilledJobs();

        LOG.debug("updating job status to KILLED for: " + id);
        // acquire write lock first, and then update status
        // if job status changed between when it was read and now,
        // this thread will simply overwrite it - final state will be KILLED
        this.em.lock(job, LockModeType.PESSIMISTIC_WRITE);
        if (!job.isDisableLogArchival()) {
            job.setArchiveLocation(NetUtil.getArchiveURI(id));
        }

        // all good - return results
        return job;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public int markZombies() {
        LOG.debug("called");
        // the equivalent query is as follows:
        // update Job set status='FAILED', finishTime=$max, exitCode=$zombie_code,
        // statusMsg='Job has been marked as a zombie'
        // where updateTime < $min and (status='RUNNING' or status='INIT')"
        final long currentTime = System.currentTimeMillis();
        final long zombieTime = CONF.getLong(
                "netflix.genie.server.janitor.zombie.delta.ms", 1800000);

        final List<Job> jobs = this.jobRepo.findAll(
                JobSpecs.findZombies(currentTime, zombieTime)
        );
        for (final Job job : jobs) {
            job.setStatus(JobStatus.FAILED);
            job.setFinishTime(currentTime);
            job.setExitCode(SubProcessStatus.ZOMBIE_JOB.code());
            job.setStatusMsg(SubProcessStatus.message(
                    SubProcessStatus.ZOMBIE_JOB.code())
            );
        }
        return jobs.size();
    }

    private void buildJobURIs(final Job job) throws GenieException {
        job.setHostName(NetUtil.getHostName());
        job.setOutputURI(getEndPoint() + "/" + JOB_DIR_PREFIX + "/" + job.getId());
        job.setKillURI(getEndPoint() + "/" + JOB_RESOURCE_PREFIX + "/" + job.getId());
    }

    private String getEndPoint() throws GenieException {
        return "http://" + NetUtil.getHostName() + ":" + SERVER_PORT;
    }

    private Job forwardJobKill(final String killURI) throws GenieException {
        return executeRequest(Verb.DELETE, killURI, null);
    }

    private Job forwardJobRequest(
            final String hostURI,
            final Job job) throws GenieException {
        return executeRequest(Verb.POST, hostURI, job);
    }

    private Job executeRequest(
            final Verb method,
            final String restURI,
            final Job job)
            throws GenieException {
        HttpResponse clientResponse = null;
        try {
            final RestClient genieClient = (RestClient) ClientFactory
                    .getNamedClient("genie");
            final HttpRequest req = HttpRequest.newBuilder()
                    .verb(method).header("Accept", "application/json")
                    .uri(new URI(restURI)).entity(job).build();
            clientResponse = genieClient.execute(req);
            if (clientResponse != null) {
                int status = clientResponse.getStatus();
                LOG.info("Response Status: " + status);
                return clientResponse.getEntity(Job.class);
            } else {
                String msg = "Received null response while auto-forwarding request to Genie instance";
                LOG.error(msg);
                throw new GenieException(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
            }
        } catch (final GenieException e) {
            // just raise it rightaway
            throw e;
        } catch (final Exception e) {
            final String msg = "Error while trying to auto-forward request: "
                    + e.getMessage();
            LOG.error(msg, e);
            throw new GenieException(HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        } finally {
            if (clientResponse != null) {
                // this is really really important
                clientResponse.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> addTagsForJob(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job id entered. Unable to add tags.");
        }
        if (tags == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tags entered.");
        }
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.getTags().addAll(tags);
            return job.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForJob(
            final String id)
            throws GenieException {

        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job id sent. Cannot retrieve tags.");
        }

        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            return job.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> updateTagsForJob(
            final String id,
            final Set<String> tags) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job id entered. Unable to update tags.");
        }
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setTags(tags);
            return job.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws GenieException
     */
    @Override
    public Set<String> removeAllTagsForJob(
            final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job id entered. Unable to remove tags.");
        }
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.getTags().clear();
            return job.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> removeTagForJob(String id, String tag)
            throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job id entered. Unable to remove tag.");
        }
        if (StringUtils.isBlank(tag)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No tag entered. Unable to remove tag.");
        }
        if (tag.equals(id)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "Cannot delete job id from the tags list.");
        }
        
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.getTags().remove(tag);
            return job.getTags();
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND,
                    "No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JobStatus finalizeJob(final String id, final int exitCode) throws GenieException {
        final Job job = this.jobRepo.findOne(id);
        if (job == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }
        job.setExitCode(exitCode);

        // only update status if not KILLED
        if (job.getStatus() != null && job.getStatus() != JobStatus.KILLED) {
            if (exitCode != SubProcessStatus.SUCCESS.code()) {
                // all other failures except s3 log archival failure
                LOG.error("Failed to execute job, exit code: "
                        + exitCode);
                String errMsg = SubProcessStatus.message(exitCode);
                if ((errMsg == null) || (errMsg.isEmpty())) {
                    errMsg = "Please look at job's stderr for more details";
                }
                job.setJobStatus(JobStatus.FAILED,
                        "Failed to execute job, Error Message: " + errMsg);
                // incr counter for failed jobs
                this.stats.incrGenieFailedJobs();
            } else {
                // success
                job.setJobStatus(JobStatus.SUCCEEDED,
                        "Job finished successfully");
                // incr counter for successful jobs
                this.stats.incrGenieSuccessfulJobs();
            }

            // set the archive location - if needed
            if (!job.isDisableLogArchival()) {
                job.setArchiveLocation(NetUtil.getArchiveURI(job.getId()));
            }

            // update the job status
            job.setUpdated(new Date());
            return job.getStatus();
        } else {
            // if job status is killed, the kill thread will update status
            LOG.debug("Job has been killed - will not update DB: " + job.getId());
            return JobStatus.KILLED;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long updateJob(final String id) throws GenieException {
        LOG.debug("Updating db for job: " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job == null) {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }

        final long lastUpdatedTimeMS = System.currentTimeMillis();
        job.setJobStatus(JobStatus.RUNNING, "Job is running");
        job.setUpdated(new Date(lastUpdatedTimeMS));
        return lastUpdatedTimeMS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setJobStatus(final String id, final JobStatus status, final String msg) throws GenieException {
        LOG.debug("Failing job with id " + id + " for reason " + msg);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setJobStatus(status, msg);
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setProcessIdForJob(final String id, final int pid) throws GenieException {
        LOG.debug("Setting the id of process for job with id " + id + " to " + pid);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setProcessHandle(pid);
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCommandInfoForJob(
            final String id,
            final String commandId,
            final String commandName) throws GenieException {
        LOG.debug("Setting the command info for job with id " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setCommandId(commandId);
            job.setCommandName(commandName);
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setApplicationInfoForJob(
            final String id,
            final String appId,
            final String appName) throws GenieException {
        LOG.debug("Setting the application info for job with id " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setApplicationId(appId);
            job.setApplicationName(appName);
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClusterInfoForJob(
            final String id,
            final String clusterId,
            final String clusterName) throws GenieException {
        LOG.debug("Setting the application info for job with id " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setExecutionClusterId(clusterId);
            job.setExecutionClusterName(clusterName);
        } else {
            throw new GenieException(
                    HttpURLConnection.HTTP_NOT_FOUND, "No job with id " + id + " exists");
        }
    }
}
