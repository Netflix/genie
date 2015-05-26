/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import com.netflix.client.http.HttpRequest;
import com.netflix.client.http.HttpRequest.Verb;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.client.BaseGenieClient;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.common.util.ProcessStatus;
import com.netflix.genie.server.jobmanager.JobManagerFactory;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.metrics.JobCountManager;
import com.netflix.genie.server.repository.jpa.JobRepository;
import com.netflix.genie.server.repository.jpa.JobSpecs;
import com.netflix.genie.server.services.ExecutionService;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.NetUtil;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Date;
import java.util.List;

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
public class ExecutionServiceJPAImpl implements ExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionServiceJPAImpl.class);

    // instance of the netflix configuration object
    private static final AbstractConfiguration CONF;

    // these can be over-ridden in the properties file
    private static final int SERVER_PORT;
    private static final String JOB_RESOURCE_PREFIX;

    // initialize static variables
    static {
        //TODO: Seem to remember something about injecting configuration using governator
        CONF = ConfigurationManager.getConfigInstance();
        SERVER_PORT = CONF.getInt("netflix.appinfo.port", 7001);
        JOB_RESOURCE_PREFIX = CONF.getString(
                "com.netflix.genie.server.job.resource.prefix", "genie/v2/jobs");
    }

    // per-instance variables
    private final GenieNodeStatistics stats;
    private final JobRepository jobRepo;
    private final JobCountManager jobCountManager;
    private final JobManagerFactory jobManagerFactory;
    private final JobService jobService;

    /**
     * Default constructor - initializes persistence manager, and other utility
     * classes.
     *
     * @param jobRepo           The job repository to use.
     * @param stats             the GenieNodeStatistics object
     * @param jobCountManager   the job count manager to use
     * @param jobManagerFactory The the job manager factory to use
     * @param jobService        The job service to use.
     */
    public ExecutionServiceJPAImpl(
            final JobRepository jobRepo,
            final GenieNodeStatistics stats,
            final JobCountManager jobCountManager,
            final JobManagerFactory jobManagerFactory,
            final JobService jobService) {
        this.jobRepo = jobRepo;
        this.stats = stats;
        this.jobCountManager = jobCountManager;
        this.jobManagerFactory = jobManagerFactory;
        this.jobService = jobService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Job submitJob(
            @NotNull(message = "No job entered to run")
            @Valid
            final Job job
    ) throws GenieException {
        LOG.debug("Called");
        if (StringUtils.isNotBlank(job.getId()) && this.jobRepo.exists(job.getId())) {
            throw new GenieConflictException("Job with ID specified already exists.");
        }

        // Check if the job is forwarded. If not this is the first node that got the request.
        // So we log it, to track all requests.
        if (!job.isForwarded()) {
            LOG.info("Received job request:" + job);
        }

        final Job forwardedJob = checkAbilityToRunOrForward(job);

        if (forwardedJob != null) {
            return forwardedJob;
        }

        // At this point we have established that the job can be run on this node.
        // Before running we validate the job and save it in the db if it passes validation.
        final Job savedJob = this.jobService.createJob(job);

        // try to run the job - return success or error
        return this.jobService.runJob(savedJob);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(
            rollbackFor = {
                    GenieException.class,
                    ConstraintViolationException.class
            }
    )
    public Job killJob(
            @NotBlank(message = "No id entered unable to kill job.")
            final String id
    ) throws GenieException {
        LOG.debug("called for id: " + id);
        final Job job = this.jobRepo.findOne(id);

        // do some basic error handling
        if (job == null) {
            throw new GenieNotFoundException("No job exists for id " + id + ". Unable to kill.");
        }

        // check if it is done already
        if (job.getStatus() == JobStatus.SUCCEEDED
                || job.getStatus() == JobStatus.KILLED
                || job.getStatus() == JobStatus.FAILED) {
            // job already exited, return status to user
            return job;
        } else if (job.getStatus() == JobStatus.INIT
                || job.getProcessHandle() == -1) {
            // can't kill a job if it is still initializing
            throw new GeniePreconditionException("Unable to kill job as it is still initializing");
        }

        // if we get here, job is still running - and can be killed
        // redirect to the right node if killURI points to a different node
        final String killURI = job.getKillURI();
        if (StringUtils.isBlank(killURI)) {
            throw new GeniePreconditionException("Failed to get killURI for jobID: " + id);
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
        job.setExitCode(ProcessStatus.JOB_KILLED.getExitCode());

        // increment counter for killed jobs
        this.stats.incrGenieKilledJobs();

        LOG.debug("updating job status to KILLED for: " + id);
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
        final ProcessStatus zombie = ProcessStatus.ZOMBIE_JOB;
        final long currentTime = new Date().getTime();
        final long zombieTime = CONF.getLong("com.netflix.genie.server.janitor.zombie.delta.ms", 1800000);

        @SuppressWarnings("unchecked")
        final List<Job> jobs = this.jobRepo.findAll(
                JobSpecs.findZombies(currentTime, zombieTime)
        );
        for (final Job job : jobs) {
            job.setStatus(JobStatus.FAILED);
            job.setFinished(new Date());
            job.setExitCode(zombie.getExitCode());
            job.setStatusMsg(zombie.getMessage());
        }
        return jobs.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(
            rollbackFor = {
                    GenieException.class,
                    ConstraintViolationException.class
            }
    )
    public JobStatus finalizeJob(
            @NotBlank(message = "No job id entered. Unable to finalize.")
            final String id,
            final int exitCode
    ) throws GenieException {
        final Job job = this.jobRepo.findOne(id);
        if (job == null) {
            throw new GenieNotFoundException("No job with id " + id + " exists");
        }
        job.setExitCode(exitCode);

        // We check if status code is killed. The kill thread sets this, but just to make sure we set
        // it here again to prevent a race condition problem. This just makes the status message as
        // killed and prevents some jobs that are killed being marked as failed
        if (exitCode == ProcessStatus.JOB_KILLED.getExitCode()) {
            LOG.debug("Process has been killed, therefore setting the appropriate status message.");
            job.setJobStatus(JobStatus.KILLED, "Job killed on user request");
            return JobStatus.KILLED;
        } else {
            if (exitCode != ProcessStatus.SUCCESS.getExitCode()) {
                // all other failures except s3 log archival failure
                LOG.error("Failed to execute job, exit code: "
                        + exitCode);
                String errMsg;
                try {
                    errMsg = ProcessStatus.parse(exitCode).getMessage();
                } catch (final GenieException ge) {
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

            // set the updated time
            job.setUpdated(new Date());
            return job.getStatus();
        }
    }

    private String getEndPoint() throws GenieException {
        return "http://" + NetUtil.getHostName() + ":" + SERVER_PORT;
    }

    private Job forwardJobKill(final String killURI) throws GenieException {
        try {
            final BaseGenieClient client = new BaseGenieClient(null);
            final HttpRequest request = BaseGenieClient.buildRequest(Verb.DELETE, killURI, null, null);
            return (Job) client.executeRequest(request, null, Job.class);
        } catch (final IOException ioe) {
            throw new GenieServerException(ioe.getMessage(), ioe);
        }
    }

    private Job forwardJobRequest(
            final String hostURI,
            final Job job) throws GenieException {
        try {
            final BaseGenieClient client = new BaseGenieClient(null);
            final HttpRequest request = BaseGenieClient.buildRequest(Verb.POST, hostURI, null, job);
            return (Job) client.executeRequest(request, null, Job.class);
        } catch (final IOException ioe) {
            throw new GenieServerException(ioe.getMessage(), ioe);
        }
    }

    /**
     * Check if we can run the job on this host or not.
     *
     * @throws GenieException
     */
    private synchronized Job checkAbilityToRunOrForward(
            final Job job) throws GenieException {
        // ensure that job won't overload system
        // synchronize until an entry is created and INIT-ed in DB
        // throttling related parameters
        final int maxRunningJobs = CONF.getInt(
                "com.netflix.genie.server.max.running.jobs", 0);
        final int jobForwardThreshold = CONF.getInt(
                "com.netflix.genie.server.forward.jobs.threshold", 0);
        final int maxIdleHostThreshold = CONF.getInt(
                "com.netflix.genie.server.max.idle.host.threshold", 0);
        final int idleHostThresholdDelta = CONF.getInt(
                "com.netflix.genie.server.idle.host.threshold.delta", 0);

        final int numRunningJobs = this.jobCountManager.getNumInstanceJobs();
        LOG.info("Number of running jobs: " + numRunningJobs);

        // find an instance with fewer than (numRunningJobs -
        // idleHostThresholdDelta)
        int idleHostThreshold = numRunningJobs - idleHostThresholdDelta;
        // if numRunningJobs is already >= maxRunningJobs, forward
        // aggressively
        // but cap it at the max
        if (idleHostThreshold > maxIdleHostThreshold
                || numRunningJobs >= maxRunningJobs) {
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

        // TODO: Gotta be something better we can do than this
        if (numRunningJobs >= maxRunningJobs) {
            // if we get here, job can't be forwarded to an idle
            // instance anymore and current node is overloaded
            throw new GenieServerUnavailableException(
                    "Number of running jobs greater than system limit ("
                            + maxRunningJobs
                            + ") - try another instance or try again later");
        }

        //We didn't forward the job so return null to signal to run the job locally
        return null;
    }
}
