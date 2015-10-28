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
package com.netflix.genie.core.jpa.services;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.util.ProcessStatus;
import com.netflix.genie.core.jobmanager.JobManagerFactory;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.specifications.JpaJobSpecs;
import com.netflix.genie.core.metrics.GenieNodeStatistics;
import com.netflix.genie.core.services.ExecutionService;
import com.netflix.genie.core.services.JobService;
import com.netflix.genie.core.util.NetUtil;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

/**
 * Implementation of the Genie Execution Service API that uses a local job
 * launcher (via the job manager implementation), and uses OpenJPA for
 * persistence.
 *
 * @author tgianos
 * @author amsharma
 */
@Service
public class JpaExecutionServiceImpl implements ExecutionService {

    private static final Logger LOG = LoggerFactory.getLogger(JpaExecutionServiceImpl.class);

    // per-instance variables
    private final GenieNodeStatistics stats;
    private final JpaJobRepository jobRepo;
    private final JobManagerFactory jobManagerFactory;
    private final JobService jobService;
    private final NetUtil netUtil;

    @Value("${netflix.appinfo.port:8080}")
    private int serverPort;
    @Value("${com.netflix.genie.server.job.resource.prefix:genie/v2/jobs}")
    private String jobResourcePrefix;
    @Value("${com.netflix.genie.server.max.running.jobs:0}")
    private int maxRunningJobs;
    @Value("${com.netflix.genie.server.forward.jobs.threshold:0}")
    private int jobForwardThreshold;
    @Value("${com.netflix.genie.server.max.idle.host.threshold:0}")
    private int maxIdleHostThreshold;
    @Value("${com.netflix.genie.server.idle.host.threshold.delta:0}")
    private int idleHostThresholdDelta;
    @Value("${com.netflix.genie.server.janitor.zombie.delta.ms:1800000}")
    private long zombieTime;

    /**
     * Default constructor - initializes persistence manager, and other utility
     * classes.
     *
     * @param jobRepo           The job repository to use.
     * @param stats             the GenieNodeStatistics object
     * @param jobManagerFactory The the job manager factory to use
     * @param jobService        The job service to use.
     * @param netUtil           The network utility to use
     */
    @Autowired
    public JpaExecutionServiceImpl(
            final JpaJobRepository jobRepo,
            final GenieNodeStatistics stats,
            final JobManagerFactory jobManagerFactory,
            final JobService jobService,
            final NetUtil netUtil
    ) {
        this.jobRepo = jobRepo;
        this.stats = stats;
        this.jobManagerFactory = jobManagerFactory;
        this.jobService = jobService;
        this.netUtil = netUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String submitJob(
            @NotNull(message = "No job request entered to run")
            @Valid
            final JobRequest jobRequest
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called");
        }
        if (StringUtils.isNotBlank(jobRequest.getId()) && this.jobRepo.exists(jobRequest.getId())) {
            throw new GenieConflictException("Job with ID specified already exists.");
        }

        // Check if the job is forwarded. If not this is the first node that got the request.
        // So we log it, to track all requests.
//        if (!job.isForwarded()) {
//            LOG.debug("Received job request:" + jobRequest);
//        }
//
//        final Job forwardedJob = checkAbilityToRunOrForward(job);
//
//        if (forwardedJob != null) {
//            return forwardedJob;
//        }

        // At this point we have established that the job can be run on this node.
        // Before running we validate the job and save it in the db if it passes validation.
        final Job savedJob = this.jobService.createJob(jobRequest);

        // try to run the job - return success or error
        return this.jobService.runJob(savedJob).getId();
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("called for id: " + id);
        }
        final JobEntity jobEntity = this.findJob(id);

        // check if it is done already
        if (jobEntity.getStatus() == JobStatus.SUCCEEDED
                || jobEntity.getStatus() == JobStatus.KILLED
                || jobEntity.getStatus() == JobStatus.FAILED) {
            // job already exited, return status to user
            return jobEntity.getDTO();
        } else if (jobEntity.getStatus() == JobStatus.INIT
                || jobEntity.getProcessHandle() == -1) {
            // can't kill a job if it is still initializing
            throw new GeniePreconditionException("Unable to kill job as it is still initializing");
        }

        // if we get here, job is still running - and can be killed
        // redirect to the right node if killURI points to a different node
        final String killURI = jobEntity.getKillURI();
        if (StringUtils.isBlank(killURI)) {
            throw new GeniePreconditionException("Failed to get killURI for jobID: " + id);
        }
//        final String localURI = getEndPoint() + "/" + this.jobResourcePrefix + "/" + id;
//
//        if (!killURI.equals(localURI)) {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("forwarding kill request to: " + killURI);
//            }
//            return forwardJobKill(killURI);
//        }

        // if we get here, killURI == localURI, and job should be killed here
        if (LOG.isDebugEnabled()) {
            LOG.debug("killing job on same instance: " + id);
        }
        this.jobManagerFactory.getJobManager(jobEntity.getDTO()).kill();

        jobEntity.setJobStatus(JobStatus.KILLED, "Job killed on user request");
        jobEntity.setExitCode(ProcessStatus.JOB_KILLED.getExitCode());

        // increment counter for killed jobs
        this.stats.incrGenieKilledJobs();

        if (LOG.isDebugEnabled()) {
            LOG.debug("updating job status to KILLED for: " + id);
        }
        if (!jobEntity.isDisableLogArchival()) {
            jobEntity.setArchiveLocation(this.netUtil.getArchiveURI(id));
        }

        // all good - return results
        return jobEntity.getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public int markZombies() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }
        final ProcessStatus zombie = ProcessStatus.ZOMBIE_JOB;
        final long currentTime = new Date().getTime();

        @SuppressWarnings("unchecked")
        final List<JobEntity> jobEntities
                = this.jobRepo.findAll(JpaJobSpecs.findZombies(currentTime, this.zombieTime));
        for (final JobEntity jobEntity : jobEntities) {
            jobEntity.setStatus(JobStatus.FAILED);
            jobEntity.setFinished(new Date());
            jobEntity.setExitCode(zombie.getExitCode());
            jobEntity.setStatusMsg(zombie.getMessage());
        }
        return jobEntities.size();
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
        final JobEntity jobEntity = this.findJob(id);
        jobEntity.setExitCode(exitCode);

        // We check if status code is killed. The kill thread sets this, but just to make sure we set
        // it here again to prevent a race condition problem. This just makes the status message as
        // killed and prevents some jobs that are killed being marked as failed
        if (exitCode == ProcessStatus.JOB_KILLED.getExitCode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Process has been killed, therefore setting the appropriate status message.");
            }
            jobEntity.setJobStatus(JobStatus.KILLED, "Job killed on user request");
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
                jobEntity.setJobStatus(JobStatus.FAILED,
                        "Failed to execute job, Error Message: " + errMsg);
                // incr counter for failed jobs
                this.stats.incrGenieFailedJobs();
            } else {
                // success
                jobEntity.setJobStatus(JobStatus.SUCCEEDED,
                        "Job finished successfully");
                // incr counter for successful jobs
                this.stats.incrGenieSuccessfulJobs();
            }

            // set the archive location - if needed
            if (!jobEntity.isDisableLogArchival()) {
                jobEntity.setArchiveLocation(this.netUtil.getArchiveURI(jobEntity.getId()));
            }

            // set the updated time
            jobEntity.setUpdated(new Date());
            return jobEntity.getStatus();
        }
    }

//    private String getEndPoint() throws GenieException {
//        return "http://" + this.netUtil.getHostName() + ":" + this.serverPort;
//    }
//
//    private Job forwardJobKill(final String killURI) throws GenieException {
//        throw new UnsupportedOperationException("Not yet implemented");
//        try {
//            final BaseGenieClient client = new BaseGenieClient(null);
//            final HttpRequest request = BaseGenieClient.buildRequest(Verb.DELETE, killURI, null, null);
//            return (Job) client.executeRequest(request, null, Job.class);
//        } catch (final IOException ioe) {
//            throw new GenieServerException(ioe.getMessage(), ioe);
//        }
//    }
//
//    private Job forwardJobRequest(final String hostURI, final Job job) throws GenieException {
//        throw new UnsupportedOperationException("Not yet implemented");
//        try {
//            final BaseGenieClient client = new BaseGenieClient(null);
//            final HttpRequest request = BaseGenieClient.buildRequest(Verb.POST, hostURI, null, job);
//            return (Job) client.executeRequest(request, null, Job.class);
//        } catch (final IOException ioe) {
//            throw new GenieServerException(ioe.getMessage(), ioe);
//        }
//    }
//
//    /**
//     * Check if we can run the job on this host or not.
//     *
//     * @throws GenieException
//     */
//    private synchronized Job checkAbilityToRunOrForward(final Job job) throws GenieException {
//
//        final int numRunningJobs = this.jobCountManager.getNumInstanceJobs();
//        LOG.info("Number of running jobs: " + numRunningJobs);
//
//        // find an instance with fewer than (numRunningJobs -
//        // idleHostThresholdDelta)
//        int idleHostThreshold = numRunningJobs - this.idleHostThresholdDelta;
//        // if numRunningJobs is already >= maxRunningJobs, forward
//        // aggressively
//        // but cap it at the max
//        if (idleHostThreshold > this.maxIdleHostThreshold || numRunningJobs >= this.maxRunningJobs) {
//            idleHostThreshold = this.maxIdleHostThreshold;
//        }
//
//        // check to see if job should be forwarded - only forward it
//        // once. the assumption is that jobForwardThreshold < maxRunningJobs
//        // (set in properties file)
//        if (numRunningJobs >= this.jobForwardThreshold && !job.isForwarded()) {
//            LOG.info("Number of running jobs greater than forwarding threshold - trying to auto-forward");
//            final String idleHost = this.jobCountManager.getIdleInstance(idleHostThreshold);
//            if (!idleHost.equals(this.netUtil.getHostName())) {
//                job.setForwarded(true);
//                this.stats.incrGenieForwardedJobs();
//                return forwardJobRequest(
//                        "http://" + idleHost + ":" + this.serverPort + "/" + this.jobResourcePrefix, job
//                );
//            } // else, no idle hosts found - run here if capacity exists
//        }
//
//        // TODO: Gotta be something better we can do than this
//        if (numRunningJobs >= maxRunningJobs) {
//            // if we get here, job can't be forwarded to an idle
//            // instance anymore and current node is overloaded
//            throw new GenieServerUnavailableException(
//                    "Number of running jobs greater than system limit ("
//                            + maxRunningJobs
//                            + ") - try another instance or try again later");
//        }
//
//        //We didn't forward the job so return null to signal to run the job locally
//        return null;
//    }

    private JobEntity findJob(final String id) throws GenieNotFoundException {
        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity != null) {
            return jobEntity;
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
    }
}
