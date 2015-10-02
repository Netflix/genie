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
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobmanager.JobManagerFactory;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.repositories.JobRepository;
import com.netflix.genie.core.jpa.repositories.JobSpecs;
import com.netflix.genie.core.metrics.GenieNodeStatistics;
import com.netflix.genie.core.services.JobService;
import com.netflix.genie.core.util.NetUtil;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.orm.jpa.JpaOptimisticLockingFailureException;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.Set;
import java.util.UUID;

/**
 * Implementation of the Job Service APIs.
 *
 * @author tgianos
 */
@Service
public class JobServiceJPAImpl implements JobService {

    private static final Logger LOG = LoggerFactory.getLogger(JobServiceJPAImpl.class);

    private final GenieNodeStatistics stats;
    private final JobRepository jobRepo;
    private final JobManagerFactory jobManagerFactory;
    private final NetUtil netUtil;

    @Value("${netflix.appinfo.port:8080}")
    private int serverPort;
    @Value("${com.netflix.genie.server.job.resource.prefix:genie/v2/jobs}")
    private String jobResourcePrefix;
    @Value("${com.netflix.genie.server.job.dir.prefix:genie-jobs}")
    private String jobDirPrefix;

    /**
     * Constructor.
     *
     * @param jobRepo           The job repository to use.
     * @param stats             The GenieNodeStatistics object
     * @param jobManagerFactory The the job manager factory to use
     * @param netUtil           The network utility code to use
     */
    @Autowired
    public JobServiceJPAImpl(
            final JobRepository jobRepo,
            final GenieNodeStatistics stats,
            final JobManagerFactory jobManagerFactory,
            final NetUtil netUtil
    ) {
        this.jobRepo = jobRepo;
        this.stats = stats;
        this.jobManagerFactory = jobManagerFactory;
        this.netUtil = netUtil;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public Job createJob(
            @NotNull(message = "No job entered. Unable to create.")
            @Valid
            final JobRequest jobRequest
    ) throws GenieException {
        final String requestId = jobRequest.getId();
        if (StringUtils.isNotBlank(requestId) && this.jobRepo.exists(requestId)) {
            throw new GenieConflictException("A job with id " + requestId + " already exists. Unable to save.");
        }

        final JobEntity jobEntity = new JobEntity(
                jobRequest.getUser(),
                jobRequest.getName(),
                jobRequest.getVersion(),
                jobRequest.getCommandArgs(),
                jobRequest.getCommandCriteria(),
                jobRequest.getClusterCriterias()
        );

        jobEntity.setId(StringUtils.isBlank(requestId) ? UUID.randomUUID().toString() : requestId);
        jobEntity.setJobStatus(JobStatus.INIT, "Initializing job");

        // Validation successful. init state in DB - return if job already exists
        try {
            // if job can be launched, update the URIs
            final String hostName = this.netUtil.getHostName();
            jobEntity.setHostName(hostName);
            final String endpoint = getEndPoint(hostName);
            jobEntity.setOutputURI(endpoint + "/" + this.jobDirPrefix + "/" + jobEntity.getId());
            jobEntity.setKillURI(endpoint + "/" + this.jobResourcePrefix + "/" + jobEntity.getId());
            final JobEntity attachedJobEntity = this.jobRepo.save(jobEntity);

            // increment number of submitted jobs as we have successfully
            // persisted it in the database.
            this.stats.incrGenieJobSubmissions();
            return attachedJobEntity.getDTO();
        } catch (final RuntimeException e) {
            //This will catch runtime as well
            LOG.error("Can't create entity in the database", e);
            throw new GenieServerException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Job getJob(
            @NotBlank(message = "No id entered. Unable to get job.")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called for id: " + id);
        }

        return this.findJob(id).getDTO();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Page<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final Pageable page
    ) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called");
        }

        @SuppressWarnings("unchecked")
        final Page<JobEntity> jobEntities = this.jobRepo.findAll(
                JobSpecs.find(
                        id,
                        jobName,
                        userName,
                        statuses,
                        tags,
                        clusterName,
                        clusterId,
                        commandName,
                        commandId),
                page
        );
        return jobEntities.map(JobEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public JobStatus getJobStatus(
            @NotBlank(message = "No id entered. Unable to get status.")
            final String id
    ) throws GenieException {
        return getJob(id).getStatus();
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
    public void setJobStatus(
            @NotBlank(message = "No id entered for the job. Unable to update the status.")
            final String id,
            @NotNull(message = "No status entered unable to update.")
            final JobStatus status,
            final String msg
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting job with id " + id + " to status " + status + " for reason " + msg);
        }
        this.findJob(id).setJobStatus(status, msg);
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
    @Retryable(JpaOptimisticLockingFailureException.class)
    public long setUpdateTime(
            @NotBlank(message = "No job id entered. Unable to set update time.")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating db for job: " + id);
        }
        final JobEntity jobEntity = this.findJob(id);

        final long lastUpdatedTimeMS = System.currentTimeMillis();
        jobEntity.setJobStatus(JobStatus.RUNNING, "Job is running");
        jobEntity.setUpdated(new Date(lastUpdatedTimeMS));
        return lastUpdatedTimeMS;
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
    public void setProcessIdForJob(
            @NotBlank(message = "No job id entered. Unable to set process id")
            final String id,
            final int pid
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting the id of process for job with id " + id + " to " + pid);
        }
        this.findJob(id).setProcessHandle(pid);
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
    public void setCommandInfoForJob(
            @NotBlank(message = "No job id entered. Unable to set command info for job.")
            final String id,
            @NotBlank(message = "No command id entered. Unable to set command info for job.")
            final String commandId,
            @NotBlank(message = "No command name entered. Unable to set command info for job.")
            final String commandName
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting the command info for job with id " + id);
        }
        final JobEntity jobEntity = this.findJob(id);
        //TODO: Should we check if this is valid
        jobEntity.setCommandId(commandId);
        jobEntity.setCommandName(commandName);
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
    public void setApplicationInfoForJob(
            @NotBlank(message = "No job id entered. Unable to update app info for job.")
            final String id,
            @NotBlank(message = "No app id entered. Unable to update app info for job.")
            final String appId,
            @NotBlank(message = "No app name entered. unable to update app info for job.")
            final String appName
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting the application info for job with id " + id);
        }
        final JobEntity jobEntity = this.findJob(id);
        jobEntity.setApplicationId(appId);
        jobEntity.setApplicationName(appName);
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
    public void setClusterInfoForJob(
            @NotBlank(message = "No job id entered. Unable to update cluster info for job.")
            final String id,
            @NotBlank(message = "No cluster id entered. Unable to update cluster info for job.")
            final String clusterId,
            @NotBlank(message = "No cluster name entered. Unable to update cluster info for job.")
            final String clusterName
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting the application info for job with id " + id);
        }
        final JobEntity jobEntity = this.findJob(id);
        jobEntity.setExecutionClusterId(clusterId);
        jobEntity.setExecutionClusterName(clusterName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public Job runJob(
            @NotNull(message = "No job entered unable to run")
            @Valid
            final Job job
    ) throws GenieException {
        try {
            this.jobManagerFactory.getJobManager(job).launch();

            // update entity in DB
            // TODO This update runs into deadlock issue, either add manual retries
            // or Spring retries.
            final JobEntity runningJobEntity = this.findJob(job.getId());
            runningJobEntity.setUpdated(new Date());
            return runningJobEntity.getDTO();
        } catch (final GenieException e) {
            LOG.error("Failed to run job: ", e);
            final JobEntity failedJobEntity = this.findJob(job.getId());
            // update db
            failedJobEntity.setJobStatus(JobStatus.FAILED, e.getMessage());
            // increment counter for failed jobs
            this.stats.incrGenieFailedJobs();
            throw e;
        }
    }

    private String getEndPoint(final String hostName) throws GenieException {
        return "http://" + hostName + ":" + this.serverPort;
    }

    private JobEntity findJob(final String id) throws GenieNotFoundException {
        final JobEntity jobEntity = this.jobRepo.findOne(id);
        if (jobEntity != null) {
            return jobEntity;
        } else {
            throw new GenieNotFoundException("No job exists with id: " + id + ". Unable to find.");
        }
    }
}
