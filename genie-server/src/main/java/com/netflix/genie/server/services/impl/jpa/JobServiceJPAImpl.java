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

import com.netflix.config.ConfigurationManager;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import com.netflix.genie.server.jobmanager.JobManagerFactory;
import com.netflix.genie.server.metrics.GenieNodeStatistics;
import com.netflix.genie.server.repository.jpa.JobRepository;
import com.netflix.genie.server.repository.jpa.JobSpecs;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.NetUtil;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import com.netflix.genie.common.model.Job_;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Implementation of the Job Service API's.
 *
 * @author tgianos
 */
public class JobServiceJPAImpl implements JobService {

    private static final Logger LOG = LoggerFactory
            .getLogger(JobServiceJPAImpl.class);

    // instance of the netflix configuration object
    private static final AbstractConfiguration CONF;

    // these can be over-ridden in the properties file
    private static final int SERVER_PORT;
    private static final String JOB_DIR_PREFIX;
    private static final String JOB_RESOURCE_PREFIX;

    // initialize static variables
    static {
        //TODO: Seem to remember something about injecting configuration using governator
        CONF = ConfigurationManager.getConfigInstance();
        SERVER_PORT = CONF.getInt("netflix.appinfo.port", 8080);
        JOB_DIR_PREFIX = CONF.getString("com.netflix.genie.server.job.dir.prefix",
                "genie-jobs");
        JOB_RESOURCE_PREFIX = CONF.getString(
                "com.netflix.genie.server.job.resource.prefix", "genie/v2/jobs");
    }

    private final GenieNodeStatistics stats;
    private final JobRepository jobRepo;
    private final JobManagerFactory jobManagerFactory;

    /**
     * Constructor.
     *
     * @param jobRepo           The job repository to use.
     * @param stats             the GenieNodeStatistics object
     * @param jobManagerFactory The the job manager factory to use
     */
    public JobServiceJPAImpl(
            final JobRepository jobRepo,
            final GenieNodeStatistics stats,
            final JobManagerFactory jobManagerFactory
    ) {
        this.jobRepo = jobRepo;
        this.stats = stats;
        this.jobManagerFactory = jobManagerFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public Job createJob(
            @NotNull(message = "No job entered. Unable to create.")
            @Valid
            final Job job
    ) throws GenieException {
        if (StringUtils.isNotEmpty(job.getId()) && this.jobRepo.exists(job.getId())) {
            throw new GenieConflictException("A job with id " + job.getId() + " already exists. Unable to save.");
        }
        // validate parameters
        job.setJobStatus(JobStatus.INIT, "Initializing job");

        // Validation successful. init state in DB - return if job already exists
        try {
            final Job persistedJob = this.jobRepo.save(job);
            // if job can be launched, update the URIs
            final String hostName = NetUtil.getHostName();
            persistedJob.setHostName(hostName);
            persistedJob.setOutputURI(
                    getEndPoint(hostName)
                            + "/" + JOB_DIR_PREFIX
                            + "/" + persistedJob.getId()
            );
            persistedJob.setKillURI(
                    getEndPoint(hostName)
                            + "/" + JOB_RESOURCE_PREFIX
                            + "/" + persistedJob.getId()
            );

            // increment number of submitted jobs as we have successfully
            // persisted it in the database.
            this.stats.incrGenieJobSubmissions();
            return persistedJob;
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
        LOG.debug("called for id: " + id);

        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            return job;
        } else {
            throw new GenieNotFoundException(
                    "No job exists for id " + id + ". Unable to retrieve.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<Job> getJobs(
            final String id,
            final String jobName,
            final String userName,
            final Set<JobStatus> statuses,
            final Set<String> tags,
            final String clusterName,
            final String clusterId,
            final String commandName,
            final String commandId,
            final int page,
            final int limit,
            final boolean descending,
            final Set<String> orderBys) {
        LOG.debug("called");

        final PageRequest pageRequest = JPAUtils.getPageRequest(
                page, limit, descending, orderBys, Job_.class, Job_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Job> jobs = this.jobRepo.findAll(
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
                pageRequest).getContent();
        return jobs;
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
        LOG.debug("Setting job with id " + id + " to status " + status + " for reason " + msg);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setJobStatus(status, msg);
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists");
        }
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
    public Set<String> addTagsForJob(
            @NotBlank(message = "No id entered. Unable to add tags.")
            final String id,
            @NotEmpty(message = "No tags entered to add.")
            final Set<String> tags
    ) throws GenieException {
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.getTags().addAll(tags);
            return job.getTags();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<String> getTagsForJob(
            @NotBlank(message = "No job id entered. Unable to get tags.")
            final String id
    ) throws GenieException {
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            return job.getTags();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
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
    public Set<String> updateTagsForJob(
            @NotBlank(message = "No job id entered. Unable to update tags.")
            final String id,
            @NotEmpty(message = "No tags entered. Unable to update tags.")
            final Set<String> tags
    ) throws GenieException {
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setTags(tags);
            return job.getTags();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
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
    public Set<String> removeAllTagsForJob(
            @NotBlank(message = "No job id entered. Unable to remove tags.")
            final String id
    ) throws GenieException {
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.getTags().clear();
            return job.getTags();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
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
    public Set<String> removeTagForJob(
            @NotBlank(message = "No id entered for job. Unable to remove tag.")
            final String id,
            @NotBlank(message = "No tag entered. Unable to remove")
            final String tag
    ) throws GenieException {
        if (id.equals(tag)) {
            throw new GeniePreconditionException(
                    "Cannot delete job id from the tags list.");
        }

        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            if (StringUtils.isNotBlank(tag)) {
                job.getTags().remove(tag);
            }
            return job.getTags();
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists.");
        }
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
    public long setUpdateTime(
            @NotBlank(message = "No job id entered. Unable to set update time.")
            final String id
    ) throws GenieException {
        LOG.debug("Updating db for job: " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job == null) {
            throw new GenieNotFoundException(
                    "No job with id " + id + " exists"
            );
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
        LOG.debug("Setting the id of process for job with id " + id + " to " + pid);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setProcessHandle(pid);
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists");
        }
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
        LOG.debug("Setting the command info for job with id " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            //TODO: Should we check if this is valid
            job.setCommandId(commandId);
            job.setCommandName(commandName);
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists");
        }
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
        LOG.debug("Setting the application info for job with id " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setApplicationId(appId);
            job.setApplicationName(appName);
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists");
        }
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
        LOG.debug("Setting the application info for job with id " + id);
        final Job job = this.jobRepo.findOne(id);
        if (job != null) {
            job.setExecutionClusterId(clusterId);
            job.setExecutionClusterName(clusterName);
        } else {
            throw new GenieNotFoundException("No job with id " + id + " exists");
        }
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
            final Job runningJob = this.jobRepo.findOne(job.getId());
            runningJob.setUpdated(new Date());
            return runningJob;
        } catch (final GenieException e) {
            LOG.error("Failed to run job: ", e);
            final Job failedJob = this.jobRepo.findOne(job.getId());
            // update db
            failedJob.setJobStatus(JobStatus.FAILED, e.getMessage());
            // increment counter for failed jobs
            this.stats.incrGenieFailedJobs();
            throw e;
        }
    }

    private String getEndPoint(final String hostName) throws GenieException {
        return "http://" + hostName + ":" + SERVER_PORT;
    }
}
