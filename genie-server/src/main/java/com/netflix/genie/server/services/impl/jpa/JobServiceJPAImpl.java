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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.data.domain.Sort.Direction;
import com.netflix.genie.common.model.Job_;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Implementation of the Job Service API's.
 *
 * @author tgianos
 */
@Named
public class JobServiceJPAImpl implements JobService {

    private static final Logger LOG = LoggerFactory
            .getLogger(JobServiceJPAImpl.class);

    // instance of the netflix configuration object
    private static final AbstractConfiguration CONF;

    // these can be over-ridden in the properties file
    private static final int SERVER_PORT;
    private static final String JOB_DIR_PREFIX;
    private static final String JOB_RESOURCE_PREFIX;

    // per-instance variables
    private final GenieNodeStatistics stats;
    private final JobRepository jobRepo;
    private final JobManagerFactory jobManagerFactory;

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
     * Constructor.
     *
     * @param jobRepo The job repository to use.
     * @param stats the GenieNodeStatistics object
     * @param jobManagerFactory The the job manager factory to use
     */
    @Inject
    public JobServiceJPAImpl(
            final JobRepository jobRepo,
            final GenieNodeStatistics stats,
            final JobManagerFactory jobManagerFactory) {
        this.jobRepo = jobRepo;
        this.stats = stats;
        this.jobManagerFactory = jobManagerFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public Job createJob(final Job job) throws GenieException {
        if (StringUtils.isNotEmpty(job.getId())
                && this.jobRepo.exists(job.getId())) {
            throw new GenieConflictException(
                    "A job with id " + job.getId() + " already exists. Unable to save."
            );
        }
        // validate parameters
        job.validate();
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
    public Job getJob(final String id) throws GenieException {
        LOG.debug("called for id: " + id);
        this.testId(id);

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
            final JobStatus status,
            final String clusterName,
            final String clusterId,
            final int page,
            final int limit) {
        LOG.debug("called");
        final PageRequest pageRequest = new PageRequest(
                page < 0 ? 0 : page,
                limit < 1 ? 1024 : limit,
                Direction.DESC,
                Job_.updated.getName()
        );

        @SuppressWarnings("unchecked")
        final List<Job> jobs = this.jobRepo.findAll(
                JobSpecs.find(
                        id,
                        jobName,
                        userName,
                        status,
                        clusterName,
                        clusterId),
                pageRequest).getContent();
        return jobs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(rollbackFor = GenieException.class)
    public Set<String> addTagsForJob(
            final String id,
            final Set<String> tags) throws GenieException {
        this.testId(id);
        if (tags == null) {
            throw new GeniePreconditionException("No tags entered.");
        }
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
    public Set<String> getTagsForJob(final String id) throws GenieException {
        this.testId(id);

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
    @Transactional(rollbackFor = GenieException.class)
    public Set<String> updateTagsForJob(
            final String id,
            final Set<String> tags) throws GenieException {
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public Set<String> removeAllTagsForJob(
            final String id) throws GenieException {
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public Set<String> removeTagForJob(final String id, final String tag)
            throws GenieException {
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public long setUpdateTime(final String id) throws GenieException {
        LOG.debug("Updating db for job: " + id);
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public void setJobStatus(
            final String id,
            final JobStatus status,
            final String msg) throws GenieException {
        LOG.debug("Setting job with id " + id + " to status " + status + " for reason " + msg);
        this.testId(id);
        if (status == null) {
            throw new GeniePreconditionException("No status entered.");
        }
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
    @Transactional(rollbackFor = GenieException.class)
    public void setProcessIdForJob(final String id, final int pid) throws GenieException {
        LOG.debug("Setting the id of process for job with id " + id + " to " + pid);
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public void setCommandInfoForJob(
            final String id,
            final String commandId,
            final String commandName) throws GenieException {
        LOG.debug("Setting the command info for job with id " + id);
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public void setApplicationInfoForJob(
            final String id,
            final String appId,
            final String appName) throws GenieException {
        LOG.debug("Setting the application info for job with id " + id);
        this.testId(id);
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
    @Transactional(rollbackFor = GenieException.class)
    public void setClusterInfoForJob(
            final String id,
            final String clusterId,
            final String clusterName) throws GenieException {
        LOG.debug("Setting the application info for job with id " + id);
        this.testId(id);
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
    @Transactional(readOnly = true)
    public JobStatus getJobStatus(final String id) throws GenieException {
        return getJob(id).getStatus();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public Job runJob(final Job job) throws GenieException {
        try {
            this.jobManagerFactory.getJobManager(job).launch();

            // update entity in DB
            // TODO This udpate runs into deadlock issue, either add manual retries
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

    private void testId(final String id) throws GenieException {
        if (StringUtils.isBlank(id)) {
            throw new GeniePreconditionException("No id entered.");
        }
    }
}
