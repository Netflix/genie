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
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jpa.entities.JobEntity;
import com.netflix.genie.core.jpa.entities.JobExecutionEntity;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.specifications.JpaJobSpecs;
import com.netflix.genie.core.services.JobSearchService;
import org.hibernate.validator.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.Set;

/**
 * Jpa implementation of the Job Search Service.
 *
 * @author amsharma
 */
@Service
public class JpaJobSearchServiceImpl implements JobSearchService {

    private static final Logger LOG = LoggerFactory.getLogger(JpaJobSearchServiceImpl.class);
    private final JpaJobRepository jobRepository;
    private final JpaJobExecutionRepository jobExecutionRepository;

    /**
     * Constructor.
     *
     * @param jobRepository          Repository to use for jobs.
     * @param jobExecutionRepository The repository to use for job execution objects
     */
    @Autowired
    public JpaJobSearchServiceImpl(
        final JpaJobRepository jobRepository,
        final JpaJobExecutionRepository jobExecutionRepository
    ) {
        this.jobRepository = jobRepository;
        this.jobExecutionRepository = jobExecutionRepository;
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
        @NotNull final Pageable page
    ) {
        LOG.debug("called");

        @SuppressWarnings("unchecked")
        final Page<JobEntity> jobEntities = this.jobRepository.findAll(
            JpaJobSpecs.find(
                id,
                jobName,
                userName,
                statuses,
                tags,
                clusterName,
                clusterId,
                commandName,
                commandId
            ),
            page
        );
        return jobEntities.map(JobEntity::getDTO);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public Set<JobExecution> getAllJobExecutionsOnHost(@NotBlank final String hostname) throws GenieException {
        final Set<JobExecutionEntity> entities
            = this.jobExecutionRepository.findByHostNameAndExitCode(hostname, JobExecutionEntity.DEFAULT_EXIT_CODE);

        final Set<JobExecution> executions = new HashSet<>();
        for (final JobExecutionEntity entity : entities) {
            executions.add(entity.getDTO());
        }
        return executions;
    }
}
