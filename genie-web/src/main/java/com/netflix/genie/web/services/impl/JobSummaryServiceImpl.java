/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.services.impl;


import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.dtos.CompletedJobSummary;
import com.netflix.genie.web.services.JobSummaryService;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Produces job summaries by collating information from various underlying data sources.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class JobSummaryServiceImpl implements JobSummaryService {
    private final JobPersistenceService jobPersistenceService;
    private final ClusterPersistenceService clusterPersistenceService;
    private final CommandPersistenceService commandPersistenceService;
    private final ApplicationPersistenceService applicationPersistenceService;

    public JobSummaryServiceImpl(
        final JobPersistenceService jobPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        final ApplicationPersistenceService applicationPersistenceService
    ) {
        this.jobPersistenceService = jobPersistenceService;
        this.clusterPersistenceService = clusterPersistenceService;
        this.commandPersistenceService = commandPersistenceService;
        this.applicationPersistenceService = applicationPersistenceService;
    }

    @Override
    public CompletedJobSummary getJobSummary(@NotBlank final String id) throws GenieException {
        final JobStatus jobFinalStatus = jobPersistenceService.getJobStatus(id);

        if (jobFinalStatus.isFinished()) {
            throw new GeniePreconditionException("Job " + id + " is not finished");
        }

        final JobRequest jobRequest = jobPersistenceService.getJobRequest(id)
            .orElseThrow(() -> new GeniePreconditionException("Job request not found for job: " + id));

        final Optional<JobSpecification> jobSpecificationOptional = jobPersistenceService.getJobSpecification(id);

        final CompletedJobSummary.Builder jobSummaryBuilder = new CompletedJobSummary.Builder(
            jobFinalStatus,
            jobRequest
        );

        if (jobSpecificationOptional.isPresent()) {
            final JobSpecification jobSpecification = jobSpecificationOptional.get();

            jobSummaryBuilder.withSpecification(jobSpecification);

            final String clusterId = jobSpecification.getCluster().getId();
            try {
                jobSummaryBuilder.withCluster(
                    this.clusterPersistenceService.getCluster(clusterId)
                );
            } catch (GenieNotFoundException e) {
                log.warn("Cluster not found: {}", clusterId);
            }

            final String commandId = jobSpecification.getCommand().getId();
            try {
                jobSummaryBuilder.withCommand(
                    this.commandPersistenceService.getCommand(commandId)
                );
            } catch (GenieNotFoundException e) {
                log.warn("Command not found: {}", commandId);
            }

            final List<JobSpecification.ExecutionResource> applicationResources = jobSpecification.getApplications();
            final Set<Application> applications = Sets.newHashSet();

            for (final JobSpecification.ExecutionResource applicationResource : applicationResources) {
                final String applicationId = applicationResource.getId();
                try {
                    applications.add(
                        this.applicationPersistenceService.getApplication(
                            applicationResource.getId()
                        )
                    );
                } catch (GenieNotFoundException e) {
                    log.warn("Application not found: {}", applicationId);
                }
            }

            jobSummaryBuilder.withApplications(applications);
        }
        
        return jobSummaryBuilder.build();
    }
}
