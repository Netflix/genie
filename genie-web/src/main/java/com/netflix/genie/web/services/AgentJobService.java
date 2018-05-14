/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieApplicationNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieCommandNotFoundException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieIdAlreadyExistsException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * A Service to collect the logic for implementing calls from the Agent when a job is launched via the CLI.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Validated
public interface AgentJobService {

    /**
     * Reserve a job id and persist job details in the database based on the supplied {@link JobRequest}.
     *
     * @param jobRequest          The job request containing all the metadata needed to reserve a job id
     * @param agentClientMetadata The metadata about the agent driving this job request
     * @return The unique id of the job which was saved in the database
     * @throws GenieIdAlreadyExistsException If the id requested along with the job request is already in use
     */
    String reserveJobId(@Valid final JobRequest jobRequest, @Valid final AgentClientMetadata agentClientMetadata);

    /**
     * Resolve the job specification for job identified by the given id. This method will persist the job specification
     * details to the database.
     *
     * @param id The id of the job to resolve the specification for. Must already have a reserved an id in the database
     * @return The job specification if one could be resolved
     * @throws GenieJobNotFoundException         If the job has not yet had its ID reserved and/or can't be found
     * @throws GenieClusterNotFoundException     When the cluster specified in the job specification doesn't actually
     *                                           exist
     * @throws GenieCommandNotFoundException     When the command specified in the job specification doesn't actually
     *                                           exist
     * @throws GenieApplicationNotFoundException When an application specified in the job specification doesn't
     *                                           actually exist
     */
    JobSpecification resolveJobSpecification(final String id);
}
