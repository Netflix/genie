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

package com.netflix.genie.web.services;

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.web.util.InspectionReport;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * Service to block agent/clients that the server wants to refuse service to.
 * For example, blacklist clients running a deprecated or incompatible versions.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
public interface AgentFilterService {

    /**
     * Inspect the Agent metadata and decide whether to allow this client to proceed.
     *
     * @param agentClientMetadata the agent client metadata
     * @return an inspection report
     */
    InspectionReport inspectAgentMetadata(@Valid AgentClientMetadata agentClientMetadata);

}
