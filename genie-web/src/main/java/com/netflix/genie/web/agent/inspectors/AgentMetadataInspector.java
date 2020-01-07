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
package com.netflix.genie.web.agent.inspectors;

import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * Component that inspects an Agent client metadata and makes decision on whether it is allowed to proceed.
 */
@Validated
public interface AgentMetadataInspector {
    /**
     * Perform inspection of an Agent client metadata.
     *
     * @param agentClientMetadata the agent client metadata
     * @return the inspection outcome
     */
    InspectionReport inspect(@Valid AgentClientMetadata agentClientMetadata);
}
