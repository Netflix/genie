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

package com.netflix.genie.web.services.impl;

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieAgentRejectedException;
import com.netflix.genie.web.services.AgentFilterService;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;
import java.util.List;

/**
 * Implementation of {@link AgentFilterService} which delegates iterates through an ordered list of
 * {@link AgentMetadataInspector}. Giving them the chance to accept and reject agents based on their provided
 * metadata. This filter accepts by default if none of the inspectors rejected.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public class AgentFilterServiceImpl implements AgentFilterService {

    private final List<AgentMetadataInspector> agentMetadataInspectorList;

    /**
     * Constructor.
     *
     * @param agentMetadataInspectorList the list of inspectors to consult
     */
    public AgentFilterServiceImpl(
        final List<AgentMetadataInspector> agentMetadataInspectorList
    ) {
        this.agentMetadataInspectorList = agentMetadataInspectorList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acceptOrThrow(@Valid final AgentClientMetadata agentClientMetadata) throws GenieAgentRejectedException {
        for (final AgentMetadataInspector agentMetadataInspector : agentMetadataInspectorList) {
            final InspectionReport outcome = agentMetadataInspector.inspect(agentClientMetadata);
            final InspectionReport.InspectionDecision decision = outcome.getDecision();
            final String message = outcome.getMessage();

            log.info(
                "Inspector: {} inspected: {}, decision: {} ({})",
                agentMetadataInspector,
                agentClientMetadata,
                decision.name(),
                message
            );

            if (decision == AgentFilterService.InspectionReport.InspectionDecision.REJECT) {
                throw new GenieAgentRejectedException("Agent rejected, " + message);
            } else if (decision == AgentFilterService.InspectionReport.InspectionDecision.ACCEPT) {
                // Bypass the rest of the inspectors.
                return;
            }

            // CONTINUE to next inspector
        }

        // Default to ACCEPT.
    }
}
