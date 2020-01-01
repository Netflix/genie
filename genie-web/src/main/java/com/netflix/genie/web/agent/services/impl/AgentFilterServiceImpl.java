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
package com.netflix.genie.web.agent.services.impl;

import com.netflix.genie.common.external.dtos.v4.AgentClientMetadata;
import com.netflix.genie.web.agent.inspectors.AgentMetadataInspector;
import com.netflix.genie.web.agent.inspectors.InspectionReport;
import com.netflix.genie.web.agent.services.AgentFilterService;
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
    public AgentFilterServiceImpl(final List<AgentMetadataInspector> agentMetadataInspectorList) {
        this.agentMetadataInspectorList = agentMetadataInspectorList;
    }

    /**
     * Inspect agent metadata and decide to accept or reject it.
     * This implementation iterates over the given set of {@link AgentMetadataInspector} and stops at the first one
     * that rejects. If none rejects, then the client is accepted.
     *
     * @param agentClientMetadata the agent client metadata
     * @return an inspection report
     */
    @Override
    public InspectionReport inspectAgentMetadata(@Valid final AgentClientMetadata agentClientMetadata) {
        for (final AgentMetadataInspector agentMetadataInspector : agentMetadataInspectorList) {
            final InspectionReport inspectionReport = agentMetadataInspector.inspect(agentClientMetadata);
            final InspectionReport.Decision decision = inspectionReport.getDecision();
            final String message = inspectionReport.getMessage();

            log.debug(
                "Inspector: {} inspected: {}, decision: {} ({})",
                agentMetadataInspector.getClass().getSimpleName(),
                agentClientMetadata,
                decision.name(),
                message
            );

            if (decision == InspectionReport.Decision.REJECT) {
                return InspectionReport.newRejection(
                    "Rejected by: "
                        + agentMetadataInspector.getClass().getSimpleName()
                        + ": "
                        + message
                );
            }
        }

        return InspectionReport.newAcceptance("All inspections passed");
    }
}
