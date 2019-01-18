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

package com.netflix.genie.web.util;

import com.netflix.genie.common.internal.dto.v4.AgentClientMetadata;
import com.netflix.genie.web.properties.AgentFilterProperties;
import com.netflix.genie.web.services.AgentFilterService;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import javax.validation.Valid;

/**
 * An {@link AgentFilterService.AgentMetadataInspector} that rejects agents whose version is older than a given version.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class MinimumVersionAgentMetadataInspector implements AgentFilterService.AgentMetadataInspector {

    private final AgentFilterProperties agentFilterProperties;

    /**
     * Constructor.
     *
     * @param agentFilterProperties agent filter properties
     */
    public MinimumVersionAgentMetadataInspector(final AgentFilterProperties agentFilterProperties) {
        this.agentFilterProperties = agentFilterProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AgentFilterService.InspectionReport inspect(@Valid final AgentClientMetadata agentClientMetadata) {

        final String minimumVersionString = this.agentFilterProperties.getMinimumVersion();
        if (StringUtils.isNotBlank(minimumVersionString)) {
            // Minimum version is set, check it
            final String agentVersionString = agentClientMetadata.getVersion().orElse(null);
            if (StringUtils.isNotBlank(agentVersionString)) {
                final ArtifactVersion minimumVersion = new DefaultArtifactVersion(minimumVersionString);
                final ArtifactVersion agentVersion = new DefaultArtifactVersion(agentVersionString);

                System.out.println(
                    "" + minimumVersion + " vs " + agentVersion + " -> " + minimumVersion.compareTo(agentVersion)
                );
                if (minimumVersion.compareTo(agentVersion) > 0) {
                    return new AgentFilterService.InspectionReport(
                        AgentFilterService.InspectionReport.InspectionDecision.REJECT,
                        "Agent version " + agentVersionString + " is lower than minimum " + minimumVersionString
                    );
                } else {
                    return new AgentFilterService.InspectionReport(
                        AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
                        "Agent version " + agentVersionString + " meets minimum " + minimumVersionString
                    );
                }

            } else {
                // Agent version not set, reject.
                return new AgentFilterService.InspectionReport(
                    AgentFilterService.InspectionReport.InspectionDecision.REJECT,
                    "Agent version not set, minimum is: " + minimumVersionString
                );
            }

        } else {
            // Minimum version not set, let this agent through.
            return new AgentFilterService.InspectionReport(
                AgentFilterService.InspectionReport.InspectionDecision.CONTINUE,
                "Minimum version not set"
            );
        }
    }
}
