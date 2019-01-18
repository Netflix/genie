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
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.artifact.versioning.ArtifactVersion;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import javax.validation.Valid;

/**
 * An {@link AgentMetadataInspector} that rejects agents whose version is older than a given version.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class MinimumVersionAgentMetadataInspector implements AgentMetadataInspector {

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
    public InspectionReport inspect(@Valid final AgentClientMetadata agentClientMetadata) {

        final String minimumVersionString = this.agentFilterProperties.getMinimumVersion();
        final String agentVersionString = agentClientMetadata.getVersion().orElse(null);

        if (StringUtils.isBlank(minimumVersionString)) {
            return InspectionReport.newAcceptance("Minimum version requirement not set");

        } else if (StringUtils.isBlank(agentVersionString)) {
            return InspectionReport.newRejection("Agent version not set");

        } else {
            final ArtifactVersion minimumVersion = new DefaultArtifactVersion(minimumVersionString);
            final ArtifactVersion agentVersion = new DefaultArtifactVersion(agentVersionString);

            final boolean deprecated = minimumVersion.compareTo(agentVersion) > 0;

            return new InspectionReport(
                deprecated ? InspectionReport.Decision.REJECT : InspectionReport.Decision.ACCEPT,
                String.format(
                    "Agent version: %s is %s than minimum: %s",
                    agentVersionString,
                    deprecated ? "older" : "newer or equal",
                    minimumVersionString
                )
            );
        }
    }
}
