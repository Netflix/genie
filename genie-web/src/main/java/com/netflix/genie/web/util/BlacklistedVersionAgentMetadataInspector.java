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

import javax.validation.Valid;

/**
 * An {@link AgentFilterService.AgentMetadataInspector} that rejects agent whose version matches a regular expression
 * obtained via properties.
 * Non-blacklisted versions are allowed to proceed through the rest of the filters.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class BlacklistedVersionAgentMetadataInspector extends BaseRegexAgentMetadataInspector {

    private final AgentFilterProperties agentFilterProperties;

    /**
     * Constructor.
     * @param agentFilterProperties version filter properties
     */
    public BlacklistedVersionAgentMetadataInspector(final AgentFilterProperties agentFilterProperties) {
        super(AgentFilterService.InspectionReport.InspectionDecision.REJECT);
        this.agentFilterProperties = agentFilterProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AgentFilterService.InspectionReport inspect(@Valid final AgentClientMetadata agentClientMetadata) {
        return super.inspectWithPattern(
            agentFilterProperties.getBlacklistedVersions(),
            agentClientMetadata.getVersion().orElse(null)
        );
    }
}
