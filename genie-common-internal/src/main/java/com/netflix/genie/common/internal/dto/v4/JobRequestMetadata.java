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
package com.netflix.genie.common.internal.dto.v4;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.util.Optional;

/**
 * Metadata gathered by the system as part of any {@link JobRequest}.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(doNotUseGetters = true)
@EqualsAndHashCode(doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class JobRequestMetadata {

    private final ApiClientMetadata apiClientMetadata;
    private final AgentClientMetadata agentClientMetadata;
    @Min(0)
    private final int numAttachments;
    @Min(0)
    private final long totalSizeOfAttachments;

    /**
     * Constructor.
     *
     * @param apiClientMetadata      The metadata about the client if this request was received via API
     * @param agentClientMetadata    The metadata about the client if this request was received from the Agent
     * @param numAttachments         The number of attachments that came with this job request
     * @param totalSizeOfAttachments The total size of the attachments that came with this job request
     */
    public JobRequestMetadata(
        @Nullable final ApiClientMetadata apiClientMetadata,
        @Nullable final AgentClientMetadata agentClientMetadata,
        final int numAttachments,
        final long totalSizeOfAttachments
    ) {
        this.apiClientMetadata = apiClientMetadata;
        this.agentClientMetadata = agentClientMetadata;
        this.numAttachments = numAttachments >= 0 ? numAttachments : 0;
        this.totalSizeOfAttachments = totalSizeOfAttachments >= 0L ? totalSizeOfAttachments : 0L;
    }

    /**
     * If the job request was sent via API this field will be populated.
     *
     * @return The API client metadata wrapped in an {@link Optional}
     */
    public Optional<ApiClientMetadata> getApiClientMetadata() {
        return Optional.ofNullable(this.apiClientMetadata);
    }

    /**
     * If the job request was sent via the Agent this field will be populated.
     *
     * @return The Agent client metadata wrapped in an {@link Optional}
     */
    public Optional<AgentClientMetadata> getAgentClientMetadata() {
        return Optional.ofNullable(this.agentClientMetadata);
    }
}
