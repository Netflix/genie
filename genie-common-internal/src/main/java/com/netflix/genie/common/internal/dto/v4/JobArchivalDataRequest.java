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
import java.util.Optional;

/**
 * Job archival options for the Genie agent.
 *
 * @author standon
 * @since 4.0.0
 */
@Getter
@ToString(doNotUseGetters = true)
@EqualsAndHashCode(doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class JobArchivalDataRequest {
    private final String requestedArchiveLocationPrefix;

    private JobArchivalDataRequest(final Builder builder) {
        this.requestedArchiveLocationPrefix = builder.bRequestedArchiveLocationPrefix;
    }

    /**
     * Get the prefix for the uri where the Agent should archive the job directory after the job finishes.
     *
     * @return Archive location prefix uri wrapped in an {@link Optional}
     */
    public Optional<String> getRequestedArchiveLocationPrefix() {
        return Optional.ofNullable(this.requestedArchiveLocationPrefix);
    }

    /**
     * Builder to create an immutable {@link JobArchivalDataRequest} instance.
     *
     * @author standon
     * @since 4.0.0
     */
    public static class Builder {
        private String bRequestedArchiveLocationPrefix;

        /**
         * URI prefix of the location used by the agent for archiving the job folder.
         *
         * @param requestedArchiveLocationPrefix URI prefix of the location used by the agent
         *                                       for archiving the job folder
         * @return The builder
         */
        public Builder withRequestedArchiveLocationPrefix(@Nullable final String requestedArchiveLocationPrefix) {
            this.bRequestedArchiveLocationPrefix = requestedArchiveLocationPrefix;
            return this;
        }

        /**
         * Build a new immutable instance of an {@link JobArchivalDataRequest}.
         *
         * @return An instance containing the fields set in this builder
         */
        public JobArchivalDataRequest build() {
            return new JobArchivalDataRequest(this);
        }
    }
}
