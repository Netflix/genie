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
 * Metadata for a Genie Agent client.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class AgentClientMetadata extends ClientMetadata {

    private final String version;
    private final Integer pid;

    /**
     * Constructor.
     *
     * @param hostname The hostname of the computer the agent is running on
     * @param version  The version of the agent that sent the request
     * @param pid      The PID of the agent that sent the request
     */
    public AgentClientMetadata(
        @Nullable final String hostname,
        @Nullable final String version,
        @Nullable final Integer pid
    ) {
        super(hostname);
        this.version = version;
        this.pid = pid;
    }

    /**
     * Get the running version of the agent if it was set.
     *
     * @return The version wrapped in an {@link Optional}
     */
    public Optional<String> getVersion() {
        return Optional.ofNullable(this.version);
    }

    /**
     * Get the process id of the Agent on the host it is running on.
     *
     * @return The pid if it was set wrapped in an {@link Optional}
     */
    public Optional<Integer> getPid() {
        return Optional.ofNullable(this.pid);
    }
}
