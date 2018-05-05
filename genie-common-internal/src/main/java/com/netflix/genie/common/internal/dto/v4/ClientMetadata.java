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
import javax.validation.constraints.Size;
import java.util.Optional;

/**
 * Common base for API and Agent client metadata.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(doNotUseGetters = true)
@EqualsAndHashCode(doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class ClientMetadata {

    @Size(max = 255, message = "The client hostname can't be longer than 255 characters")
    private final String hostname;

    /**
     * Constructor.
     *
     * @param hostname The hostname of the client to the Genie server
     */
    public ClientMetadata(@Nullable final String hostname) {
        this.hostname = hostname;
    }

    /**
     * Get the hostname of the client.
     *
     * @return The hostname if there is one wrapped in an {@link Optional}
     */
    public Optional<String> getHostname() {
        return Optional.ofNullable(this.hostname);
    }
}
