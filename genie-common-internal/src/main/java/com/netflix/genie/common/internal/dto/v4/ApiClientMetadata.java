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
 * Data class to hold information about a Genie client whether it is an API client or an Agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(callSuper = true, doNotUseGetters = true)
@EqualsAndHashCode(callSuper = true, doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class ApiClientMetadata extends ClientMetadata {
    private final String userAgent;

    /**
     * Constructor.
     *
     * @param hostname  The hostname of the client that sent the API request
     * @param userAgent The user agent string
     */
    public ApiClientMetadata(@Nullable final String hostname, @Nullable final String userAgent) {
        super(hostname);
        this.userAgent = userAgent;
    }

    /**
     * Get the user agent string for the client.
     *
     * @return The user agent string if there is one wrapped in an {@link Optional}
     */
    public Optional<String> getUserAgent() {
        return Optional.ofNullable(this.userAgent);
    }
}
