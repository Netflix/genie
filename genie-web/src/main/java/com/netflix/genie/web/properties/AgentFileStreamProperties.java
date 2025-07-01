/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Properties related to {@link com.netflix.genie.web.agent.services.AgentFileStreamService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = AgentFileStreamProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class AgentFileStreamProperties {

    /**
     * Prefix for all properties related to the agent file transfer service.
     */
    public static final String PROPERTY_PREFIX = "genie.agent.filestream";

    /**
     * How many active transfer to allow.
     */
    private int maxConcurrentTransfers = 100;

    /**
     * How long to wait for the first chunk of data to be received before timing out.
     */
    private Duration unclaimedStreamStartTimeout = Duration.ofSeconds(10);

    /**
     * Time allowed to a transfer to make progress before it's marked as stalled and terminated.
     */
    private Duration stalledTransferTimeout = Duration.ofSeconds(20);

    /**
     * How often to check on transfer in progress (and terminate the ones that did not make progress).
     */
    private Duration stalledTransferCheckInterval = Duration.ofSeconds(5);

    /**
     * How long to wait before retrying to append a chunk of data into a stream buffer.
     */
    private Duration writeRetryDelay = Duration.ofMillis(300);

    /**
     * How long to store a manifest before considering it stale and evicting it.
     */
    private Duration manifestCacheExpiration = Duration.ofSeconds(30);

}
