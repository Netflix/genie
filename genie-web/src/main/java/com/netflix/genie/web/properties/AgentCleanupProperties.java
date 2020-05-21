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
package com.netflix.genie.web.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Properties related to cleaning up jobs associated to AWOL/MIA agents.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = AgentCleanupProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class AgentCleanupProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.tasks.agent-cleanup";

    /**
     * The property that determines if the {@link com.netflix.genie.web.tasks.leader.AgentJobCleanupTask} is enabled.
     */
    public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

    private boolean enabled = true;

    private Duration refreshInterval = Duration.ofSeconds(10);

    private Duration reconnectTimeLimit = Duration.ofMinutes(2);

    private Duration launchTimeLimit = Duration.ofMinutes(4);
}
