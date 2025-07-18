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
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/**
 * Properties for {@link com.netflix.genie.web.agent.services.AgentConnectionTrackingService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = AgentConnectionTrackingServiceProperties.PREFIX)
@Getter
@Setter
@Validated
public class AgentConnectionTrackingServiceProperties {
    /**
     * Properties prefix.
     */
    static final String PREFIX = "genie.agent.connection-tracking";

    @NotNull
    @DurationMin(seconds = 1)
    private Duration cleanupInterval = Duration.ofSeconds(2);

    @NotNull
    @DurationMin(seconds = 1)
    private Duration connectionExpirationPeriod = Duration.ofSeconds(10);
}
