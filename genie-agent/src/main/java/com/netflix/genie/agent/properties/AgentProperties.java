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
package com.netflix.genie.agent.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Root properties class for agent.
 *
 * @author mprimi
 * @since 4.0.0
 */
@ConfigurationProperties(prefix = AgentProperties.PREFIX)
@Getter
@Setter
public class AgentProperties {
    /**
     * Properties prefix.
     */
    public static final String PREFIX = "genie.agent.runtime";

    private Duration emergencyShutdownDelay = Duration.ofMinutes(5);
    private FileStreamServiceProperties fileStreamService = new FileStreamServiceProperties();
}
