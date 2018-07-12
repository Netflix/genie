/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import javax.validation.Valid;

/**
 * All properties related to jobs in Genie.
 *
 * @author tgianos
 * @since 3.0.0
 */
@ConfigurationProperties(prefix = JobsProperties.PROPERTY_PREFIX)
@Getter
@Setter
@Validated
public class JobsProperties {

    /**
     * The property prefix for all properties in this group.
     */
    public static final String PROPERTY_PREFIX = "genie.jobs";

    @Valid
    private JobsCleanupProperties cleanup = new JobsCleanupProperties();

    @Valid
    private JobsForwardingProperties forwarding = new JobsForwardingProperties();

    @Valid
    private JobsLocationsProperties locations = new JobsLocationsProperties();

    @Valid
    private JobsMaxProperties max = new JobsMaxProperties();

    @Valid
    private JobsMemoryProperties memory = new JobsMemoryProperties();

    @Valid
    private JobsUsersProperties users = new JobsUsersProperties();

    @Valid
    private ExponentialBackOffTriggerProperties completionCheckBackOff = new ExponentialBackOffTriggerProperties();
}
