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
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;

/**
 * All properties related to jobs in Genie.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@Setter
@Validated
public class JobsProperties {

    @Valid
    private JobsForwardingProperties forwarding;
    @Valid
    private JobsLocationsProperties locations;
    @Valid
    private JobsMemoryProperties memory;
    @Valid
    private JobsUsersProperties users;
    @Valid
    private JobsActiveLimitProperties activeLimit;

    /**
     * Constructor.
     *
     * @param forwarding  forwarding properties
     * @param locations   locations properties
     * @param memory      memory properties
     * @param users       users properties
     * @param activeLimit active limit properties
     */
    public JobsProperties(
        @Valid final JobsForwardingProperties forwarding,
        @Valid final JobsLocationsProperties locations,
        @Valid final JobsMemoryProperties memory,
        @Valid final JobsUsersProperties users,
        @Valid final JobsActiveLimitProperties activeLimit
    ) {
        this.forwarding = forwarding;
        this.locations = locations;
        this.memory = memory;
        this.users = users;
        this.activeLimit = activeLimit;
    }

    /**
     * Create a JobsProperties initialized with default values (for use in tests).
     *
     * @return a new {@link JobsProperties} instance.
     */
    public static JobsProperties getJobsPropertiesDefaults() {
        return new JobsProperties(
            new JobsForwardingProperties(),
            new JobsLocationsProperties(),
            new JobsMemoryProperties(),
            new JobsUsersProperties(),
            new JobsActiveLimitProperties()
        );
    }
}
