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
package com.netflix.genie.core.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

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
    @NotNull
    private JobsCleanupProperties cleanup = new JobsCleanupProperties();

    @NotNull
    private JobsForwardingProperties forwarding = new JobsForwardingProperties();

    @NotNull
    private JobsLocationsProperties locations = new JobsLocationsProperties();

    @NotNull
    private JobsMaxProperties max = new JobsMaxProperties();

    @NotNull
    private JobsMemoryProperties memory = new JobsMemoryProperties();

    @NotNull
    private JobsUsersProperties users = new JobsUsersProperties();

    @NotNull
    private ExponentialBackOffTriggerProperties completionCheckBackOff = new ExponentialBackOffTriggerProperties();
}
