/*
 *
 *  Copyright 2026 Netflix, Inc.
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

/** Properties for the leader scheduler heartbeat task. */
@ConfigurationProperties(prefix = LeaderSchedulerHeartbeatProperties.PROPERTY_PREFIX)
@Validated
@Getter
@Setter
public class LeaderSchedulerHeartbeatProperties {

    /** The property prefix for this group. */
    public static final String PROPERTY_PREFIX = "genie.tasks.leader-task-heartbeat";

    /** The property that controls whether the heartbeat task is registered. */
    public static final String ENABLED_PROPERTY = PROPERTY_PREFIX + ".enabled";

    /** Whether the heartbeat task is enabled. */
    private boolean enabled = true;
}
