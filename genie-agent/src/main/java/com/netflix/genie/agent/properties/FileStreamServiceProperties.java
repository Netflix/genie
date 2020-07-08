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

import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.common.internal.properties.ExponentialBackOffTriggerProperties;
import com.netflix.genie.common.internal.util.ExponentialBackOffTrigger;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.time.Duration;

/**
 * Properties for {@link AgentFileStreamService}.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
@Validated
public class FileStreamServiceProperties {
    /**
     * Exponential backoff for errors in control stream.
     */
    @NotNull
    private ExponentialBackOffTriggerProperties errorBackOff = new ExponentialBackOffTriggerProperties(
        ExponentialBackOffTrigger.DelayType.FROM_PREVIOUS_EXECUTION_BEGIN,
        Duration.ofSeconds(1),
        Duration.ofSeconds(10),
        1.1f
    );

    /**
     * Enable compression of data.
     */
    private boolean enableCompression = true;

    /**
     * Maximum size of file chunk transmitted.
     */
    private DataSize dataChunkMaxSize = DataSize.ofMegabytes(1);

    /**
     * Maximum number of files transmitted concurrently.
     */
    @Min(1)
    private int maxConcurrentStreams = 5;

    /**
     * Time allowed to the service to complete ongoing transfers before shutting down.
     */
    @DurationMin(seconds = 1)
    private Duration drainTimeout = Duration.ofSeconds(15);
}
