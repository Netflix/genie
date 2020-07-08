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
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Properties to configure the Agent shutdown process.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
@Validated
public class ShutdownProperties {
    /**
     * Time allowed to the job state machine to complete after the job has been killed.
     */
    @DurationMin(seconds = 1)
    private Duration executionCompletionLeeway = Duration.ofSeconds(60);

    /**
     * Time allowed to internal task executors to complete running tasks before shutting them down.
     */
    @DurationMin(seconds = 1)
    private Duration internalExecutorsLeeway = Duration.ofSeconds(30);

    /**
     * Time allowed to internal task schedulers to complete running tasks before shutting them down.
     */
    @DurationMin(seconds = 1)
    private Duration internalSchedulersLeeway = Duration.ofSeconds(30);

    /**
     * Time allowed to Spring task executor to complete running tasks before shutting them down.
     */
    @DurationMin(seconds = 1)
    private Duration systemExecutorLeeway = Duration.ofSeconds(60);

    /**
     * Time allowed to Spring task scheduler to complete running tasks before shutting them down.
     */
    @DurationMin(seconds = 1)
    private Duration systemSchedulerLeeway = Duration.ofSeconds(60);
}
