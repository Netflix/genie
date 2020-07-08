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

import java.time.Duration;

/**
 * Properties to configure the Agent shutdown process.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Getter
@Setter
public class ShutdownProperties {
    private Duration executionCompletionLeeway = Duration.ofSeconds(60);
    private Duration internalExecutorsLeeway = Duration.ofSeconds(30);
    private Duration internalSchedulersLeeway = Duration.ofSeconds(30);
    private Duration systemExecutorLeeway = Duration.ofSeconds(60);
    private Duration systemSchedulerLeeway = Duration.ofSeconds(60);
}
