/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.jobs;

import com.netflix.genie.web.scripts.ExecutionModeFilterScript;
import com.netflix.genie.web.util.JobExecutionModeSelector;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Optional;

/**
 * Configuration for Jobs Setup and Run.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
// TODO: This is going to go away once the V4 API is in place
public class JobsAutoConfiguration {

    /**
     * Create a {@link JobExecutionModeSelector} if one does not exist.
     *
     * @param environment               The environment
     * @param meterRegistry             The metrics registry to use
     * @param executionModeFilterScript The filter script (if one is loaded)
     * @return a {@link JobExecutionModeSelector}
     */
    @Bean
    @ConditionalOnMissingBean(JobExecutionModeSelector.class)
    public JobExecutionModeSelector jobExecutionModeSelector(
        final Environment environment,
        final MeterRegistry meterRegistry,
        final Optional<ExecutionModeFilterScript> executionModeFilterScript
    ) {
        return new JobExecutionModeSelector(
            environment,
            meterRegistry,
            executionModeFilterScript.orElse(null)
        );
    }
}
