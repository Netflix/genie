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

import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.scripts.ExecutionModeFilterScript;
import com.netflix.genie.web.util.JobExecutionModeSelector;
import com.netflix.genie.web.util.ProcessChecker;
import com.netflix.genie.web.util.UnixProcessChecker;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.beans.factory.BeanCreationException;
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
     * Create a {@link ProcessChecker.Factory} suitable for UNIX systems.
     *
     * @param executor       The executor where checks are executed
     * @param jobsProperties The jobs properties
     * @return a {@link ProcessChecker.Factory}
     */
    @Bean
    @ConditionalOnMissingBean(ProcessChecker.Factory.class)
    public ProcessChecker.Factory processCheckerFactory(
        final Executor executor,
        final JobsProperties jobsProperties
    ) {
        if (SystemUtils.IS_OS_UNIX) {
            return new UnixProcessChecker.Factory(
                executor,
                jobsProperties.getUsers().isRunAsUserEnabled()
            );
        } else {
            throw new BeanCreationException("No implementation available for non-UNIX systems");
        }
    }

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
