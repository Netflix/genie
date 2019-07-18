/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.tasks.node;

import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.properties.DiskCleanupProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.tasks.node.DiskCleanupTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.TaskScheduler;

import java.io.IOException;

/**
 * Auto configuration for tasks that run on every Genie server node.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        DiskCleanupProperties.class,
    }
)
public class NodeAutoConfiguration {

    /**
     * If required get a {@link DiskCleanupTask} instance for use.
     *
     * @param properties       The disk cleanup properties to use.
     * @param scheduler        The scheduler to use to schedule the cron trigger.
     * @param jobsDir          The resource representing the location of the job directory
     * @param jobSearchService The service to find jobs with
     * @param jobsProperties   The jobs properties to use
     * @param processExecutor  The process executor to use to delete directories
     * @param registry         The metrics registry
     * @return The {@link DiskCleanupTask} instance
     * @throws IOException When it is unable to open a file reference to the job directory
     */
    @Bean
    @ConditionalOnProperty(value = DiskCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(DiskCleanupTask.class)
    public DiskCleanupTask diskCleanupTask(
        final DiskCleanupProperties properties,
        @Qualifier("genieTaskScheduler") final TaskScheduler scheduler,
        @Qualifier("jobsDir") final Resource jobsDir,
        final JobSearchService jobSearchService,
        final JobsProperties jobsProperties,
        final Executor processExecutor,
        final MeterRegistry registry
    ) throws IOException {
        return new DiskCleanupTask(
            properties,
            scheduler,
            jobsDir,
            jobSearchService,
            jobsProperties,
            processExecutor,
            registry
        );
    }
}
