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
package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.FetchingCacheService;
import com.netflix.genie.agent.execution.services.JobMonitorService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.execution.services.KillService;
import com.netflix.genie.agent.execution.statemachine.ExecutionContext;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.TaskScheduler;

import java.io.File;


class ServicesAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    ServicesAutoConfiguration.class
                )
            )
            .withUserConfiguration(ServicesAutoConfigurationTest.MocksConfiguration.class);

    @Test
    void executionContext() {
        contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(AgentProperties.class);
                Assertions.assertThat(context).hasSingleBean(DownloadService.class);
                Assertions.assertThat(context).hasSingleBean(FetchingCacheService.class);
                Assertions.assertThat(context).hasSingleBean(KillService.class);
                Assertions.assertThat(context).hasSingleBean(JobSetupService.class);
                Assertions.assertThat(context).hasSingleBean(JobMonitorService.class);

                // All beans are lazy, so above assertions will trivially pass.
                // Validate by forcing instantiation
                Assertions.assertThat(context).getBean(DownloadService.class).isNotNull();
                Assertions.assertThat(context).getBean(FetchingCacheService.class).isNotNull();
                Assertions.assertThat(context).getBean(KillService.class).isNotNull();
                Assertions.assertThat(context).getBean(JobSetupService.class).isNotNull();
                Assertions.assertThat(context).getBean(JobMonitorService.class).isNotNull();
            }
        );
    }

    @Configuration
    static class MocksConfiguration {

        @Bean
        ResourceLoader resourceLoader() {
            return Mockito.mock(ResourceLoader.class);
        }

        @Bean
        ArgumentDelegates.CacheArguments cacheArguments() {
            // Cannot use @TempDir here because static class initialization via annotation
            final File temp = Files.newTemporaryFolder();
            final ArgumentDelegates.CacheArguments mock = Mockito.mock(ArgumentDelegates.CacheArguments.class);
            Mockito.when(mock.getCacheDirectory()).thenReturn(temp);
            return mock;
        }

        @Bean
        FileLockFactory fileLockFactory() {
            return Mockito.mock(FileLockFactory.class);
        }

        @Bean
        JobDirectoryManifestCreatorService manifestCreatorService() {
            return Mockito.mock(JobDirectoryManifestCreatorService.class);
        }

        @Bean
        AgentJobService agentJobService() {
            return Mockito.mock(AgentJobService.class);
        }

        @Bean(name = "sharedAgentTaskExecutor")
        TaskExecutor taskExecutor() {
            return Mockito.mock(TaskExecutor.class);
        }

        @Bean(name = "sharedAgentTaskScheduler")
        TaskScheduler taskScheduler() {
            return Mockito.mock(TaskScheduler.class);
        }

        @Bean
        ExecutionContext executionContext() {
            return Mockito.mock(ExecutionContext.class);
        }
    }
}
