/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.agent.spring.autoconfigure;

import com.netflix.genie.agent.AgentMetadataImpl;
import com.netflix.genie.agent.properties.AgentProperties;
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.task.TaskExecutorCustomizer;
import org.springframework.boot.task.TaskSchedulerCustomizer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Tests for {@link AgentAutoConfiguration}.
 *
 * @author standon
 * @since 4.0.0
 */
class AgentAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                AgentAutoConfiguration.class
            )
        );

    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(GenieHostInfo.class);
                Assertions.assertThat(context).hasSingleBean(AgentMetadataImpl.class);
                Assertions.assertThat(context).hasSingleBean(FileLockFactory.class);
                Assertions
                    .assertThat(context)
                    .getBean("sharedAgentTaskExecutor")
                    .isOfAnyClassIn(ThreadPoolTaskExecutor.class);
                Assertions.assertThat(context).hasBean("sharedAgentTaskScheduler");
                Assertions
                    .assertThat(context)
                    .getBean("sharedAgentTaskScheduler")
                    .isOfAnyClassIn(ThreadPoolTaskScheduler.class);
                Assertions.assertThat(context).hasBean("heartBeatServiceTaskScheduler");
                Assertions
                    .assertThat(context)
                    .getBean("heartBeatServiceTaskScheduler")
                    .isOfAnyClassIn(ThreadPoolTaskScheduler.class);
                Assertions.assertThat(context).hasSingleBean(AgentProperties.class);
                Assertions.assertThat(context).hasSingleBean(TaskExecutorCustomizer.class);
                Assertions.assertThat(context).hasSingleBean(TaskSchedulerCustomizer.class);
            }
        );
    }

    @Test
    void testTaskExecutorCustomizer() {
        final AgentProperties properties = new AgentProperties();
        final TaskExecutorCustomizer customizer = new AgentAutoConfiguration().taskExecutorCustomizer(properties);
        final ThreadPoolTaskExecutor taskExecutor = Mockito.mock(ThreadPoolTaskExecutor.class);
        customizer.customize(taskExecutor);
        Mockito.verify(taskExecutor).setWaitForTasksToCompleteOnShutdown(true);
        Mockito.verify(taskExecutor).setAwaitTerminationSeconds(60);
    }

    @Test
    void testTaskSchedulerCustomizer() {
        final AgentProperties properties = new AgentProperties();
        final TaskSchedulerCustomizer customizer = new AgentAutoConfiguration().taskSchedulerCustomizer(properties);
        final ThreadPoolTaskScheduler taskScheduler = Mockito.mock(ThreadPoolTaskScheduler.class);
        customizer.customize(taskScheduler);
        Mockito.verify(taskScheduler).setWaitForTasksToCompleteOnShutdown(true);
        Mockito.verify(taskScheduler).setAwaitTerminationSeconds(60);
    }
}
