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
import com.netflix.genie.agent.utils.locks.impl.FileLockFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Tests for {@link AgentAutoConfiguration}.
 *
 * @author standon
 * @since 4.0.0
 */
public class AgentAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                AgentAutoConfiguration.class
            )
        );

    /**
     * Make sure all the expected beans exist.
     */
    @Test
    public void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(AgentMetadataImpl.class);
                Assertions.assertThat(context).hasSingleBean(FileLockFactory.class);
                Assertions.assertThat(context).hasSingleBean(AsyncTaskExecutor.class);
                Assertions.assertThat(context).hasBean("sharedAgentTaskExecutor");
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
            }
        );
    }
}
