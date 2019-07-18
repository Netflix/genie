/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.spring.autoconfigure.tasks;

import com.netflix.genie.web.properties.TasksExecutorPoolProperties;
import com.netflix.genie.web.properties.TasksSchedulerPoolProperties;
import org.apache.commons.exec.Executor;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Unit tests for {@link TasksAutoConfiguration} class.
 *
 * @author tgianos
 * @since 3.0.0
 */
public class TasksAutoConfigurationTest {

    private ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    TasksAutoConfiguration.class
                )
            );

    /**
     * All the expected beans exist.
     */
    @Test
    public void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(TasksExecutorPoolProperties.class);
                Assertions.assertThat(context).hasSingleBean(TasksSchedulerPoolProperties.class);

                Assertions.assertThat(context).hasSingleBean(Executor.class);

                Assertions.assertThat(context).hasBean("genieTaskScheduler");
                Assertions.assertThat(context).hasBean("genieAsyncTaskExecutor");
                Assertions.assertThat(context).hasBean("genieSyncTaskExecutor");
            }
        );
    }
}
