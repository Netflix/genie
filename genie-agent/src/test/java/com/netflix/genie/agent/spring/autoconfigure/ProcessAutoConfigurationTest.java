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
package com.netflix.genie.agent.spring.autoconfigure;

import brave.Tracer;
import com.netflix.genie.agent.execution.process.JobProcessManager;
import com.netflix.genie.agent.execution.process.impl.JobProcessManagerImpl;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

/**
 * Tests for {@link ProcessAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class ProcessAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                AgentAutoConfiguration.class,
                ProcessAutoConfiguration.class
            )
        )
        .withUserConfiguration(ExternalBeans.class);

    @Test
    void expectedBeansExist() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(JobProcessManagerImpl.class);
                Assertions.assertThat(context).hasSingleBean(JobProcessManager.class);
            }
        );
    }

    private static class ExternalBeans {
        @Bean
        BraveTracingComponents tracingComponents() {
            return new BraveTracingComponents(
                Mockito.mock(Tracer.class),
                Mockito.mock(BraveTracePropagator.class),
                Mockito.mock(BraveTracingCleanup.class),
                Mockito.mock(BraveTagAdapter.class)
            );
        }
    }
}
