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
package com.netflix.genie.web.spring.autoconfigure.agent.launchers;

import brave.Tracer;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.agent.launchers.impl.LocalAgentLauncherImpl;
import com.netflix.genie.web.agent.launchers.impl.TitusAgentLauncherImpl;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.properties.TitusAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;

import java.util.UUID;

/**
 * Tests for {@link AgentLaunchersAutoConfiguration}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class AgentLaunchersAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    AgentLaunchersAutoConfiguration.class
                )
            )
            .withUserConfiguration(UserConfig.class);

    /**
     * All the expected beans should exist when the auto configuration is applied.
     */
    @Test
    void testExpectedBeansExist() {
        this.contextRunner
            .withPropertyValues(
                "genie.agent.launcher.local.enabled=true"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(LocalAgentLauncherProperties.class);
                    Assertions.assertThat(context).hasSingleBean(TitusAgentLauncherProperties.class);
                    Assertions.assertThat(context).hasSingleBean(ExecutorFactory.class);
                    Assertions.assertThat(context).hasSingleBean(LocalAgentLauncherImpl.class);
                    Assertions.assertThat(context).doesNotHaveBean(TitusAgentLauncherImpl.TitusJobRequestAdapter.class);
                    Assertions.assertThat(context).doesNotHaveBean(TitusAgentLauncherImpl.class);
                    Assertions.assertThat(context).doesNotHaveBean("titusAPIRetryPolicy");
                    Assertions.assertThat(context).doesNotHaveBean(TitusAgentLauncherImpl.TitusAPIRetryPolicy.class);
                    Assertions.assertThat(context).doesNotHaveBean("titusAPIBackoffPolicy");
                    Assertions.assertThat(context).doesNotHaveBean("titusAPIRetryTemplate");
                    Assertions.assertThat(context).doesNotHaveBean("titusRestTemplate");
                }
            );
    }

    /**
     * .
     */
    @Test
    void testTitusAgentLauncherOnlyBean() {
        this.contextRunner
            .withPropertyValues(
                "genie.agent.launcher.titus.enabled=true",
                "genie.agent.launcher.local.enabled=false"
            )
            .run(
                context -> {
                    Assertions.assertThat(context).hasSingleBean(LocalAgentLauncherProperties.class);
                    Assertions.assertThat(context).hasSingleBean(TitusAgentLauncherProperties.class);
                    Assertions.assertThat(context).hasBean("titusAPIRetryPolicy");
                    Assertions.assertThat(context).hasSingleBean(TitusAgentLauncherImpl.TitusAPIRetryPolicy.class);
                    Assertions.assertThat(context).hasBean("titusAPIBackoffPolicy");
                    Assertions.assertThat(context).hasBean("titusAPIRetryTemplate");
                    Assertions.assertThat(context).hasBean("titusRestTemplate");
                    Assertions.assertThat(context).hasSingleBean(TitusAgentLauncherImpl.TitusJobRequestAdapter.class);
                    Assertions.assertThat(context).hasSingleBean(TitusAgentLauncherImpl.class);
                    Assertions.assertThat(context).doesNotHaveBean(LocalAgentLauncherImpl.class);
                }
            );
    }

    static class UserConfig {
        @Bean
        GenieWebHostInfo genieWebHostInfo() {
            return new GenieWebHostInfo(UUID.randomUUID().toString());
        }

        @Bean
        GenieWebRpcInfo genieWebRpcInfo() {
            return new GenieWebRpcInfo(33_433);
        }

        @Bean
        PersistenceService geniePersistenceService() {
            return Mockito.mock(PersistenceService.class);
        }

        @Bean
        DataServices genieDataServices(final PersistenceService persistenceService) {
            final DataServices dataServices = Mockito.mock(DataServices.class);
            Mockito.when(dataServices.getPersistenceService()).thenReturn(persistenceService);
            return dataServices;
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        RestTemplateBuilder restTemplateBuilder() {
            return new RestTemplateBuilder();
        }

        @Bean
        BraveTracingComponents genieTracingComponents() {
            return new BraveTracingComponents(
                Mockito.mock(Tracer.class),
                Mockito.mock(BraveTracePropagator.class),
                Mockito.mock(BraveTracingCleanup.class),
                Mockito.mock(BraveTagAdapter.class)
            );
        }
    }
}
