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
package com.netflix.genie.web.spring.autoconfigure.services;

import brave.Tracer;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import com.netflix.genie.web.properties.JobsActiveLimitProperties;
import com.netflix.genie.web.properties.JobsForwardingProperties;
import com.netflix.genie.web.properties.JobsLocationsProperties;
import com.netflix.genie.web.properties.JobsMemoryProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.JobsUsersProperties;
import com.netflix.genie.web.selectors.AgentLauncherSelector;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.RequestForwardingService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

/**
 * Tests for {@link ServicesAutoConfiguration} class.
 *
 * @author mprimi
 * @since 4.0.0
 */
class ServicesAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner =
        new ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    ServicesAutoConfiguration.class
                )
            )
            .withUserConfiguration(RequiredMockConfig.class);

    @Test
    void canCreateBeans() {
        this.contextRunner.run(
            context -> {
                Assertions.assertThat(context).hasSingleBean(JobsForwardingProperties.class);
                Assertions.assertThat(context).hasSingleBean(JobsLocationsProperties.class);
                Assertions.assertThat(context).hasSingleBean(JobsMemoryProperties.class);
                Assertions.assertThat(context).hasSingleBean(JobsUsersProperties.class);
                Assertions.assertThat(context).hasSingleBean(JobsActiveLimitProperties.class);
                Assertions.assertThat(context).hasSingleBean(AttachmentServiceProperties.class);
                Assertions.assertThat(context).hasSingleBean(JobsProperties.class);
                Assertions.assertThat(context).hasSingleBean(AttachmentService.class);
                Assertions.assertThat(context).hasSingleBean(JobResolverService.class);
                Assertions.assertThat(context).hasSingleBean(JobDirectoryServerService.class);
                Assertions.assertThat(context).hasSingleBean(JobLaunchService.class);
                Assertions.assertThat(context).hasSingleBean(ArchivedJobService.class);
                Assertions.assertThat(context).hasSingleBean(RequestForwardingService.class);
            }
        );
    }

    private static class RequiredMockConfig {

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        ResourceLoader resourceLoader() {
            return Mockito.mock(ResourceLoader.class);
        }

        @Bean
        DataServices dataServices() {
            return Mockito.mock(DataServices.class);
        }

        @Bean
        AgentFileStreamService agentFileStreamService() {
            return Mockito.mock(AgentFileStreamService.class);
        }

        @Bean
        AgentRoutingService agentRoutingService() {
            return Mockito.mock(AgentRoutingService.class);
        }

        @Bean
        S3ClientFactory s3ClientFactory() {
            return Mockito.mock(S3ClientFactory.class);
        }

        @Bean
        CommandSelector commandSelector() {
            return Mockito.mock(CommandSelector.class);
        }

        @Bean
        ClusterSelector clusterSelector1() {
            return Mockito.mock(ClusterSelector.class);
        }

        @Bean
        ClusterSelector clusterSelector2() {
            return Mockito.mock(ClusterSelector.class);
        }

        @Bean
        AgentLauncherSelector agentLauncherSelector() {
            return Mockito.mock(AgentLauncherSelector.class);
        }

        @Bean
        GenieHostInfo genieHostInfo() {
            return new GenieHostInfo(UUID.randomUUID().toString());
        }

        @Bean(name = "genieRestTemplate")
        RestTemplate genieRestTemplate() {
            return new RestTemplate();
        }

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
