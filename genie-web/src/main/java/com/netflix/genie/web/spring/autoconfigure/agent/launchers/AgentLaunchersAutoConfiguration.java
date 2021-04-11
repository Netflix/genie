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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.agent.launchers.impl.LocalAgentLauncherImpl;
import com.netflix.genie.web.agent.launchers.impl.TitusAgentLauncherImpl;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.introspection.GenieWebHostInfo;
import com.netflix.genie.web.introspection.GenieWebRpcInfo;
import com.netflix.genie.web.properties.LocalAgentLauncherProperties;
import com.netflix.genie.web.properties.TitusAgentLauncherProperties;
import com.netflix.genie.web.util.ExecutorFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.web.client.RestTemplateAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * Auto configuration for beans responsible for launching Genie Agent instances.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        LocalAgentLauncherProperties.class,
        TitusAgentLauncherProperties.class
    }
)
@AutoConfigureAfter(
    {
        RestTemplateAutoConfiguration.class
    }
)
public class AgentLaunchersAutoConfiguration {

    /**
     * Provide a {@link RestTemplate} instance used for calling the Titus REST API if no other instance is provided.
     *
     * @param restTemplateBuilder The Spring {@link RestTemplateBuilder} instance to use
     * @return The rest template to use
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(name = "titusRestTemplate")
    public RestTemplate titusRestTemplate(final RestTemplateBuilder restTemplateBuilder) {
        return restTemplateBuilder.build();
    }

    /**
     * Provides a default implementation of {@link TitusAgentLauncherImpl.TitusJobRequestAdapter} that is a no-op
     * if no other implementation has been provided elsewhere.
     *
     * @return A no-op implementation
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(TitusAgentLauncherImpl.TitusJobRequestAdapter.class)
    public TitusAgentLauncherImpl.TitusJobRequestAdapter titusJobRequestAdapter() {
        // No-Op default implementation
        return new TitusAgentLauncherImpl.TitusJobRequestAdapter() {
        };
    }

    /**
     * Provides a default implementation of {@link org.springframework.retry.RetryPolicy} that retries based on a set
     * of HTTP status codes. Currently just {@link org.springframework.http.HttpStatus#SERVICE_UNAVAILABLE} and
     * {@link org.springframework.http.HttpStatus#REQUEST_TIMEOUT}. Max retries set to 3.
     *
     * @return A {@link TitusAgentLauncherImpl.TitusAPIRetryPolicy} instance with the default settings applied
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(name = "titusAPIRetryPolicy", value = RetryPolicy.class)
    public TitusAgentLauncherImpl.TitusAPIRetryPolicy titusAPIRetryPolicy() {
        return new TitusAgentLauncherImpl.TitusAPIRetryPolicy(
            EnumSet.of(HttpStatus.SERVICE_UNAVAILABLE, HttpStatus.REQUEST_TIMEOUT),
            3
        );
    }

    /**
     * Provides a default implementation of {@link org.springframework.retry.backoff.BackOffPolicy} if no other has
     * been defined in the context.
     *
     * @return A default {@link ExponentialBackOffPolicy} instance
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(name = "titusAPIBackoffPolicy", value = BackOffPolicy.class)
    public ExponentialBackOffPolicy titusAPIBackoffPolicy() {
        return new ExponentialBackOffPolicy();
    }

    /**
     * Provides a default implementation of {@link RetryTemplate} that will be used to retry failed Titus api calls
     * based on the retry policy and backoff policies defined in the application context.
     *
     * @param retryPolicy   The {@link RetryPolicy} to use for Titus API call failures
     * @param backOffPolicy The {@link BackOffPolicy} to use for Titus API call failures
     * @return A {@link RetryTemplate} instance configured with the supplied retry and backoff policies
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(name = "titusAPIRetryTemplate", value = RetryTemplate.class)
    public RetryTemplate titusAPIRetryTemplate(
        @Qualifier("titusAPIRetryPolicy") final RetryPolicy retryPolicy,
        @Qualifier("titusAPIBackoffPolicy") final BackOffPolicy backOffPolicy
    ) {
        final RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

    /**
     * Provide a {@link TitusAgentLauncherImpl} implementation which launches agent processes in a dedicated Titus
     * container if enabled via property.
     *
     * @param restTemplate                 the rest template
     * @param retryTemplate                The {@link RetryTemplate} instance to use to retry failed Titus API calls
     * @param titusJobRequestAdapter       The {@link TitusAgentLauncherImpl.TitusJobRequestAdapter} implementation to
     *                                     use
     * @param genieHostInfo                the metadata about the local server and host
     * @param titusAgentLauncherProperties the configuration properties
     * @param tracingComponents            The {@link BraveTracingComponents} instance to use
     * @param environment                  The application {@link Environment} used to pull dynamic properties after
     *                                     launch
     * @param registry                     the metric registry
     * @return a {@link TitusAgentLauncherImpl}
     */
    @Bean
    @ConditionalOnProperty(name = TitusAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    public TitusAgentLauncherImpl titusAgentLauncher(
        @Qualifier("titusRestTemplate") final RestTemplate restTemplate,
        @Qualifier("titusAPIRetryTemplate") final RetryTemplate retryTemplate,
        final TitusAgentLauncherImpl.TitusJobRequestAdapter titusJobRequestAdapter,
        final GenieHostInfo genieHostInfo,
        final TitusAgentLauncherProperties titusAgentLauncherProperties,
        final BraveTracingComponents tracingComponents,
        final Environment environment,
        final MeterRegistry registry
    ) {
        final Cache<String, String> healthIndicatorCache = Caffeine.newBuilder()
            .maximumSize(titusAgentLauncherProperties.getHealthIndicatorMaxSize())
            .expireAfterWrite(
                titusAgentLauncherProperties.getHealthIndicatorExpiration().getSeconds(),
                TimeUnit.SECONDS
            )
            .build();

        return new TitusAgentLauncherImpl(
            restTemplate,
            retryTemplate,
            titusJobRequestAdapter,
            healthIndicatorCache,
            genieHostInfo,
            titusAgentLauncherProperties,
            tracingComponents,
            environment,
            registry
        );
    }

    /**
     * Provide an {@link ExecutorFactory} instance if no other was defined.
     *
     * @return Instance of {@link ExecutorFactory}
     */
    @Bean
    @ConditionalOnMissingBean(ExecutorFactory.class)
    public ExecutorFactory processExecutorFactory() {
        return new ExecutorFactory();
    }

    /**
     * Provide a {@link AgentLauncher} implementation which launches local agent processes if enabled via property.
     *
     * @param genieWebHostInfo   The {@link GenieWebHostInfo} of this instance
     * @param genieWebRpcInfo    The {@link GenieWebRpcInfo} of this instance
     * @param dataServices       The {@link DataServices} instance to use
     * @param launcherProperties The properties related to launching an agent locally
     * @param executorFactory    The {@link ExecutorFactory} to use to launch agent processes
     * @param tracingComponents  The {@link BraveTracingComponents} instance to use
     * @param registry           The {@link MeterRegistry} to register metrics
     * @return A {@link LocalAgentLauncherImpl} instance
     */
    @Bean
    @ConditionalOnProperty(name = LocalAgentLauncherProperties.ENABLE_PROPERTY, havingValue = "true")
    public LocalAgentLauncherImpl localAgentLauncher(
        final GenieWebHostInfo genieWebHostInfo,
        final GenieWebRpcInfo genieWebRpcInfo,
        final DataServices dataServices,
        final LocalAgentLauncherProperties launcherProperties,
        final ExecutorFactory executorFactory,
        final BraveTracingComponents tracingComponents,
        final MeterRegistry registry
    ) {
        return new LocalAgentLauncherImpl(
            genieWebHostInfo,
            genieWebRpcInfo,
            dataServices,
            launcherProperties,
            executorFactory,
            tracingComponents,
            registry
        );
    }
}
