/*
 *
 *  Copyright 2021 Netflix, Inc.
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
package com.netflix.genie.common.internal.spring.autoconfigure;

import brave.Tracer;
import com.netflix.genie.common.internal.tracing.TracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.common.internal.tracing.brave.impl.DefaultBraveTagAdapterImpl;
import com.netflix.genie.common.internal.tracing.brave.impl.EnvVarBraveTracePropagatorImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.reporter.AsyncReporter;

import java.util.Set;

/**
 * Auto configuration for common tracing components within Genie server and agent.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Configuration
public class CommonTracingAutoConfiguration {

    /**
     * Provide an implementation of {@link TracePropagator} if no other has been defined.
     *
     * @return instance of {@link EnvVarBraveTracePropagatorImpl}
     */
    @Bean
    @ConditionalOnMissingBean(BraveTracePropagator.class)
    public EnvVarBraveTracePropagatorImpl braveTracePropagator() {
        return new EnvVarBraveTracePropagatorImpl();
    }

    /**
     * Provide a {@link BraveTracingCleanup} based on the context.
     *
     * @param reporters Any {@link AsyncReporter} instances configured
     * @return A {@link BraveTracingCleanup} instance
     */
    @Bean
    @ConditionalOnMissingBean(BraveTracingCleanup.class)
    public BraveTracingCleanup braveTracingCleaner(final Set<AsyncReporter<?>> reporters) {
        return new BraveTracingCleanup(reporters);
    }

    /**
     * Provide a {@link BraveTagAdapter} instance if no other has been provided.
     *
     * @return A {@link DefaultBraveTagAdapterImpl} instance which just directly applies the tags to the span
     */
    @Bean
    @ConditionalOnMissingBean(BraveTagAdapter.class)
    public DefaultBraveTagAdapterImpl braveTagAdapter() {
        return new DefaultBraveTagAdapterImpl();
    }

    /**
     * Provide a {@link BraveTracingComponents} instance based on Brave if no other has been provided.
     *
     * @param tracer          The {@link Tracer} instance to use
     * @param tracePropagator The {@link BraveTracePropagator} to use
     * @param tracingCleanup  The {@link BraveTracingCleanup} to use
     * @param tagAdapter      The {@link BraveTagAdapter} instance to use
     * @return A {@link BraveTracingComponents} instance
     */
    @Bean
    @ConditionalOnMissingBean(BraveTracingComponents.class)
    public BraveTracingComponents braveTracingComponents(
        final Tracer tracer,
        final BraveTracePropagator tracePropagator,
        final BraveTracingCleanup tracingCleanup,
        final BraveTagAdapter tagAdapter
    ) {
        return new BraveTracingComponents(tracer, tracePropagator, tracingCleanup, tagAdapter);
    }
}
