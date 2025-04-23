/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.data.services.impl.jpa;

import brave.SpanCustomizer;
import brave.Tracer;
import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.netflix.genie.common.internal.spring.autoconfigure.CommonTracingAutoConfiguration;
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter;
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.web.data.observers.PersistedJobStatusObserver;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaCriterionRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.data.services.impl.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.spring.autoconfigure.ValidationAutoConfiguration;
import com.netflix.genie.web.spring.autoconfigure.data.DataAutoConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

/**
 * Base class to save on configuration.
 *
 * @author tgianos
 * @since 4.0.0
 */
@DataJpaTest
@TestExecutionListeners(
    {
        DependencyInjectionTestExecutionListener.class,
        TransactionDbUnitTestExecutionListener.class
    }
)
@Import(
    {
        DataAutoConfiguration.class,
        ValidationAutoConfiguration.class,
        CommonTracingAutoConfiguration.class,
        JpaPersistenceServiceIntegrationTestBase.TestConfig.class
    }
)
//@TestPropertySource(
//    properties = {
//        "logging.level.com.netflix.genie.web.data.services.impl.jpa=DEBUG", // Genie JPA package
//        "logging.level.org.hibernate.SQL=DEBUG",                            // Show SQL queries
//        "logging.level.org.hibernate.type.descriptor.sql=TRACE",            // Parameters, extracted values and more
//    }
//)
class JpaPersistenceServiceIntegrationTestBase {

    @Autowired
    protected PersistedJobStatusObserver persistedJobStatusObserver;

    @Autowired
    protected CommonTracingAutoConfiguration commonTracingAutoConfiguration;

    @Autowired
    protected JpaApplicationRepository applicationRepository;

    @Autowired
    protected JpaClusterRepository clusterRepository;

    @Autowired
    protected JpaCommandRepository commandRepository;

    @Autowired
    protected JpaJobRepository jobRepository;

    @Autowired
    protected JpaFileRepository fileRepository;

    @Autowired
    protected JpaTagRepository tagRepository;

    @Autowired
    protected JpaCriterionRepository criterionRepository;

    @Autowired
    protected JpaPersistenceServiceImpl service;

    @Autowired
    protected TestEntityManager entityManager;

    @AfterEach
    void resetMocks() {
        // Could use @DirtiesContext but seems excessive
        Mockito.reset(this.persistedJobStatusObserver);
        Mockito.reset(this.commonTracingAutoConfiguration);
    }

    /**
     * Test configuration to provide mock beans.
     */
    @Configuration
    static class TestConfig {
        @Bean
        @Primary
        public Tracer tracer() {
            final Tracer mockTracer = Mockito.mock(Tracer.class, Answers.RETURNS_DEEP_STUBS);
            final SpanCustomizer mockSpanCustomizer = Mockito.mock(SpanCustomizer.class, Answers.RETURNS_DEEP_STUBS);
            Mockito.when(mockTracer.currentSpanCustomizer()).thenReturn(mockSpanCustomizer);
            return mockTracer;
        }

        @Bean
        @Primary
        public BraveTagAdapter braveTagAdapter() {
            return Mockito.mock(BraveTagAdapter.class);
        }

        @Bean
        @Primary
        public BraveTracePropagator braveTracePropagator() {
            return Mockito.mock(BraveTracePropagator.class);
        }

        @Bean
        @Primary
        public BraveTracingComponents braveTracingComponents(
            final Tracer tracer,
            final BraveTagAdapter tagAdapter,
            final BraveTracePropagator tracePropagator
        ) {
            final BraveTracingComponents mockComponents = Mockito.mock(BraveTracingComponents.class);
            Mockito.when(mockComponents.getTracer()).thenReturn(tracer);
            Mockito.when(mockComponents.getTagAdapter()).thenReturn(tagAdapter);
            Mockito.when(mockComponents.getTracePropagator()).thenReturn(tracePropagator);
            return mockComponents;
        }

        @Bean
        @Primary
        public PersistedJobStatusObserver persistedJobStatusObserver() {
            return Mockito.mock(PersistedJobStatusObserver.class);
        }

        @Bean
        @Primary
        public CommonTracingAutoConfiguration commonTracingAutoConfiguration() {
            return Mockito.mock(CommonTracingAutoConfiguration.class);
        }
    }
}
