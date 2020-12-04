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

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.netflix.genie.common.internal.spring.autoconfigure.CommonTracingAutoConfiguration;
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
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.sleuth.autoconfig.brave.BraveAutoConfiguration;
import org.springframework.context.annotation.Import;
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
        BraveAutoConfiguration.class,
        CommonTracingAutoConfiguration.class
    }
)
@MockBean(
    {
        PersistedJobStatusObserver.class //TODO: Needed for JobEntityListener but should be in DataAutoConfiguration
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
    protected PersistedJobStatusObserver persistedJobStatusObserver;

    @Autowired
    protected TestEntityManager entityManager;

    @AfterEach
    void resetMocks() {
        // Could use @DirtiesContext but seems excessive
        Mockito.reset(this.persistedJobStatusObserver);
    }
}
