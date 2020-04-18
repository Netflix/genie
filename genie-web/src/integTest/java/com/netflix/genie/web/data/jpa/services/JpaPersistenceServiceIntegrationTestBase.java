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
package com.netflix.genie.web.data.jpa.services;

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.netflix.genie.web.data.observers.PersistedJobStatusObserver;
import com.netflix.genie.web.data.repositories.jpa.JpaAgentConnectionRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaApplicationRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaClusterRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCommandRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaCriterionRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaFileRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaJobRepository;
import com.netflix.genie.web.data.repositories.jpa.JpaTagRepository;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.spring.autoconfigure.ValidationAutoConfiguration;
import com.netflix.genie.web.spring.autoconfigure.data.DataAutoConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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
        ValidationAutoConfiguration.class
    }
)
@MockBean(
    {
        AttachmentService.class,
        PersistedJobStatusObserver.class //TODO: Needed for JobEntityListener but should be in DataAutoConfiguration
    }
)
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
    protected JpaAgentConnectionRepository agentConnectionRepository;

    @Autowired
    protected JpaCriterionRepository criterionRepository;

    @Autowired
    protected JpaPersistenceServiceImpl service;

    @Autowired
    protected PersistedJobStatusObserver persistedJobStatusObserver;

    @Autowired
    protected AttachmentService attachmentService;

    @AfterEach
    void resetMocks() {
        // Could use @DirtiesContext but seems excessive
        Mockito.reset(this.persistedJobStatusObserver, this.attachmentService);
    }
}
