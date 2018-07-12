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
package com.netflix.genie.web.jpa.services;

import com.github.springtestdbunit.TransactionDbUnitTestExecutionListener;
import com.netflix.genie.GenieTestApp;
import com.netflix.genie.web.jpa.repositories.JpaAgentConnectionRepository;
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;

/**
 * Base class to save on configuration.
 *
 * @author tgianos
 */
@RunWith(SpringRunner.class)
//@DataJpaTest
@SpringBootTest(classes = GenieTestApp.class, webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestExecutionListeners(
    {
        DependencyInjectionTestExecutionListener.class,
        DirtiesContextTestExecutionListener.class,
        TransactionalTestExecutionListener.class,
        TransactionDbUnitTestExecutionListener.class
    }
)
@ActiveProfiles(
    {
        "integration",
        "db",
        "db-tomcat", // Force use of Tomcat Connection pool since Hikari blows up spring dbunit
        "db-h2",
    }
)
public abstract class DBIntegrationTestBase {

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

    /**
     * Clean out the db after every test.
     */
    @After
    public void cleanup() {
        this.jobRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
        this.fileRepository.deleteAll();
        this.tagRepository.deleteAll();
        this.agentConnectionRepository.deleteAll();
    }
}
