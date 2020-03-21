/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.data.services.jpa;

import com.github.springtestdbunit.annotation.DatabaseTearDown;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Integration tests for the {@link JpaAgentConnectionPersistenceServiceImpl} class.
 *
 * @author mprimi
 * @since 4.0.0
 */
@DatabaseTearDown("cleanup.xml")
class JpaAgentConnectionPersistenceServiceImplIntegrationTest extends DBIntegrationTestBase {

    private static final String JOB1 = "job1";
    private static final String HOST1 = "host1";
    private static final String JOB2 = "job2";
    private static final String HOST2 = "host2";
    private static final String JOB3 = "job3";
    private static final String HOST3 = "host3";

    // This needs to be injected as a Spring Bean otherwise transactions don't work as there is no proxy
    @Autowired
    private JpaAgentConnectionPersistenceServiceImpl agentConnectionPersistenceService;

    /**
     * Perform assorted operations on persisted connections.
     */
    @Test
    void createUpdateDelete() {
        // Check empty
        this.verifyExpectedConnections();
        Assertions.assertThat(this.agentConnectionRepository.count()).isEqualTo(0L);

        // Create two connections for two jobs on different servers
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.saveAgentConnection(JOB2, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));
        this.verifyAgentConnectionsOnServer(HOST1, 1L);
        this.verifyAgentConnectionsOnServer(HOST2, 1L);

        // Migrate a connection, with delete before update
        this.agentConnectionPersistenceService.removeAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST2), Pair.of(JOB2, HOST2));
        this.verifyAgentConnectionsOnServer(HOST1, 0L);
        this.verifyAgentConnectionsOnServer(HOST2, 2L);

        // Migrate a connection with update before delete
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.removeAgentConnection(JOB1, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));
        this.verifyAgentConnectionsOnServer(HOST1, 1L);
        this.verifyAgentConnectionsOnServer(HOST2, 1L);

        // Migrate a connection, landing on the same server with deletion
        this.agentConnectionPersistenceService.removeAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST1);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));
        this.verifyAgentConnectionsOnServer(HOST1, 1L);
        this.verifyAgentConnectionsOnServer(HOST2, 1L);

        // Migrate a connection, landing on the same server without deletion (unexpected in practice)
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST1);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));
        this.verifyAgentConnectionsOnServer(HOST1, 1L);
        this.verifyAgentConnectionsOnServer(HOST2, 1L);

        // Delete all
        this.agentConnectionPersistenceService.removeAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.removeAgentConnection(JOB2, HOST2);
        this.verifyExpectedConnections();
        this.verifyAgentConnectionsOnServer(HOST1, 0L);
        this.verifyAgentConnectionsOnServer(HOST2, 0L);

        // Create new connections
        this.agentConnectionPersistenceService.saveAgentConnection(JOB1, HOST1);
        this.agentConnectionPersistenceService.saveAgentConnection(JOB2, HOST1);
        this.agentConnectionPersistenceService.saveAgentConnection(JOB3, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST1), Pair.of(JOB3, HOST2));
        Assertions
            .assertThat(this.agentConnectionPersistenceService.removeAllAgentConnectionToServer(HOST3))
            .isEqualTo(0L);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST1), Pair.of(JOB3, HOST2));
        Assertions
            .assertThat(this.agentConnectionPersistenceService.removeAllAgentConnectionToServer(HOST1))
            .isEqualTo(2L);
        this.verifyExpectedConnections(Pair.of(JOB3, HOST2));
        Assertions
            .assertThat(this.agentConnectionPersistenceService.removeAllAgentConnectionToServer(HOST2))
            .isEqualTo(1L);
        this.verifyExpectedConnections();
    }

    @SafeVarargs
    private final void verifyExpectedConnections(final Pair<String, String>... expectedConnections) {
        Assertions.assertThat(this.agentConnectionRepository.count()).isEqualTo(expectedConnections.length);

        for (final Pair<String, String> expectedConnection : expectedConnections) {
            // Verify a connection exists for this job id
            final String jobId = expectedConnection.getLeft();
            final String hostname = expectedConnection.getRight();

            Assertions
                .assertThat(this.agentConnectionPersistenceService.lookupAgentConnectionServer(jobId))
                .isPresent()
                .contains(hostname);
        }
    }

    private void verifyAgentConnectionsOnServer(final String hostname, final long expectedNumConnections) {
        Assertions
            .assertThat(this.agentConnectionPersistenceService.getNumAgentConnectionsOnServer(hostname))
            .isEqualTo(expectedNumConnections);
    }
}
