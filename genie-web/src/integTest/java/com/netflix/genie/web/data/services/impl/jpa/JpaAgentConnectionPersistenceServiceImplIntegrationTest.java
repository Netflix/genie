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
package com.netflix.genie.web.data.services.impl.jpa;

import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link JpaPersistenceServiceImpl} focusing on the Agent Connection APIs.
 *
 * @author mprimi
 * @since 4.0.0
 */
class JpaAgentConnectionPersistenceServiceImplIntegrationTest extends JpaPersistenceServiceIntegrationTestBase {

    private static final String JOB1 = "job1";
    private static final String HOST1 = "host1";
    private static final String JOB2 = "job2";
    private static final String HOST2 = "host2";
    private static final String JOB3 = "job3";
    private static final String HOST3 = "host3";

    @Test
    void createUpdateDelete() {
        // Check empty
        this.verifyExpectedConnections();
        Assertions.assertThat(this.agentConnectionRepository.count()).isEqualTo(0L);

        // Create two connections for two jobs on different servers
        this.service.saveAgentConnection(JOB1, HOST1);
        this.service.saveAgentConnection(JOB2, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));

        // Migrate a connection, with delete before update
        this.service.removeAgentConnection(JOB1, HOST1);
        this.service.saveAgentConnection(JOB1, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST2), Pair.of(JOB2, HOST2));

        // Migrate a connection with update before delete
        this.service.saveAgentConnection(JOB1, HOST1);
        this.service.removeAgentConnection(JOB1, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));

        // Migrate a connection, landing on the same server with deletion
        this.service.removeAgentConnection(JOB1, HOST1);
        this.service.saveAgentConnection(JOB1, HOST1);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));

        // Migrate a connection, landing on the same server without deletion (unexpected in practice)
        this.service.saveAgentConnection(JOB1, HOST1);
        this.service.saveAgentConnection(JOB1, HOST1);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST2));

        // Delete all
        this.service.removeAgentConnection(JOB1, HOST1);
        this.service.removeAgentConnection(JOB2, HOST2);
        this.verifyExpectedConnections();

        // Create new connections
        this.service.saveAgentConnection(JOB1, HOST1);
        this.service.saveAgentConnection(JOB2, HOST1);
        this.service.saveAgentConnection(JOB3, HOST2);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST1), Pair.of(JOB3, HOST2));
        Assertions
            .assertThat(this.service.removeAllAgentConnectionsToServer(HOST3))
            .isEqualTo(0L);
        this.verifyExpectedConnections(Pair.of(JOB1, HOST1), Pair.of(JOB2, HOST1), Pair.of(JOB3, HOST2));
        Assertions
            .assertThat(this.service.removeAllAgentConnectionsToServer(HOST1))
            .isEqualTo(2L);
        this.verifyExpectedConnections(Pair.of(JOB3, HOST2));
        Assertions
            .assertThat(this.service.removeAllAgentConnectionsToServer(HOST2))
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
                .assertThat(this.service.lookupAgentConnectionServer(jobId))
                .isPresent()
                .contains(hostname);
        }
    }
}
