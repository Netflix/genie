/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.agent.services.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This integration test "documents" the behavior of Curator's Discovery Service and the assumptions on top of which
 * {@link AgentRoutingServiceCuratorDiscoveryImpl} is build upon.
 * - Multiple calls to {@code registerService} are idempotent
 * - {@code registerService} overwrites the existing service instance (if one exists) regardless of owner/timestamp
 * - {@code unregisterService} deletes the service instance regardless of owner/timestamp
 * - {@code unregisterService} deletes the service instance regardless of owner/timestamp
 */
class CuratorDiscoveryIntegrationTest {

    protected static final String SERVICE_NAME = "agent_connections";
    protected static final ServiceType SERVICE_TYPE = ServiceType.DYNAMIC;

    private final String jobId = UUID.randomUUID().toString();
    private final AgentRoutingServiceCuratorDiscoveryImpl.Agent agent =
        new AgentRoutingServiceCuratorDiscoveryImpl.Agent(jobId);
    private final String address1 = "A.B.C.D";
    private final String address2 = "A.B.X.Y";
    private final ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> agentInstance1 = new ServiceInstance<>(
        SERVICE_NAME,
        jobId,
        address1,
        null,
        null,
        agent,
        Instant.now().minus(10, ChronoUnit.SECONDS).getEpochSecond(),
        SERVICE_TYPE,
        null
    );
    private final ServiceInstance<AgentRoutingServiceCuratorDiscoveryImpl.Agent> agentInstance2 = new ServiceInstance<>(
        SERVICE_NAME,
        jobId,
        address2,
        null,
        null,
        agent,
        Instant.now().getEpochSecond(),
        SERVICE_TYPE,
        null
    );

    private TestingServer zkServer;
    private CuratorFramework curator;
    private ServiceDiscovery<AgentRoutingServiceCuratorDiscoveryImpl.Agent> serviceDiscovery;

    @BeforeEach
    void setUp() throws Exception {
        this.zkServer = new TestingServer();

        this.curator = CuratorFrameworkFactory.builder()
            .connectString(zkServer.getConnectString())
            .retryPolicy(new ExponentialBackoffRetry(50, 4))
            .build();

        this.curator.start();
        this.curator.blockUntilConnected(10, TimeUnit.SECONDS);

        this.serviceDiscovery = ServiceDiscoveryBuilder.builder(AgentRoutingServiceCuratorDiscoveryImpl.Agent.class)
            .basePath("/discovery")
            .client(curator)
            .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        this.curator.close();
        this.zkServer.stop();
    }

    @Test
    void registerMultipleTimes() throws Exception {

        this.serviceDiscovery.registerService(agentInstance1);
        this.serviceDiscovery.registerService(agentInstance1);
        this.serviceDiscovery.registerService(agentInstance1);

        Assertions.assertThat(this.serviceDiscovery.queryForInstances(SERVICE_NAME)).hasSize(1);
        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isEqualTo(agentInstance1);

        this.serviceDiscovery.unregisterService(agentInstance1);

        Assertions.assertThat(this.serviceDiscovery.queryForInstances(SERVICE_NAME)).hasSize(0);
        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isNull();
    }

    @Test
    void lastRegisterOverwrites() throws Exception {

        this.serviceDiscovery.registerService(agentInstance1);

        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isEqualTo(agentInstance1);

        this.serviceDiscovery.registerService(agentInstance2);

        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isEqualTo(agentInstance2);

        this.serviceDiscovery.registerService(agentInstance1);

        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isEqualTo(agentInstance1);
    }

    @Test
    void unregisterDeletesInstanceByName() throws Exception {

        this.serviceDiscovery.registerService(agentInstance1);

        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isEqualTo(agentInstance1);

        this.serviceDiscovery.unregisterService(agentInstance2);

        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isNull();
    }

    @Test
    void updateFailsIfInstanceDoesNotExist() throws Exception {

        this.serviceDiscovery.registerService(agentInstance1);
        this.serviceDiscovery.unregisterService(agentInstance1);

        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isNull();

        Exception updateError = null;
        try {
            this.serviceDiscovery.updateService(agentInstance1);
        } catch (Exception e) {
            updateError = e;
        }
        Assertions.assertThat(updateError).isNotNull();
        Assertions.assertThat(this.serviceDiscovery.queryForInstance(SERVICE_NAME, jobId)).isNull();
    }
}
