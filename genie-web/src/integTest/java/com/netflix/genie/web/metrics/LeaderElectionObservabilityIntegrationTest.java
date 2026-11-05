/*
 *
 *  Copyright 2026 Netflix, Inc.
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
package com.netflix.genie.web.metrics;

import com.netflix.genie.web.services.ClusterLeaderService;
import com.netflix.genie.web.services.impl.ClusterLeaderServiceCuratorImpl;
import com.netflix.genie.web.tasks.leader.LeaderSchedulerHeartbeatTask;
import com.netflix.genie.web.tasks.leader.LeaderTasksCoordinator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.integration.leader.Candidate;
import org.springframework.integration.leader.Context;
import org.springframework.integration.leader.event.AbstractLeaderEvent;
import org.springframework.integration.leader.event.DefaultLeaderEventPublisher;
import org.springframework.integration.zookeeper.leader.LeaderInitiator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Integration tests for the Curator-backed application leader-election metrics.
 *
 * <p>Runs an actual Zookeeper instance and checks metrics as leader election happens.</p>
 */
class LeaderElectionObservabilityIntegrationTest {

    private TestingServer zkServer;
    private ElectionNode node1;
    private ElectionNode node2;

    @BeforeEach
    void setUp() throws Exception {
        this.zkServer = new TestingServer();
        this.node1 = new ElectionNode("node-1", this.zkServer.getConnectString());
        this.node2 = new ElectionNode("node-2", this.zkServer.getConnectString());
        this.node1.start();
        this.node2.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (this.node1 != null) {
            this.node1.close();
        }
        if (this.node2 != null) {
            this.node2.close();
        }
        if (this.zkServer != null) {
            this.zkServer.close();
        }
    }

    /**
     * Kill the server and wait for leadership to be revoked.
     * Then, restart the server and wait for election to succeed.
     *
     * @throws Exception if the test server cannot be restarted
     */
    @Test
    void metricsFollowLeadershipLossAndReelection() throws Exception {
        awaitCondition(
            () -> this.node1.service.isLeader() != this.node2.service.isLeader(),
            "exactly one leader should be elected initially"
        );
        Assertions.assertThat(this.node1.leaderGauge() + this.node2.leaderGauge()).isEqualTo(1.0);
        awaitCondition(
            () -> this.node1.heartbeatCount() + this.node2.heartbeatCount() > 0.0,
            "the elected node should execute the heartbeat task"
        );
        final double initialHeartbeat1 = this.node1.heartbeatCount();
        final double initialHeartbeat2 = this.node2.heartbeatCount();

        this.zkServer.stop();
        awaitCondition(
            () -> !this.node1.service.isLeader() && !this.node2.service.isLeader(),
            "initial leader node should have its leadership revoked after zookeeper server dies"
        );
        Assertions.assertThat(this.node1.leaderGauge() + this.node2.leaderGauge()).isEqualTo(0.0);

        this.zkServer.restart();
        awaitCondition(
            () -> this.node1.service.isLeader() ^ this.node2.service.isLeader(),
            "one contender should be elected after the server restarts"
        );
        Assertions.assertThat(this.node1.leaderGauge() + this.node2.leaderGauge()).isEqualTo(1.0);
        final ElectionNode electedNode = this.node1.service.isLeader() ? this.node1 : this.node2;
        awaitCondition(
            () -> electedNode.heartbeatCount() > Math.max(initialHeartbeat1, initialHeartbeat2),
            "newly elected node should resume the leader-task heartbeat"
        );
    }

    /** Helper to wait for async Zookeeper operations to complete. */
    private static void awaitCondition(final BooleanSupplier condition, final String description)
        throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        boolean conditionMet = condition.getAsBoolean();
        while (!conditionMet && System.nanoTime() < deadline) {
            Thread.sleep(100L);
            conditionMet = condition.getAsBoolean();
        }
        Assertions.assertThat(conditionMet).as(description).isTrue();
    }

    private static final class ElectionNode implements AutoCloseable {

        private final CuratorFramework curator;
        private final LeaderInitiator initiator;
        private final ClusterLeaderService service;
        private final SimpleMeterRegistry registry;
        private final LeaderTasksCoordinator coordinator;
        private final ThreadPoolTaskScheduler scheduler;
        private final LeaderSchedulerHeartbeatTask heartbeat;

        private ElectionNode(final String id, final String connectString) {
            this.curator = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(4_000)
                .connectionTimeoutMs(1_000)
                .retryPolicy(new ExponentialBackoffRetry(100, 3))
                .build();
            this.curator.start();
            this.scheduler = new ThreadPoolTaskScheduler();
            this.scheduler.setPoolSize(1);
            this.scheduler.setThreadNamePrefix("leader-election-test-" + id + "-");
            this.scheduler.initialize();
            this.initiator = new LeaderInitiator(
                this.curator,
                new TestCandidate(id),
                "genie-test-leader"
            );
            this.service = new ClusterLeaderServiceCuratorImpl(this.initiator);
            this.registry = new SimpleMeterRegistry();
            new LeaderElectionMetrics(this.service, this.registry);
            this.heartbeat = new LeaderSchedulerHeartbeatTask(this.registry);
            this.coordinator = new LeaderTasksCoordinator(this.scheduler, Set.of(this.heartbeat));
            final ApplicationEventPublisher publisher = event -> {
                if (event instanceof AbstractLeaderEvent) {
                    this.coordinator.onLeaderEvent((AbstractLeaderEvent) event);
                }
            };
            this.initiator.setLeaderEventPublisher(new DefaultLeaderEventPublisher(publisher));
        }

        private void start() throws InterruptedException {
            if (!this.curator.blockUntilConnected(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Curator did not connect to the test server");
            }
            this.initiator.start();
        }

        private double leaderGauge() {
            return this.registry
                .get(LeaderElectionMetrics.LEADER_GAUGE_NAME)
                .gauge()
                .value();
        }

        private double heartbeatCount() {
            return this.registry
                .get(LeaderSchedulerHeartbeatTask.HEARTBEAT_COUNTER_NAME)
                .counter()
                .count();
        }

        @Override
        public void close() throws IOException {
            this.initiator.stop();
            this.coordinator.preDestroy();
            this.scheduler.destroy();
            this.curator.close();
        }
    }

    private static final class TestCandidate implements Candidate {

        private final String id;

        private TestCandidate(final String id) {
            this.id = id;
        }

        @Override
        public String getRole() {
            return "genie-test-leader";
        }

        @Override
        public String getId() {
            return this.id;
        }

        @Override
        public void onGranted(final Context context) {
        }

        @Override
        public void onRevoked(final Context context) {
        }
    }
}
