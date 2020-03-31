/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.tasks.leader;

import com.netflix.genie.web.events.GenieEventBus;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.integration.leader.event.OnRevokedEvent;

/**
 * Unit tests for the LocalLeader class.
 *
 * @author tgianos
 * @since 3.0.0
 */
class LocalLeaderTest {

    private LocalLeader localLeader;
    private GenieEventBus genieEventBus;
    private ContextRefreshedEvent contextRefreshedEvent;
    private ContextClosedEvent contextClosedEvent;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.genieEventBus = Mockito.mock(GenieEventBus.class);
        this.contextRefreshedEvent = Mockito.mock(ContextRefreshedEvent.class);
        this.contextClosedEvent = Mockito.mock(ContextClosedEvent.class);
    }

    /**
     * Tear down the tests to prepare for the next iteration.
     */
    @AfterEach
    void tearDown() {
        this.localLeader = null;
    }

    /**
     * Ensure behavior in case of start, stop, start when already running and stop when not running in case the module
     * is configured to be leader.
     */
    @Test
    void startAndStopIfLeader() {
        this.localLeader = new LocalLeader(this.genieEventBus, true);
        Assertions.assertThat(this.localLeader.isRunning()).isFalse();
        Assertions.assertThat(this.localLeader.isLeader()).isFalse();
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));

        // Start with application context event
        this.localLeader.startLeadership(this.contextRefreshedEvent);
        Assertions.assertThat(this.localLeader.isRunning()).isTrue();
        Assertions.assertThat(this.localLeader.isLeader()).isTrue();
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));

        // Start again
        this.localLeader.start();
        Assertions.assertThat(this.localLeader.isRunning()).isTrue();
        Assertions.assertThat(this.localLeader.isLeader()).isTrue();
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));

        // Stop with application context event
        this.localLeader.stopLeadership(this.contextClosedEvent);
        Assertions.assertThat(this.localLeader.isRunning()).isFalse();
        Assertions.assertThat(this.localLeader.isLeader()).isFalse();
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));

        // Stop again
        this.localLeader.stopLeadership(this.contextClosedEvent);
        Assertions.assertThat(this.localLeader.isRunning()).isFalse();
        Assertions.assertThat(this.localLeader.isLeader()).isFalse();
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));
    }

    /**
     * Ensure behavior in case of start, stop in case the module is not configured to be leader.
     */
    @Test
    void startAndStopIfNotLeader() {
        this.localLeader = new LocalLeader(this.genieEventBus, false);
        Assertions.assertThat(this.localLeader.isRunning()).isFalse();
        Assertions.assertThat(this.localLeader.isLeader()).isFalse();
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));

        // Start
        this.localLeader.start();
        Assertions.assertThat(this.localLeader.isRunning()).isTrue();
        Assertions.assertThat(this.localLeader.isLeader()).isFalse();
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));

        // Stop
        this.localLeader.stopLeadership(this.contextClosedEvent);
        Assertions.assertThat(this.localLeader.isRunning()).isFalse();
        Assertions.assertThat(this.localLeader.isLeader()).isFalse();
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));
    }
}
