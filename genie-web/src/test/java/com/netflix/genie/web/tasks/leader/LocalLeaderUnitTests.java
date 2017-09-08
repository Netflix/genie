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

import com.netflix.genie.core.events.GenieEventBus;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
@Category(UnitTest.class)
public class LocalLeaderUnitTests {

    private LocalLeader localLeader;
    private GenieEventBus genieEventBus;
    private ContextRefreshedEvent contextRefreshedEvent;
    private ContextClosedEvent contextClosedEvent;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.genieEventBus = Mockito.mock(GenieEventBus.class);
        this.contextRefreshedEvent = Mockito.mock(ContextRefreshedEvent.class);
        this.contextClosedEvent = Mockito.mock(ContextClosedEvent.class);
    }

    /**
     * Tear down the tests to prepare for the next iteration.
     */
    @After
    public void tearDown() {
        this.localLeader = null;
    }

    /**
     * Make sure the event to grant leadership is fired if the node is the leader.
     */
    @Test
    public void canStartLeadershipIfLeader() {
        this.localLeader = new LocalLeader(this.genieEventBus, true);
        this.localLeader.startLeadership(this.contextRefreshedEvent);
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
    }

    /**
     * Make sure the event to grant leadership is not fired if the node is not the leader.
     */
    @Test
    public void wontStartLeadershipIfNotLeader() {
        this.localLeader = new LocalLeader(this.genieEventBus, false);
        this.localLeader.startLeadership(this.contextRefreshedEvent);
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnGrantedEvent.class));
    }

    /**
     * Make sure the event to revoke leadership is fired if the node is the leader.
     */
    @Test
    public void canStopLeadershipIfLeader() {
        this.localLeader = new LocalLeader(this.genieEventBus, true);
        this.localLeader.stopLeadership(this.contextClosedEvent);
        Mockito.verify(this.genieEventBus, Mockito.times(1)).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));
    }

    /**
     * Make sure the event to revoke leadership is not fired if the node is not the leader.
     */
    @Test
    public void wontStopLeadershipIfNotLeader() {
        this.localLeader = new LocalLeader(this.genieEventBus, false);
        this.localLeader.stopLeadership(this.contextClosedEvent);
        Mockito.verify(this.genieEventBus, Mockito.never()).publishSynchronousEvent(Mockito.any(OnRevokedEvent.class));
    }
}
