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
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.integration.leader.event.OnRevokedEvent;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to control leadership activities when remote leadership isn't enabled and this node has been forcibly
 * elected as the leader.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
@ThreadSafe
public class LocalLeader {

    private final GenieEventBus genieEventBus;
    private final boolean isLeader;
    private final AtomicBoolean isRunning;

    /**
     * Constructor.
     *
     * @param genieEventBus The spring application event publisher to use to invoke that this node is a leader
     * @param isLeader      Whether this node should be the leader or not. Should only be one in a cluster but not
     *                      enforced by Genie at this time
     */
    public LocalLeader(final GenieEventBus genieEventBus, final boolean isLeader) {
        this.genieEventBus = genieEventBus;
        this.isLeader = isLeader;
        this.isRunning = new AtomicBoolean(false);
        if (this.isLeader) {
            log.info("Constructing LocalLeader. This node IS the leader.");
        } else {
            log.info("Constructing LocalLeader. This node IS NOT the leader.");
        }
    }

    /**
     * Auto-start once application is up and running.
     *
     * @param event The Spring Boot application ready event to startup on
     */
    @EventListener
    public void startLeadership(final ContextRefreshedEvent event) {
        log.debug("Starting Leadership due to {}", event);
        this.start();
    }

    /**
     * Auto-stop once application is shutting down.
     *
     * @param event The application context closing event
     */
    @EventListener
    public void stopLeadership(final ContextClosedEvent event) {
        log.debug("Stopping Leadership due to {}", event);
        this.stop();
    }

    /**
     * If configured to be leader and previously started, deactivate and send a leadership lost notification.
     * NOOP if not running or if this node is not configured to be leader.
     */
    public void stop() {
        if (this.isRunning.compareAndSet(true, false) && this.isLeader) {
            log.info("Stopping Leadership");
            this.genieEventBus.publishSynchronousEvent(new OnRevokedEvent(this, null, "leader"));
        }
    }

    /**
     * If configured to be leader, activate and send a leadership acquired notification.
     * NOOP if already running or if this node is not configured to be leader.
     */
    public void start() {
        if (this.isRunning.compareAndSet(false, true) && this.isLeader) {
            log.debug("Starting Leadership");
            this.genieEventBus.publishSynchronousEvent(new OnGrantedEvent(this, null, "leader"));
        }
    }

    /**
     * Whether this module is active.
     *
     * @return true if {@link LocalLeader#start()} was called.
     */
    public boolean isRunning() {
        return this.isRunning.get();
    }

    /**
     * Whether local node is leader.
     *
     * @return true if this module is active and the node is configured to be leader
     */
    public boolean isLeader() {
        return this.isRunning() && this.isLeader;
    }
}
