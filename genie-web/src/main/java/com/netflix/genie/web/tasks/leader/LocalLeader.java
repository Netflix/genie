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

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.integration.leader.event.OnRevokedEvent;

/**
 * A class to control leadership activities when remote leadership isn't enabled and this node has been forcibly
 * elected as the leader.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LocalLeader {

    private final ApplicationEventPublisher publisher;
    private final boolean isLeader;

    /**
     * Constructor.
     *
     * @param publisher The spring application event publisher to use to invoke that this node is a leader
     * @param isLeader  Whether this node should be the leader or not. Should only be on in a cluster but not enforced
     */
    public LocalLeader(final ApplicationEventPublisher publisher, final boolean isLeader) {
        this.publisher = publisher;
        this.isLeader = isLeader;
        if (this.isLeader) {
            log.info("Constructing LocalLeader. This node IS the leader.");
        } else {
            log.info("Constructing LocalLeader. This node IS NOT the leader.");
        }
    }

    /**
     * Event listener for when a context is started up.
     *
     * @param event The Spring Boot application ready event to startup on
     */
    @EventListener
    public void startLeadership(final ContextRefreshedEvent event) {
        if (this.isLeader) {
            log.debug("Starting Leadership due to {}", event);
            this.publisher.publishEvent(new OnGrantedEvent(this, null, "leader"));
        }
    }

    /**
     * Before the application shuts down need to turn off leadership activities.
     *
     * @param event The application context closing event
     */
    @EventListener
    public void stopLeadership(final ContextClosedEvent event) {
        if (this.isLeader) {
            log.debug("Stopping Leadership due to {}", event);
            this.publisher.publishEvent(new OnRevokedEvent(this, null, "leader"));
        }
    }
}
