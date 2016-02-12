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
package com.netflix.genie.web.leader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.cluster.leader.event.AbstractLeaderEvent;
import org.springframework.cloud.cluster.leader.event.OnGrantedEvent;
import org.springframework.cloud.cluster.leader.event.OnRevokedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Class which handles coordinating leadership related tasks. Listens for leadership grant and revoke events and starts
 * tasks associated with being the cluster leader.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Component
@Slf4j
public class LeaderCoordinator {

    /**
     * Leadership event listener. Starts and stop processes when this Genie node is elected the leader of the cluster.
     *
     * @param leaderEvent The leader grant or revoke event
     * @see org.springframework.cloud.cluster.leader.event.OnGrantedEvent
     * @see org.springframework.cloud.cluster.leader.event.OnRevokedEvent
     */
    @EventListener
    public void onLeaderEvent(final AbstractLeaderEvent leaderEvent) {
        if (leaderEvent instanceof OnGrantedEvent) {
            log.info("Leadership granted.");
        } else if (leaderEvent instanceof OnRevokedEvent) {
            log.info("Leadership revoked.");
        } else {
            log.warn("Unknown leadership event {}", leaderEvent);
        }
    }
}
