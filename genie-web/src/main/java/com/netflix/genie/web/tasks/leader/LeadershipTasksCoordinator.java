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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.cluster.leader.event.AbstractLeaderEvent;
import org.springframework.cloud.cluster.leader.event.OnGrantedEvent;
import org.springframework.cloud.cluster.leader.event.OnRevokedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import javax.annotation.PreDestroy;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

/**
 * Class which handles coordinating leadership related tasks. Listens for leadership grant and revoke events and starts
 * tasks associated with being the cluster leader.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Slf4j
public class LeadershipTasksCoordinator {

    private final Set<LeadershipTask> tasks;
    private final Set<ScheduledFuture<?>> futures;
    private final TaskScheduler taskScheduler;
    private boolean isRunning;

    /**
     * Constructor.
     *
     * @param taskScheduler The task executor to use.
     * @param tasks         The leadership tasks to run
     */
    public LeadershipTasksCoordinator(final TaskScheduler taskScheduler, final Collection<LeadershipTask> tasks) {
        this.futures = Sets.newHashSet();
        this.taskScheduler = taskScheduler;
        this.isRunning = false;
        this.tasks = Sets.newHashSet();
        if (tasks != null) {
            this.tasks.addAll(tasks);
        }
    }

    /**
     * Make sure any threads are taken care of before this object is destroyed.
     */
    @PreDestroy
    public void preDestroy() {
        this.cancelTasks();
    }

    /**
     * Leadership event listener. Starts and stop processes when this Genie node is elected the leader of the cluster.
     * <p>
     * Synchronized to ensure no race conditions between threads trying to start and stop leadership tasks.
     *
     * @param leaderEvent The leader grant or revoke event
     * @see org.springframework.cloud.cluster.leader.event.OnGrantedEvent
     * @see org.springframework.cloud.cluster.leader.event.OnRevokedEvent
     */
    @EventListener
    public synchronized void onLeaderEvent(final AbstractLeaderEvent leaderEvent) {
        if (leaderEvent instanceof OnGrantedEvent) {
            if (this.isRunning) {
                return;
            }
            log.info("Leadership granted.");
            this.isRunning = true;
            this.tasks.stream().forEach(
                task -> {
                    switch (task.getScheduleType()) {
                        case TRIGGER:
                            final Trigger trigger = task.getTrigger();
                            log.info(
                                "Scheduling leadership task {} to run with trigger {}",
                                task.getClass().getCanonicalName(),
                                trigger
                            );
                            this.futures.add(this.taskScheduler.schedule(task, trigger));
                            break;
                        case FIXED_RATE:
                            final long rate = task.getFixedRate();
                            log.info(
                                "Scheduling leadership task {} to run every {} second(s)",
                                task.getClass().getCanonicalName(),
                                rate / 1000.0
                            );
                            this.futures.add(this.taskScheduler.scheduleAtFixedRate(task, rate));
                            break;
                        case FIXED_DELAY:
                            final long delay = task.getFixedDelay();
                            log.info(
                                "Scheduling leadership task {} to run at a fixed delay of every {} second(s)",
                                task.getClass().getCanonicalName(),
                                delay / 1000.0
                            );
                            this.futures.add(this.taskScheduler.scheduleWithFixedDelay(task, delay));
                            break;
                        default:
                            log.error("Unknown Genie task type {}", task.getScheduleType());
                    }
                }
            );
        } else if (leaderEvent instanceof OnRevokedEvent) {
            if (!this.isRunning) {
                return;
            }
            log.info("Leadership revoked.");
            this.isRunning = false;
            this.cancelTasks();
        } else {
            log.warn("Unknown leadership event {}. Ignoring.", leaderEvent);
        }
    }

    private void cancelTasks() {
        for (final ScheduledFuture<?> future : this.futures) {
            log.info("Attempting to cancel thread {}", future.toString());
            if (future.cancel(true)) {
                log.info("Successfully cancelled.");
            } else {
                log.info("Failed to cancel.");
            }
        }

        // Clear out the tasks
        this.futures.clear();
    }
}
