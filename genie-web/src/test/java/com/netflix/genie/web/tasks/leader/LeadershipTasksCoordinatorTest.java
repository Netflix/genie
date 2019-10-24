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
import com.netflix.genie.web.tasks.GenieTaskScheduleType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.integration.leader.Context;
import org.springframework.integration.leader.event.AbstractLeaderEvent;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.integration.leader.event.OnRevokedEvent;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.Trigger;

import java.util.Set;
import java.util.concurrent.ScheduledFuture;

/**
 * Unit tests for LeadershipTasksCoordinator.
 *
 * @author tgianos
 * @since 3.0.0
 */
class LeadershipTasksCoordinatorTest {

    private LeadershipTasksCoordinator coordinator;
    private TaskScheduler scheduler;
    private LeadershipTask task1;
    private LeadershipTask task2;
    private LeadershipTask task3;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        this.scheduler = Mockito.mock(TaskScheduler.class);
        this.task1 = Mockito.mock(LeadershipTask.class);
        this.task2 = Mockito.mock(LeadershipTask.class);
        this.task3 = Mockito.mock(LeadershipTask.class);
        final Set<LeadershipTask> tasks = Sets.newHashSet(this.task1, this.task2, this.task3);
        this.coordinator = new LeadershipTasksCoordinator(this.scheduler, tasks);
    }

    /**
     * Make sure all leadership activities are started when leadership is granted.
     */
    @Test
    void canStartLeadershipTasks() {
        final long task1Period = 13238;
        Mockito.when(this.task1.getScheduleType()).thenReturn(GenieTaskScheduleType.FIXED_RATE);
        Mockito.when(this.task1.getFixedRate()).thenReturn(task1Period);
        final long task2Period = 3891082;
        Mockito.when(this.task2.getScheduleType()).thenReturn(GenieTaskScheduleType.FIXED_DELAY);
        Mockito.when(this.task2.getFixedDelay()).thenReturn(task2Period);
        final Trigger task3Trigger = Mockito.mock(Trigger.class);
        Mockito.when(this.task3.getScheduleType()).thenReturn(GenieTaskScheduleType.TRIGGER);
        Mockito.when(this.task3.getTrigger()).thenReturn(task3Trigger);

        final OnGrantedEvent event = new OnGrantedEvent(this, null, "blah");

        this.coordinator.onLeaderEvent(event);

        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task1, task1Period);
        Mockito.verify(this.task1, Mockito.never()).getFixedDelay();
        Mockito.verify(this.task1, Mockito.never()).getTrigger();
        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleWithFixedDelay(this.task2, task2Period);
        Mockito.verify(this.task2, Mockito.never()).getFixedRate();
        Mockito.verify(this.task2, Mockito.never()).getTrigger();
        Mockito.verify(this.scheduler, Mockito.times(1)).schedule(this.task3, task3Trigger);
        Mockito.verify(this.task3, Mockito.never()).getFixedRate();
        Mockito.verify(this.task3, Mockito.never()).getFixedDelay();

        //Make sure a second OnGrantedEvent doesn't do anything if it's already running
        this.coordinator.onLeaderEvent(event);

        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task1, task1Period);
        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleWithFixedDelay(this.task2, task2Period);
        Mockito.verify(this.scheduler, Mockito.times(1)).schedule(this.task3, task3Trigger);
    }

    /**
     * Make sure all leadership activities are stopped when leadership is revoked.
     */
    @Test
    @SuppressWarnings("unchecked")
    void canStopLeadershipTasks() {
        final long task1Period = 13238;
        Mockito.when(this.task1.getScheduleType()).thenReturn(GenieTaskScheduleType.FIXED_RATE);
        Mockito.when(this.task1.getFixedRate()).thenReturn(task1Period);
        final long task2Period = 3891082;
        Mockito.when(this.task2.getScheduleType()).thenReturn(GenieTaskScheduleType.FIXED_DELAY);
        Mockito.when(this.task2.getFixedDelay()).thenReturn(task2Period);
        final Trigger task3Trigger = Mockito.mock(Trigger.class);
        Mockito.when(this.task3.getScheduleType()).thenReturn(GenieTaskScheduleType.TRIGGER);
        Mockito.when(this.task3.getTrigger()).thenReturn(task3Trigger);

        final ScheduledFuture future1 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future1.cancel(true)).thenReturn(true);
        Mockito.when(this.scheduler.scheduleAtFixedRate(this.task1, task1Period)).thenReturn(future1);

        final ScheduledFuture future2 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future2.cancel(true)).thenReturn(true);
        Mockito.when(this.scheduler.scheduleWithFixedDelay(this.task2, task2Period)).thenReturn(future2);

        final ScheduledFuture future3 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future3.cancel(true)).thenReturn(false);
        Mockito.when(this.scheduler.schedule(this.task3, task3Trigger)).thenReturn(future3);

        final OnGrantedEvent grantedEvent = new OnGrantedEvent(this, null, "blah");

        this.coordinator.onLeaderEvent(grantedEvent);

        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task1, task1Period);
        Mockito.verify(this.task1, Mockito.never()).getFixedDelay();
        Mockito.verify(this.task1, Mockito.never()).getTrigger();
        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleWithFixedDelay(this.task2, task2Period);
        Mockito.verify(this.task2, Mockito.never()).getFixedRate();
        Mockito.verify(this.task2, Mockito.never()).getTrigger();
        Mockito.verify(this.scheduler, Mockito.times(1)).schedule(this.task3, task3Trigger);
        Mockito.verify(this.task3, Mockito.never()).getFixedRate();
        Mockito.verify(this.task3, Mockito.never()).getFixedDelay();

        // Should now be running

        final OnRevokedEvent revokedEvent = new OnRevokedEvent(this, null, "blah");

        this.coordinator.onLeaderEvent(revokedEvent);

        Mockito.verify(future1, Mockito.times(1)).cancel(true);
        Mockito.verify(future2, Mockito.times(1)).cancel(true);
        Mockito.verify(future3, Mockito.times(1)).cancel(true);

        // Call again to make sure nothing is invoked even though they were cancelled
        this.coordinator.onLeaderEvent(revokedEvent);

        Mockito.verify(future1, Mockito.times(1)).cancel(true);
        Mockito.verify(future2, Mockito.times(1)).cancel(true);
        Mockito.verify(future3, Mockito.times(1)).cancel(true);
    }

    /**
     * Make sure unhandled commands are ignored.
     */
    @Test
    void doesIgnoreUnknownEvent() {
        final AbstractLeaderEvent leaderEvent = new AbstractLeaderEvent(this) {
            /**
             * Gets the {@link Context} associated with this event.
             *
             * @return the context
             */
            @Override
            public Context getContext() {
                return new Context() {
                    @Override
                    public boolean isLeader() {
                        return false;
                    }

                    @Override
                    public void yield() {
                    }
                };
            }
        };

        this.coordinator.onLeaderEvent(leaderEvent);
        Mockito.verify(
            this.scheduler, Mockito.never()).scheduleAtFixedRate(Mockito.any(Runnable.class), Mockito.anyLong()
        );
    }
}
