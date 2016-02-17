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
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.cloud.cluster.leader.Context;
import org.springframework.cloud.cluster.leader.event.AbstractLeaderEvent;
import org.springframework.cloud.cluster.leader.event.OnGrantedEvent;
import org.springframework.cloud.cluster.leader.event.OnRevokedEvent;
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
@Category(UnitTest.class)
public class LeadershipTasksCoordinatorUnitTests {

    private LeadershipTasksCoordinator coordinator;
    private TaskScheduler scheduler;
    private LeadershipTask task1;
    private LeadershipTask task2;
    private LeadershipTask task3;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.scheduler = Mockito.mock(TaskScheduler.class);
        this.task1 = Mockito.mock(LeadershipTask.class);
        Mockito.when(this.task1.getTrigger()).thenReturn(null);
        this.task2 = Mockito.mock(LeadershipTask.class);
        Mockito.when(this.task2.getTrigger()).thenReturn(null);
        this.task3 = Mockito.mock(LeadershipTask.class);
        Mockito.when(this.task3.getTrigger()).thenReturn(null);
        final Set<LeadershipTask> tasks = Sets.newHashSet(this.task1, this.task2, this.task3);
        this.coordinator = new LeadershipTasksCoordinator(this.scheduler, tasks);
    }

    /**
     * Make sure all leadership activities are started when leadership is granted.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStartLeadershipTasks() {
        final long task1Period = 13238;
        Mockito.when(this.task1.getPeriod()).thenReturn(task1Period);
        final long task2Period = 3891082;
        Mockito.when(this.task2.getPeriod()).thenReturn(task2Period);
        final Trigger task3Trigger = Mockito.mock(Trigger.class);
        Mockito.when(this.task3.getTrigger()).thenReturn(task3Trigger);

        final OnGrantedEvent event = new OnGrantedEvent(this, null);

        this.coordinator.onLeaderEvent(event);

        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task1, task1Period);
        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task2, task2Period);
        Mockito.verify(this.task3, Mockito.never()).getPeriod();
        Mockito.verify(this.scheduler, Mockito.times(1)).schedule(this.task3, task3Trigger);

        //Make sure a second OnGrantedEvent doesn't do anything if it's already running
        this.coordinator.onLeaderEvent(event);

        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task1, task1Period);
        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task2, task2Period);
        Mockito.verify(this.scheduler, Mockito.times(1)).schedule(this.task3, task3Trigger);
    }

    /**
     * Make sure all leadership activities are stopped when leadership is revoked.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void canStopLeadershipTasks() {
        final Trigger task1Trigger = Mockito.mock(Trigger.class);
        Mockito.when(this.task1.getTrigger()).thenReturn(task1Trigger);
        final long task2Period = -2;
        Mockito.when(this.task2.getPeriod()).thenReturn(task2Period);
        final long task3Period = 135234;
        Mockito.when(this.task3.getPeriod()).thenReturn(task3Period);

        final ScheduledFuture future1 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future1.cancel(true)).thenReturn(true);
        Mockito.when(this.scheduler.schedule(this.task1, task1Trigger)).thenReturn(future1);

        final ScheduledFuture future3 = Mockito.mock(ScheduledFuture.class);
        Mockito.when(future3.cancel(true)).thenReturn(false);
        Mockito.when(this.scheduler.scheduleAtFixedRate(this.task3, task3Period)).thenReturn(future3);

        final OnGrantedEvent grantedEvent = new OnGrantedEvent(this, null);

        this.coordinator.onLeaderEvent(grantedEvent);

        Mockito.verify(this.scheduler, Mockito.times(1)).schedule(this.task1, task1Trigger);
        Mockito.verify(this.scheduler, Mockito.never())
            .scheduleAtFixedRate(Mockito.eq(this.task2), Mockito.anyLong());
        Mockito.verify(this.scheduler, Mockito.times(1)).scheduleAtFixedRate(this.task3, task3Period);

        // Should now be running

        final OnRevokedEvent revokedEvent = new OnRevokedEvent(this, null);

        this.coordinator.onLeaderEvent(revokedEvent);

        Mockito.verify(future1, Mockito.times(1)).cancel(true);

        // Call again to make sure nothing is invoked even though they were cancelled
        this.coordinator.onLeaderEvent(revokedEvent);

        Mockito.verify(future1, Mockito.times(1)).cancel(true);
    }

    /**
     * Make sure unhandled commands are ignored.
     */
    @Test
    public void doesIgnoreUnknownEvent() {
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
