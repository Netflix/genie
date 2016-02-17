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
package com.netflix.genie.web.configs;

import com.google.common.collect.Sets;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.tasks.leader.LeadershipTask;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.TaskScheduler;

import java.util.Collection;

/**
 * Unit tests for LeadershipConfig class.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class LeadershipConfigUnitTests {

    /**
     * Make sure can get a valid leadership tasks coordinator.
     */
    @Test
    public void canGetLeadershipTasksCoordinator() {
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Collection<LeadershipTask> tasks = Sets.newHashSet();
        Assert.assertNotNull(new LeadershipConfig().leadershipTasksCoordinator(scheduler, tasks));
    }

    /**
     * Make sure we can get a valid LocalLeader if it's needed.
     */
    @Test
    public void canGetLocalLeader() {
        final ApplicationEventPublisher publisher = Mockito.mock(ApplicationEventPublisher.class);
        Assert.assertNotNull(new LeadershipConfig().localLeader(publisher, true));
    }
}
