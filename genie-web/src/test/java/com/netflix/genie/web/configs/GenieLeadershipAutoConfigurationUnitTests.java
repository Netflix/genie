/*
 *
 *  Copyright 2017 Netflix, Inc.
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
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.properties.LeadershipProperties;
import com.netflix.genie.web.properties.ZookeeperLeadershipProperties;
import com.netflix.genie.web.tasks.leader.LeadershipTask;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.scheduling.TaskScheduler;

import java.util.Collection;

/**
 * Unit tests for the LeadershipConfig class.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Category(UnitTest.class)
public class GenieLeadershipAutoConfigurationUnitTests {

    /**
     * Make sure can get a valid leadership tasks coordinator.
     */
    @Test
    public void canGetLeadershipTasksCoordinator() {
        final TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        final Collection<LeadershipTask> tasks = Sets.newHashSet();
        Assert.assertNotNull(new GenieLeadershipAutoConfiguration().leadershipTasksCoordinator(scheduler, tasks));
    }

    /**
     * Make sure the bean is created successfully.
     */
    @Test
    public void canGetLeaderInitiatorFactoryBean() {
        final CuratorFramework client = Mockito.mock(CuratorFramework.class);
        final ZookeeperLeadershipProperties zookeeperLeadershipProperties = new ZookeeperLeadershipProperties();
        Assert.assertNotNull(
            new GenieLeadershipAutoConfiguration().leaderInitiatorFactory(client, zookeeperLeadershipProperties)
        );
    }

    /**
     * Make sure we can get a valid LocalLeader if it's needed.
     */
    @Test
    public void canGetLocalLeader() {
        final GenieEventBus genieEventBus = Mockito.mock(GenieEventBus.class);
        Assert.assertNotNull(
            new GenieLeadershipAutoConfiguration().localLeader(genieEventBus, new LeadershipProperties())
        );
    }
}
