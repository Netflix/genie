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

import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.properties.LeadershipProperties;
import com.netflix.genie.web.properties.ZookeeperLeadershipProperties;
import com.netflix.genie.web.tasks.leader.LeadershipTask;
import com.netflix.genie.web.tasks.leader.LeadershipTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.zookeeper.config.LeaderInitiatorFactoryBean;
import org.springframework.scheduling.TaskScheduler;

import java.util.Collection;

/**
 * Beans for Leadership of a Genie cluster.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        LeadershipProperties.class,
        ZookeeperLeadershipProperties.class
    }
)
public class GenieLeadershipAutoConfiguration {

    /**
     * Create the LeadershipTasksCoordination bean used to start and stop all leadership related tasks based on
     * whether leadership is granted or revoked.
     *
     * @param taskScheduler The task scheduler to use for scheduling leadership tasks
     * @param tasks         The leadership tasks to schedule
     * @return The leader coordinator
     */
    @Bean
    @ConditionalOnBean(LeadershipTask.class)
    public LeadershipTasksCoordinator leadershipTasksCoordinator(
        final TaskScheduler taskScheduler,
        final Collection<LeadershipTask> tasks
    ) {
        return new LeadershipTasksCoordinator(taskScheduler, tasks);
    }

    /**
     * The leadership initialization factory bean which will create a LeaderInitiator to kick off the leader election
     * process within this node for the cluster if Zookeeper is configured.
     *
     * @param client                        The curator framework client to use
     * @param zookeeperLeadershipProperties The Zookeeper properties to use
     * @return The factory bean
     */
    @Bean
    @ConditionalOnProperty(value = "spring.cloud.zookeeper.enabled", havingValue = "true")
    public LeaderInitiatorFactoryBean leaderInitiatorFactory(
        final CuratorFramework client,
        final ZookeeperLeadershipProperties zookeeperLeadershipProperties
    ) {
        final LeaderInitiatorFactoryBean factoryBean = new LeaderInitiatorFactoryBean();
        factoryBean.setClient(client);
        factoryBean.setPath(zookeeperLeadershipProperties.getPath());
        factoryBean.setRole("cluster");
        return factoryBean;
    }

    /**
     * If Spring Zookeeper Leadership is disabled and this node is forced to be the leader create the local leader
     * bean which will fire appropriate events.
     *
     * @param genieEventBus        The genie event bus implementation to use
     * @param leadershipProperties Properties related to static leadership configuration for the Genie cluster
     * @return The local leader bean
     */
    @Bean
    @ConditionalOnProperty(value = "spring.cloud.zookeeper.enabled", havingValue = "false", matchIfMissing = true)
    public LocalLeader localLeader(
        final GenieEventBus genieEventBus,
        final LeadershipProperties leadershipProperties
    ) {
        return new LocalLeader(genieEventBus, leadershipProperties.isEnabled());
    }
}
