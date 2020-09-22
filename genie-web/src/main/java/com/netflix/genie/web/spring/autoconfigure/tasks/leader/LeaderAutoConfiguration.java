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
package com.netflix.genie.web.spring.autoconfigure.tasks.leader;

import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.properties.ArchiveStatusCleanupProperties;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.properties.LeadershipProperties;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.services.ClusterLeaderService;
import com.netflix.genie.web.services.impl.ClusterLeaderServiceCuratorImpl;
import com.netflix.genie.web.services.impl.ClusterLeaderServiceLocalLeaderImpl;
import com.netflix.genie.web.spring.actuators.LeaderElectionActuator;
import com.netflix.genie.web.spring.autoconfigure.ZookeeperAutoConfiguration;
import com.netflix.genie.web.spring.autoconfigure.tasks.TasksAutoConfiguration;
import com.netflix.genie.web.tasks.leader.AgentJobCleanupTask;
import com.netflix.genie.web.tasks.leader.ArchiveStatusCleanupTask;
import com.netflix.genie.web.tasks.leader.DatabaseCleanupTask;
import com.netflix.genie.web.tasks.leader.LeaderTask;
import com.netflix.genie.web.tasks.leader.LeaderTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import com.netflix.genie.web.tasks.leader.UserMetricsTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.integration.zookeeper.leader.LeaderInitiator;
import org.springframework.scheduling.TaskScheduler;

import java.util.Set;

/**
 * Beans for Leadership of a Genie cluster.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        AgentCleanupProperties.class,
        ArchiveStatusCleanupProperties.class,
        DatabaseCleanupProperties.class,
        LeadershipProperties.class,
        UserMetricsProperties.class,
    }
)
@AutoConfigureAfter(
    {
        TasksAutoConfiguration.class,
        ZookeeperAutoConfiguration.class
    }
)
public class LeaderAutoConfiguration {

    /**
     * Create the LeadershipTasksCoordination bean used to start and stop all leadership related tasks based on
     * whether leadership is granted or revoked.
     *
     * @param taskScheduler The task scheduler to use for scheduling leadership tasks
     * @param tasks         The leadership tasks to schedule
     * @return The leader coordinator
     */
    @Bean
    @ConditionalOnMissingBean(LeaderTasksCoordinator.class)
    public LeaderTasksCoordinator leaderTasksCoordinator(
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        final Set<LeaderTask> tasks
    ) {
        return new LeaderTasksCoordinator(taskScheduler, tasks);
    }

    /**
     * Create a {@link DatabaseCleanupTask} if one is required.
     *
     * @param cleanupProperties The properties to use to configure this task
     * @param environment       The application {@link Environment} to pull properties from
     * @param dataServices      The {@link DataServices} encapsulation instance to use
     * @param registry          The metrics registry
     * @return The {@link DatabaseCleanupTask} instance to use if the conditions match
     */
    @Bean
    @ConditionalOnProperty(value = DatabaseCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(DatabaseCleanupTask.class)
    public DatabaseCleanupTask databaseCleanupTask(
        final DatabaseCleanupProperties cleanupProperties,
        final Environment environment,
        final DataServices dataServices,
        final MeterRegistry registry
    ) {
        return new DatabaseCleanupTask(
            cleanupProperties,
            environment,
            dataServices,
            registry
        );
    }

    /**
     * If required get a {@link UserMetricsTask} instance for use.
     *
     * @param registry              The metrics registry
     * @param dataServices          The {@link DataServices} instance to use
     * @param userMetricsProperties The properties
     * @return The {@link UserMetricsTask} instance
     */
    @Bean
    @ConditionalOnProperty(value = UserMetricsProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(UserMetricsTask.class)
    public UserMetricsTask userMetricsTask(
        final MeterRegistry registry,
        final DataServices dataServices,
        final UserMetricsProperties userMetricsProperties
    ) {
        return new UserMetricsTask(
            registry,
            dataServices,
            userMetricsProperties
        );
    }

    /**
     * If required, get a {@link AgentJobCleanupTask} instance for use.
     *
     * @param dataServices           The {@link DataServices} encapsulation instance to use
     * @param agentCleanupProperties the agent cleanup properties
     * @param registry               the metrics registry
     * @param agentRoutingService    the agent routing service
     * @return a {@link AgentJobCleanupTask}
     */
    @Bean
    @ConditionalOnProperty(value = AgentCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(AgentJobCleanupTask.class)
    public AgentJobCleanupTask agentJobCleanupTask(
        final DataServices dataServices,
        final AgentCleanupProperties agentCleanupProperties,
        final MeterRegistry registry,
        final AgentRoutingService agentRoutingService
    ) {
        return new AgentJobCleanupTask(
            dataServices,
            agentCleanupProperties,
            registry,
            agentRoutingService
        );
    }

    /**
     * If required, get a {@link ArchiveStatusCleanupTask} instance for use.
     *
     * @param dataServices                   The {@link DataServices} encapsulation instance to use
     * @param agentRoutingService            the agent routing service
     * @param archiveStatusCleanupProperties the archive status cleanup properties
     * @param registry                       the metrics registry
     * @return a {@link AgentJobCleanupTask}
     */
    @Bean
    @ConditionalOnProperty(value = ArchiveStatusCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(ArchiveStatusCleanupTask.class)
    public ArchiveStatusCleanupTask archiveStatusCleanupTask(
        final DataServices dataServices,
        final AgentRoutingService agentRoutingService,
        final ArchiveStatusCleanupProperties archiveStatusCleanupProperties,
        final MeterRegistry registry
    ) {
        return new ArchiveStatusCleanupTask(
            dataServices,
            agentRoutingService,
            archiveStatusCleanupProperties,
            registry
        );
    }

    /**
     * Create a {@link ClusterLeaderService} based on Zookeeper/Curator if {@link LeaderInitiator} is
     * available and the bean does not already exist.
     *
     * @param leaderInitiator the Spring Zookeeper/Curator based leader election component
     * @return a {@link ClusterLeaderService}
     */
    @Bean
    @ConditionalOnBean(LeaderInitiator.class)
    @ConditionalOnMissingBean(ClusterLeaderService.class)
    public ClusterLeaderService curatorClusterLeaderService(final LeaderInitiator leaderInitiator) {
        return new ClusterLeaderServiceCuratorImpl(leaderInitiator);
    }

    /**
     * If Zookeeper isn't available and this node is forced to be the leader create the local leader
     * bean which will fire appropriate events.
     *
     * @param genieEventBus        The genie event bus implementation to use
     * @param leadershipProperties Properties related to static leadership configuration for the Genie cluster
     * @return The local leader bean
     */
    @Bean
    @ConditionalOnMissingBean(
        {
            LeaderInitiator.class,
            LocalLeader.class
        }
    )
    public LocalLeader localLeader(
        final GenieEventBus genieEventBus,
        final LeadershipProperties leadershipProperties
    ) {
        return new LocalLeader(genieEventBus, leadershipProperties.isEnabled());
    }

    /**
     * Create a {@link ClusterLeaderService} based on static configuration if {@link LocalLeader} is
     * available and the bean does not already exist.
     *
     * @param localLeader the configuration-based leader election component
     * @return a {@link ClusterLeaderService}
     */
    @Bean
    @ConditionalOnBean(LocalLeader.class)
    @ConditionalOnMissingBean(ClusterLeaderService.class)
    public ClusterLeaderService localClusterLeaderService(final LocalLeader localLeader) {
        return new ClusterLeaderServiceLocalLeaderImpl(localLeader);
    }

    /**
     * Create a {@link LeaderElectionActuator} bean if one is not already defined and if
     * {@link ClusterLeaderService} is available. This bean is an endpoint that gets registered in Spring Actuator.
     *
     * @param clusterLeaderService the cluster leader service
     * @return a {@link LeaderElectionActuator}
     */
    @Bean
    @ConditionalOnBean(ClusterLeaderService.class)
    @ConditionalOnMissingBean(LeaderElectionActuator.class)
    public LeaderElectionActuator leaderElectionActuator(final ClusterLeaderService clusterLeaderService) {
        return new LeaderElectionActuator(clusterLeaderService);
    }
}
