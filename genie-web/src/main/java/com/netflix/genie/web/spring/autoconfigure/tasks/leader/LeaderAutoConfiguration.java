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

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.FilePersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.data.services.TagPersistenceService;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.properties.AgentCleanupProperties;
import com.netflix.genie.web.properties.ClusterCheckerProperties;
import com.netflix.genie.web.properties.DatabaseCleanupProperties;
import com.netflix.genie.web.properties.LeadershipProperties;
import com.netflix.genie.web.properties.UserMetricsProperties;
import com.netflix.genie.web.properties.ZookeeperLeadershipProperties;
import com.netflix.genie.web.spring.autoconfigure.tasks.TasksAutoConfiguration;
import com.netflix.genie.web.tasks.leader.AgentJobCleanupTask;
import com.netflix.genie.web.tasks.leader.ClusterCheckerTask;
import com.netflix.genie.web.tasks.leader.DatabaseCleanupTask;
import com.netflix.genie.web.tasks.leader.LeadershipTask;
import com.netflix.genie.web.tasks.leader.LeadershipTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import com.netflix.genie.web.tasks.leader.UserMetricsTask;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.autoconfigure.endpoint.web.WebEndpointProperties;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.zookeeper.ZookeeperAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.zookeeper.config.LeaderInitiatorFactoryBean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.client.RestTemplate;

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
        ClusterCheckerProperties.class,
        DatabaseCleanupProperties.class,
        LeadershipProperties.class,
        UserMetricsProperties.class,
        ZookeeperLeadershipProperties.class
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
    @ConditionalOnMissingBean(LeadershipTasksCoordinator.class)
    public LeadershipTasksCoordinator leadershipTasksCoordinator(
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        final Set<LeadershipTask> tasks
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
    @ConditionalOnBean(CuratorFramework.class)
    @ConditionalOnMissingBean(LeaderInitiatorFactoryBean.class)
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
            CuratorFramework.class,
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
     * Create a {@link ClusterCheckerTask} if one hasn't been supplied.
     *
     * @param genieHostInfo         Information about the host this Genie process is running on
     * @param properties            The properties to use to configure the task
     * @param jobSearchService      The job search service to use
     * @param jobPersistenceService The job persistence service to use
     * @param restTemplate          The rest template for http calls
     * @param webEndpointProperties The properties where Spring actuator is running
     * @param registry              The spectator registry for getting metrics
     * @return The {@link ClusterCheckerTask} instance
     */
    @Bean
    @ConditionalOnMissingBean(ClusterCheckerTask.class)
    public ClusterCheckerTask clusterCheckerTask(
        final GenieHostInfo genieHostInfo,
        final ClusterCheckerProperties properties,
        final JobSearchService jobSearchService,
        final JobPersistenceService jobPersistenceService,
        @Qualifier("genieRestTemplate") final RestTemplate restTemplate,
        final WebEndpointProperties webEndpointProperties,
        final MeterRegistry registry
    ) {
        return new ClusterCheckerTask(
            genieHostInfo,
            properties,
            jobSearchService,
            jobPersistenceService,
            restTemplate,
            webEndpointProperties,
            registry
        );
    }

    /**
     * Create a {@link DatabaseCleanupTask} if one is required.
     *
     * @param cleanupProperties         The properties to use to configure this task
     * @param jobPersistenceService     The persistence service to use to cleanup the data store
     * @param clusterPersistenceService The cluster service to use to delete terminated clusters
     * @param filePersistenceService    The file service to use to delete unused file references
     * @param tagPersistenceService     The tag service to use to delete unused tag references
     * @param registry                  The metrics registry
     * @return The {@link DatabaseCleanupTask} instance to use if the conditions match
     */
    @Bean
    @ConditionalOnProperty(value = DatabaseCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(DatabaseCleanupTask.class)
    public DatabaseCleanupTask databaseCleanupTask(
        final DatabaseCleanupProperties cleanupProperties,
        final JobPersistenceService jobPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final FilePersistenceService filePersistenceService,
        final TagPersistenceService tagPersistenceService,
        final MeterRegistry registry
    ) {
        return new DatabaseCleanupTask(
            cleanupProperties,
            jobPersistenceService,
            clusterPersistenceService,
            filePersistenceService,
            tagPersistenceService,
            registry
        );
    }

    /**
     * If required get a {@link UserMetricsTask} instance for use.
     *
     * @param registry              The metrics registry
     * @param jobSearchService      The job search service
     * @param userMetricsProperties The properties
     * @return The {@link UserMetricsTask} instance
     */
    @Bean
    @ConditionalOnProperty(value = UserMetricsProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(UserMetricsTask.class)
    public UserMetricsTask userMetricsTask(
        final MeterRegistry registry,
        final JobSearchService jobSearchService,
        final UserMetricsProperties userMetricsProperties
    ) {
        return new UserMetricsTask(
            registry,
            jobSearchService,
            userMetricsProperties
        );
    }

    /**
     * If required, get a {@link AgentJobCleanupTask} instance for use.
     *
     * @param jobSearchService       The job search service
     * @param jobPersistenceService  the job persistence service
     * @param agentCleanupProperties the agent cleanup properties
     * @param registry               the metrics registry
     * @return a {@link AgentJobCleanupTask}
     */
    @Bean
    @ConditionalOnProperty(value = AgentCleanupProperties.ENABLED_PROPERTY, havingValue = "true")
    @ConditionalOnMissingBean(AgentJobCleanupTask.class)
    public AgentJobCleanupTask agentJobCleanupTask(
        final JobSearchService jobSearchService,
        final JobPersistenceService jobPersistenceService,
        final AgentCleanupProperties agentCleanupProperties,
        final MeterRegistry registry
    ) {
        return new AgentJobCleanupTask(
            jobSearchService,
            jobPersistenceService,
            agentCleanupProperties,
            registry
        );
    }
}
