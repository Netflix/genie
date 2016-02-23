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

import com.netflix.genie.web.tasks.leader.LeadershipTask;
import com.netflix.genie.web.tasks.leader.LeadershipTasksCoordinator;
import com.netflix.genie.web.tasks.leader.LocalLeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.Collection;

/**
 * Configuration of beans for asynchronous tasks within Genie.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Configuration
public class TaskConfig {

    /**
     * Get a task scheduler.
     *
     * @return The task scheduler
     */
    @Bean
    public TaskScheduler taskScheduler() {
        return new ThreadPoolTaskScheduler();
    }

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
     * If Spring Cloud Leadership is disabled and this node is forced to be the leader create the local leader
     * bean which will fire appropriate events.
     *
     * @param publisher The application event publisher to use
     * @param isLeader  Whether this node is the leader of the cluster or not
     * @return The local leader bean
     */
    @Bean
    @ConditionalOnProperty(value = "spring.cloud.cluster.leader.enabled", havingValue = "false")
    public LocalLeader localLeader(
        final ApplicationEventPublisher publisher,
        @Value("${genie.leader.enabled}") final boolean isLeader
    ) {
        return new LocalLeader(publisher, isLeader);
    }
}
