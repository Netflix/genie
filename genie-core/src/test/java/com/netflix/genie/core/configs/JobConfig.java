/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.configs;

/**
 * @author amsharma
 */

import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.jobs.workflow.impl.ApplicationTask;
import com.netflix.genie.core.jobs.workflow.impl.ClusterTask;
import com.netflix.genie.core.jobs.workflow.impl.CommandTask;
import com.netflix.genie.core.jobs.workflow.impl.IntialSetupTask;
import com.netflix.genie.core.jobs.workflow.impl.JobKickoffTask;
import com.netflix.genie.core.jobs.workflow.impl.JobTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Configuration for Jobs Setup and Run.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
public class JobConfig {
    /**
     * Create an setup Task bean that does initial setup before any of the tasks start.
     *
     * @return An application task object
     */
    @Bean
    @Order(value = 1)
    public WorkflowTask initialSetupTask() {
        return new IntialSetupTask();
    }

    /**
     * Create an Application Task bean that processes all Applications needed for a job.
     *
     * @return An application task object
     */
    @Bean
    @Order(value = 2)
    public WorkflowTask applicationProcessorTask() {
        return new ApplicationTask();
    }

    /**
     * Create an Command Task bean that processes the command needed for a job.
     *
     * @return An application task object
     */
    @Bean
    @Order(value = 3)
    public WorkflowTask commandProcessorTask() {
        return new CommandTask();
    }

    /**
     * Create an Cluster Task bean that processes the cluster needed for a job.
     *
     * @return An application task object
     */
    @Bean
    @Order(value = 4)
    public WorkflowTask clusterProcessorTask() {
        return new ClusterTask();
    }

    /**
     * Create an Job Task bean that processes Job information provided by user.
     *
     * @return An application task object
     */
    @Bean
    @Order(value = 5)
    public WorkflowTask jobProcessorTask() {
        return new JobTask();
    }

    /**
     * Create an Job Kickoff Task bean that runs the job.
     *
     * @return An application task object
     */
    @Bean
    @Order(value = 6)
    public WorkflowTask jobKickoffTask() {
        return new JobKickoffTask();
    }
}
