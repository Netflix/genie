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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.jobs.workflow.impl.ApplicationTask;
import com.netflix.genie.core.jobs.workflow.impl.ClusterTask;
import com.netflix.genie.core.jobs.workflow.impl.CommandTask;
import com.netflix.genie.core.jobs.workflow.impl.InitialSetupTask;
import com.netflix.genie.core.jobs.workflow.impl.JobFailureAndKillHandlerLogicTask;
import com.netflix.genie.core.jobs.workflow.impl.JobKickoffTask;
import com.netflix.genie.core.jobs.workflow.impl.JobTask;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.FileTransfer;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.genie.core.services.impl.LocalFileTransferImpl;
import com.netflix.spectator.api.Registry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Autowired;
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
public class JobConfigTest {
    /**
     * Bean to create a local file transfer object.
     *
     * @return A unix copy implementation of the FileTransferService.
     */
    @Bean
    @Order(value = 2)
    public FileTransfer localFileTransfer() {
        return new LocalFileTransferImpl();
    }

    /**
     * Create a task that adds logic to handle kill requests to a job.
     *
     * @param registry The metrics registry to use
     * @return A jobKillLogicTask bean
     */
    @Bean
    @Order(value = 0)
    public WorkflowTask jobKillLogicTask(final Registry registry) {
        return new JobFailureAndKillHandlerLogicTask(registry);
    }

    /**
     * Create an setup Task bean that does initial setup before any of the tasks start.
     *
     * @param registry The metrics registry to use
     * @return An application task object
     */
    @Bean
    @Order(value = 1)
    public WorkflowTask initialSetupTask(final Registry registry) {
        return new InitialSetupTask(registry);
    }

    /**
     * Create an Cluster Task bean that processes the cluster needed for a job.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer service
     * @return An application task object
     */
    @Bean
    @Order(value = 2)
    public WorkflowTask clusterProcessorTask(
        final Registry registry,
        final GenieFileTransferService fts) {
        return new ClusterTask(registry, fts);
    }

    /**
     * Create an Application Task bean that processes all Applications needed for a job.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer service
     * @return An application task object
     */
    @Bean
    @Order(value = 3)
    public WorkflowTask applicationProcessorTask(
        final Registry registry,
        final GenieFileTransferService fts) {
        return new ApplicationTask(registry, fts);
    }

    /**
     * Create an Command Task bean that processes the command needed for a job.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer service
     * @return An application task object
     */
    @Bean
    @Order(value = 4)
    public WorkflowTask commandProcessorTask(
        final Registry registry,
        final GenieFileTransferService fts) {
        return new CommandTask(registry, fts);
    }

    /**
     * Create an Job Task bean that processes Job information provided by user.
     *
     * @param attachmentService An implementation of the attachment service
     * @param registry          The metrics registry to use
     * @param fts               File transfer service
     * @return An job task object
     * @throws GenieException if there is any problem
     */
    @Bean
    @Order(value = 5)
    @Autowired
    public WorkflowTask jobProcessorTask(
        final AttachmentService attachmentService,
        final Registry registry,
        final GenieFileTransferService fts
    ) throws GenieException {
        return new JobTask(attachmentService, registry, fts);
    }

    /**
     * Create an Job Kickoff Task bean that runs the job.
     *
     * @param jobsProperties The jobs properties to use
     * @param executor       An instance of an executor
     * @param hostname       Host on which job will run
     * @param registry       The metrics registry to use
     * @return An application task object
     */
    @Bean
    @Order(value = 6)
    @Autowired
    public WorkflowTask jobKickoffTask(
        final JobsProperties jobsProperties,
        final Executor executor,
        final String hostname,
        final Registry registry
    ) {
        return new JobKickoffTask(
            jobsProperties.getUsers().isRunAsUserEnabled(),
            jobsProperties.getUsers().isCreationEnabled(),
            executor,
            hostname,
            registry
        );
    }
}
