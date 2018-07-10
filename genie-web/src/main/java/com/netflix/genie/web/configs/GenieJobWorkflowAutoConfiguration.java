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
package com.netflix.genie.web.configs;

import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
import com.netflix.genie.web.jobs.workflow.impl.ApplicationTask;
import com.netflix.genie.web.jobs.workflow.impl.ClusterTask;
import com.netflix.genie.web.jobs.workflow.impl.CommandTask;
import com.netflix.genie.web.jobs.workflow.impl.InitialSetupTask;
import com.netflix.genie.web.jobs.workflow.impl.JobFailureAndKillHandlerLogicTask;
import com.netflix.genie.web.jobs.workflow.impl.JobKickoffTask;
import com.netflix.genie.web.jobs.workflow.impl.JobTask;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.HttpFileTransferImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration for Jobs Setup and Run.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
// TODO: This is going to go away once the V4 API is in place
public class GenieJobWorkflowAutoConfiguration {
    /**
     * Bean to create a local file transfer object.
     *
     * @return A unix copy implementation of the FileTransferService.
     */
    @Bean(name = {"file.system.file", "file.system.null"})
    @Order(value = 2)
    public LocalFileTransferImpl localFileTransfer() {
        return new LocalFileTransferImpl();
    }

    /**
     * Bean to create a http[s] file transfer object.
     *
     * @param restTemplate The rest template to use
     * @param registry     The registry to use for metrics
     * @return A http implementation of the FileTransferService.
     */
    @Bean(name = {"file.system.http", "file.system.https"})
    @Order(value = 3)
    public HttpFileTransferImpl httpFileTransfer(final RestTemplate restTemplate, final MeterRegistry registry) {
        return new HttpFileTransferImpl(restTemplate, registry);
    }


    /**
     * Create a task that adds logic to handle kill requests to a job.
     *
     * @param registry The metrics registry to use
     * @return A jobKillLogicTask bean.
     */
    @Bean
    @Order(value = 0)
    public WorkflowTask jobKillLogicTask(final MeterRegistry registry) {
        return new JobFailureAndKillHandlerLogicTask(registry);
    }

    /**
     * Create an setup Task bean that does initial setup before any of the tasks start.
     *
     * @param registry The metrics registry to use
     * @return An initial setup task object
     */
    @Bean
    @Order(value = 1)
    public WorkflowTask initialSetupTask(final MeterRegistry registry) {
        return new InitialSetupTask(registry);
    }

    /**
     * Create an Cluster Task bean that processes the cluster needed for a job.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer implementation
     * @return An cluster task object
     */
    @Bean
    @Order(value = 2)
    public WorkflowTask clusterProcessorTask(
        final MeterRegistry registry,
        @Qualifier("cacheGenieFileTransferService") final GenieFileTransferService fts
    ) {
        return new ClusterTask(registry, fts);
    }

    /**
     * Create an Application Task bean that processes all Applications needed for a job.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer implementation
     * @return An application task object
     */
    @Bean
    @Order(value = 3)
    public WorkflowTask applicationProcessorTask(
        final MeterRegistry registry,
        @Qualifier("cacheGenieFileTransferService") final GenieFileTransferService fts
    ) {
        return new ApplicationTask(registry, fts);
    }

    /**
     * Create an Command Task bean that processes the command needed for a job.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer implementation
     * @return An command task object
     */
    @Bean
    @Order(value = 4)
    public WorkflowTask commandProcessorTask(
        final MeterRegistry registry,
        @Qualifier("cacheGenieFileTransferService") final GenieFileTransferService fts
    ) {
        return new CommandTask(registry, fts);
    }

    /**
     * Create an Job Task bean that processes Job information provided by user.
     *
     * @param attachmentService An implementation of the attachment service
     * @param registry          The metrics registry to use
     * @param fts               File transfer implementation
     * @return An job task object
     */
    @Bean
    @Order(value = 5)
    public WorkflowTask jobProcessorTask(
        final AttachmentService attachmentService,
        final MeterRegistry registry,
        @Qualifier("genieFileTransferService") final GenieFileTransferService fts
    ) {
        return new JobTask(attachmentService, registry, fts);
    }

    /**
     * Create an Job Kickoff Task bean that runs the job.
     *
     * @param jobsProperties The various jobs properties
     * @param executor       An instance of an executor
     * @param genieHostInfo  Info about the host Genie is running on
     * @param registry       The metrics registry to use
     * @return An application task object
     */
    @Bean
    @Order(value = 6)
    public WorkflowTask jobKickoffTask(
        final JobsProperties jobsProperties,
        final Executor executor,
        final GenieHostInfo genieHostInfo,
        final MeterRegistry registry
    ) {
        return new JobKickoffTask(
            jobsProperties.getUsers().isRunAsUserEnabled(),
            jobsProperties.getUsers().isCreationEnabled(),
            executor,
            genieHostInfo.getHostname(),
            registry
        );
    }
}
