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
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.genie.core.services.impl.LocalFileTransferImpl;
import com.netflix.genie.core.util.ProcessChecker;
import com.netflix.genie.core.util.UnixProcessChecker;
import com.netflix.genie.web.services.impl.HttpFileTransferImpl;
import com.netflix.spectator.api.Registry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
public class JobConfig {
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
    public HttpFileTransferImpl httpFileTransfer(final RestTemplate restTemplate, final Registry registry) {
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
    public WorkflowTask jobKillLogicTask(final Registry registry) {
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
    public WorkflowTask initialSetupTask(final Registry registry) {
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
        final Registry registry,
        @Qualifier("cacheGenieFileTransferService")
        final GenieFileTransferService fts) {
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
        final Registry registry,
        @Qualifier("cacheGenieFileTransferService")
        final GenieFileTransferService fts) {
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
        final Registry registry,
        @Qualifier("cacheGenieFileTransferService")
        final GenieFileTransferService fts) {
        return new CommandTask(registry, fts);
    }

    /**
     * Create an Job Task bean that processes Job information provided by user.
     *
     * @param attachmentService An implementation of the attachment service
     * @param registry          The metrics registry to use
     * @param fts               File transfer implementation
     * @return An job task object
     * @throws GenieException if there is any problem
     */
    @Bean
    @Order(value = 5)
    @Autowired
    public WorkflowTask jobProcessorTask(
        final AttachmentService attachmentService,
        final Registry registry,
        @Qualifier("genieFileTransferService")
        final GenieFileTransferService fts
    ) throws GenieException {
        return new JobTask(attachmentService, registry, fts);
    }

    /**
     * Create an Job Kickoff Task bean that runs the job.
     *
     * @param jobsProperties The various jobs properties
     * @param executor       An instance of an executor
     * @param hostName       Host on which the job will run
     * @param registry       The metrics registry to use
     * @return An application task object
     */
    @Bean
    @Order(value = 6)
    @Autowired
    public WorkflowTask jobKickoffTask(
        final JobsProperties jobsProperties,
        final Executor executor,
        final String hostName,
        final Registry registry
    ) {
        return new JobKickoffTask(
            jobsProperties.getUsers().isRunAsUserEnabled(),
            jobsProperties.getUsers().isCreationEnabled(),
            executor,
            hostName,
            registry
        );
    }

    /**
     * Create a {@link ProcessChecker.Factory} suitable for UNIX systems.
     *
     * @param executor       The executor where checks are executed
     * @param jobsProperties The jobs properties
     * @return a {@link ProcessChecker.Factory}
     */
    @Bean
    @Autowired
    @ConditionalOnMissingBean(ProcessChecker.Factory.class)
    public ProcessChecker.Factory processCheckerFactory(
        final Executor executor,
        final JobsProperties jobsProperties
    ) {
        if (SystemUtils.IS_OS_UNIX) {
            return new UnixProcessChecker.Factory(
                executor,
                jobsProperties.getUsers().isRunAsUserEnabled()
            );
        } else {
            throw new BeanCreationException("No implementation available for non-UNIX systems");
        }
    }
}
