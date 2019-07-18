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
package com.netflix.genie.web.spring.autoconfigure.jobs;

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.jobs.workflow.impl.ApplicationTask;
import com.netflix.genie.web.jobs.workflow.impl.ClusterTask;
import com.netflix.genie.web.jobs.workflow.impl.CommandTask;
import com.netflix.genie.web.jobs.workflow.impl.InitialSetupTask;
import com.netflix.genie.web.jobs.workflow.impl.JobFailureAndKillHandlerLogicTask;
import com.netflix.genie.web.jobs.workflow.impl.JobKickoffTask;
import com.netflix.genie.web.jobs.workflow.impl.JobTask;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.S3FileTransferProperties;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.HttpFileTransferImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.services.impl.S3FileTransferImpl;
import com.netflix.genie.web.util.ProcessChecker;
import com.netflix.genie.web.util.UnixProcessChecker;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
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
@EnableConfigurationProperties(
    {
        S3FileTransferProperties.class
    }
)
// TODO: This is going to go away once the V4 API is in place
public class JobsAutoConfiguration {
    /**
     * Bean to create a local file transfer object.
     *
     * @return A unix copy implementation of the FileTransferService.
     */
    @Bean(name = {"file.system.file", "file.system.null"})
    @Order(value = 2)
    @ConditionalOnMissingBean(LocalFileTransferImpl.class)
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
    @ConditionalOnMissingBean(HttpFileTransferImpl.class)
    public HttpFileTransferImpl httpFileTransfer(final RestTemplate restTemplate, final MeterRegistry registry) {
        return new HttpFileTransferImpl(restTemplate, registry);
    }

    /**
     * Returns a bean which has an s3 implementation of the File Transfer interface.
     *
     * @param s3ClientFactory          S3 client factory to use
     * @param registry                 The metrics registry to use
     * @param s3FileTransferProperties Configuration properties
     * @return An s3 implementation of the FileTransfer interface
     */
    @Bean(name = {"file.system.s3", "file.system.s3n", "file.system.s3a"})
    @Order(value = 1)
    @ConditionalOnMissingBean(S3FileTransferImpl.class)
    @ConditionalOnBean(S3ClientFactory.class)
    public S3FileTransferImpl s3FileTransferImpl(
        final S3ClientFactory s3ClientFactory,
        final MeterRegistry registry,
        final S3FileTransferProperties s3FileTransferProperties
    ) {
        return new S3FileTransferImpl(s3ClientFactory, registry, s3FileTransferProperties);
    }

    /**
     * Create a task that adds logic to handle kill requests to a job.
     *
     * @param registry The metrics registry to use
     * @return A jobKillLogicTask bean.
     */
    @Bean
    @Order(value = 0)
    @ConditionalOnMissingBean(JobFailureAndKillHandlerLogicTask.class)
    public JobFailureAndKillHandlerLogicTask jobKillLogicTask(final MeterRegistry registry) {
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
    @ConditionalOnMissingBean(InitialSetupTask.class)
    public InitialSetupTask initialSetupTask(final MeterRegistry registry) {
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
    @ConditionalOnMissingBean(ClusterTask.class)
    public ClusterTask clusterProcessorTask(
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
    @ConditionalOnMissingBean(ApplicationTask.class)
    public ApplicationTask applicationProcessorTask(
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
    @ConditionalOnMissingBean(CommandTask.class)
    public CommandTask commandProcessorTask(
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
    @ConditionalOnMissingBean(JobTask.class)
    public JobTask jobProcessorTask(
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
    @ConditionalOnMissingBean(JobKickoffTask.class)
    public JobKickoffTask jobKickoffTask(
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

    /**
     * Create a {@link ProcessChecker.Factory} suitable for UNIX systems.
     *
     * @param executor       The executor where checks are executed
     * @param jobsProperties The jobs properties
     * @return a {@link ProcessChecker.Factory}
     */
    @Bean
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
