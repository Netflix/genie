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
package com.netflix.genie.web.spring.autoconfigure.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import com.netflix.genie.web.properties.ExponentialBackOffTriggerProperties;
import com.netflix.genie.web.properties.FileCacheProperties;
import com.netflix.genie.web.properties.JobsActiveLimitProperties;
import com.netflix.genie.web.properties.JobsCleanupProperties;
import com.netflix.genie.web.properties.JobsForwardingProperties;
import com.netflix.genie.web.properties.JobsLocationsProperties;
import com.netflix.genie.web.properties.JobsMaxProperties;
import com.netflix.genie.web.properties.JobsMemoryProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.JobsUsersProperties;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobFileService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.services.LegacyAttachmentService;
import com.netflix.genie.web.services.MailService;
import com.netflix.genie.web.services.impl.ArchivedJobServiceImpl;
import com.netflix.genie.web.services.impl.CacheGenieFileTransferService;
import com.netflix.genie.web.services.impl.DiskJobFileServiceImpl;
import com.netflix.genie.web.services.impl.FileSystemAttachmentService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.JobDirectoryServerServiceImpl;
import com.netflix.genie.web.services.impl.JobKillServiceImpl;
import com.netflix.genie.web.services.impl.JobKillServiceV3;
import com.netflix.genie.web.services.impl.JobLaunchServiceImpl;
import com.netflix.genie.web.services.impl.JobResolverServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileSystemAttachmentServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.services.impl.LocalJobRunner;
import com.netflix.genie.web.services.impl.S3AttachmentServiceImpl;
import com.netflix.genie.web.tasks.job.JobCompletionService;
import com.netflix.genie.web.util.ProcessChecker;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.retry.support.RetryTemplate;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Configuration for all the services.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
@EnableConfigurationProperties(
    {
        FileCacheProperties.class,
        JobsCleanupProperties.class,
        JobsForwardingProperties.class,
        JobsLocationsProperties.class,
        JobsMaxProperties.class,
        JobsMemoryProperties.class,
        JobsUsersProperties.class,
        ExponentialBackOffTriggerProperties.class,
        JobsActiveLimitProperties.class,
        AttachmentServiceProperties.class
    }
)
@Slf4j
public class ServicesAutoConfiguration {

    /**
     * Collection of properties related to job execution.
     *
     * @param cleanup                cleanup properties
     * @param forwarding             forwarding properties
     * @param locations              locations properties
     * @param max                    max properties
     * @param memory                 memory properties
     * @param users                  users properties
     * @param completionCheckBackOff completion back-off properties
     * @param activeLimit            active limit properties
     * @return a {@code JobsProperties} instance
     */
    @Bean
    public JobsProperties jobsProperties(
        final JobsCleanupProperties cleanup,
        final JobsForwardingProperties forwarding,
        final JobsLocationsProperties locations,
        final JobsMaxProperties max,
        final JobsMemoryProperties memory,
        final JobsUsersProperties users,
        final ExponentialBackOffTriggerProperties completionCheckBackOff,
        final JobsActiveLimitProperties activeLimit
    ) {
        return new JobsProperties(
            cleanup,
            forwarding,
            locations,
            max,
            memory,
            users,
            completionCheckBackOff,
            activeLimit
        );
    }

    /**
     * Get an local implementation of the JobKillService.
     *
     * @param genieHostInfo         Information about the host the Genie process is running on
     * @param dataServices          The {@link DataServices} instance to use
     * @param executor              The executor to use to run system processes.
     * @param jobsProperties        The jobs properties to use
     * @param genieEventBus         The application event bus to use to publish system wide events
     * @param genieWorkingDir       Working directory for genie where it creates jobs directories.
     * @param objectMapper          The Jackson ObjectMapper used to serialize from/to JSON
     * @param processCheckerFactory The process checker factory
     * @return A job kill service instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobKillServiceV3.class)
    public JobKillServiceV3 jobKillServiceV3(
        final GenieHostInfo genieHostInfo,
        final DataServices dataServices,
        final Executor executor,
        final JobsProperties jobsProperties,
        final GenieEventBus genieEventBus,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final ObjectMapper objectMapper,
        final ProcessChecker.Factory processCheckerFactory
    ) {
        return new JobKillServiceV3(
            genieHostInfo.getHostname(),
            dataServices,
            executor,
            jobsProperties.getUsers().isRunAsUserEnabled(),
            genieEventBus,
            genieWorkingDir,
            objectMapper,
            processCheckerFactory
        );
    }

    /**
     * Get an local implementation of the JobKillService.
     *
     * @param jobKillServiceV3 Service to kill V3 jobs.
     * @param jobKillServiceV4 Service to kill V4 jobs.
     * @param dataServices     The {@link DataServices} instance to use
     * @return A job kill service instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobKillService.class)
    public JobKillServiceImpl jobKillService(
        final JobKillServiceV3 jobKillServiceV3,
        final JobKillServiceV4 jobKillServiceV4,
        final DataServices dataServices
    ) {
        return new JobKillServiceImpl(
            jobKillServiceV3,
            jobKillServiceV4,
            dataServices
        );
    }

    /**
     * Get an instance of the Genie File Transfer service.
     *
     * @param fileTransferFactory file transfer implementation factory
     * @return A singleton for GenieFileTransferService
     * @throws GenieException If there is any problem
     */
    @Bean
    @ConditionalOnMissingBean(name = "genieFileTransferService")
    public GenieFileTransferService genieFileTransferService(
        final FileTransferFactory fileTransferFactory
    ) throws GenieException {
        return new GenieFileTransferService(fileTransferFactory);
    }

    /**
     * Get an instance of the Cache Genie File Transfer service.
     *
     * @param fileTransferFactory file transfer implementation factory
     * @param fileCacheProperties Properties related to the file cache that can be set by the admin
     * @param localFileTransfer   local file transfer service
     * @param registry            Registry
     * @return A singleton for GenieFileTransferService
     * @throws GenieException If there is any problem
     */
    @Bean
    @ConditionalOnMissingBean(name = "cacheGenieFileTransferService")
    public GenieFileTransferService cacheGenieFileTransferService(
        final FileTransferFactory fileTransferFactory,
        final FileCacheProperties fileCacheProperties,
        final LocalFileTransferImpl localFileTransfer,
        final MeterRegistry registry
    ) throws GenieException {
        return new CacheGenieFileTransferService(
            fileTransferFactory,
            fileCacheProperties.getLocation().toString(),
            localFileTransfer,
            registry
        );
    }

    /**
     * Get a implementation of the JobSubmitterService that runs jobs locally.
     *
     * @param dataServices    The {@link DataServices} instance to use
     * @param genieEventBus   The genie event bus implementation to use
     * @param workflowTasks   List of all the workflow tasks to be executed.
     * @param genieWorkingDir Working directory for genie where it creates jobs directories.
     * @param registry        The metrics registry to use
     * @return An instance of the JobSubmitterService.
     */
    @Bean
    @ConditionalOnMissingBean(JobSubmitterService.class)
    public JobSubmitterService jobSubmitterService(
        final DataServices dataServices,
        final GenieEventBus genieEventBus,
        final List<WorkflowTask> workflowTasks,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final MeterRegistry registry
    ) {
        return new LocalJobRunner(
            dataServices,
            genieEventBus,
            workflowTasks,
            genieWorkingDir,
            registry
        );
    }

    /**
     * The attachment service to use.
     *
     * @param jobsProperties All properties related to jobs
     * @return The attachment service to use
     */
    @Bean
    @ConditionalOnMissingBean(LegacyAttachmentService.class)
    public FileSystemAttachmentService legacyAttachmentService(final JobsProperties jobsProperties) {
        return new FileSystemAttachmentService(jobsProperties.getLocations().getAttachments().toString());
    }

    /**
     * The attachment service to use.
     *
     * @param s3ClientFactory             the S3 client factory
     * @param attachmentServiceProperties the service properties
     * @param meterRegistry               the meter registry
     * @return The attachment service to use
     * @throws IOException if the local filesystem implmentation is used and it fails to initialize
     */
    @Bean
    @ConditionalOnMissingBean(AttachmentService.class)
    public AttachmentService attachmentService(
        final S3ClientFactory s3ClientFactory,
        final AttachmentServiceProperties attachmentServiceProperties,
        final MeterRegistry meterRegistry
    ) throws IOException {
        final @NotNull URI location = attachmentServiceProperties.getLocationPrefix();
        final String scheme = location.getScheme();
        if ("s3".equals(scheme)) {
            return new S3AttachmentServiceImpl(s3ClientFactory, attachmentServiceProperties, meterRegistry);
        } else if ("file".equals(scheme)) {
            return new LocalFileSystemAttachmentServiceImpl(attachmentServiceProperties);
        } else {
            throw new IllegalStateException(
                "Unknown attachment service implementation to use for location: " + location
            );
        }
    }

    /**
     * FileTransfer factory.
     *
     * @return FileTransfer factory
     */
    @Bean
    @ConditionalOnMissingBean(name = "fileTransferFactory", value = ServiceLocatorFactoryBean.class)
    public ServiceLocatorFactoryBean fileTransferFactory() {
        final ServiceLocatorFactoryBean factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(FileTransferFactory.class);
        return factoryBean;
    }

    /**
     * Get a {@link JobFileService} implementation if one is required.
     *
     * @param jobsDir The job directory resource
     * @return A {@link DiskJobFileServiceImpl} instance
     * @throws IOException When the job directory can't be created or isn't a directory
     */
    @Bean
    @ConditionalOnMissingBean(JobFileService.class)
    public DiskJobFileServiceImpl jobFileService(@Qualifier("jobsDir") final Resource jobsDir) throws IOException {
        return new DiskJobFileServiceImpl(jobsDir);
    }

    /**
     * Get an implementation of {@link JobResolverService} if one hasn't already been defined.
     *
     * @param dataServices     The {@link DataServices} encapsulation instance to use
     * @param clusterSelectors The {@link ClusterSelector} implementations to use
     * @param commandSelector  The {@link CommandSelector} implementation to use
     * @param registry         The metrics repository to use
     * @param jobsProperties   The properties for running a job set by the user
     * @param environment      The Spring application {@link Environment} for dynamic property resolution
     * @return A {@link JobResolverServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobResolverService.class)
    public JobResolverServiceImpl jobResolverService(
        final DataServices dataServices,
        @NotEmpty final List<ClusterSelector> clusterSelectors,
        final CommandSelector commandSelector,
        final MeterRegistry registry,
        final JobsProperties jobsProperties,
        final Environment environment
    ) {
        return new JobResolverServiceImpl(
            dataServices,
            clusterSelectors,
            commandSelector,
            registry,
            jobsProperties,
            environment
        );
    }

    /**
     * Get an implementation of {@link JobCompletionService} if one hasn't already been defined.
     *
     * @param dataServices      The {@link DataServices} instance to use
     * @param jobArchiveService The {@link JobArchiveService} implementation to use
     * @param genieWorkingDir   Working directory for genie where it creates jobs directories.
     * @param mailService       The mail service
     * @param registry          Registry
     * @param jobsProperties    The jobs properties to use
     * @param retryTemplate     The retry template
     * @return an instance of {@link JobCompletionService}
     * @throws GenieException if the bean fails during construction
     */
    @Bean
    @ConditionalOnMissingBean(JobCompletionService.class)
    public JobCompletionService jobCompletionService(
        final DataServices dataServices,
        final JobArchiveService jobArchiveService,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final MailService mailService,
        final MeterRegistry registry,
        final JobsProperties jobsProperties,
        @Qualifier("genieRetryTemplate") final RetryTemplate retryTemplate
    ) throws GenieException {
        return new JobCompletionService(
            dataServices,
            jobArchiveService,
            genieWorkingDir,
            mailService,
            registry,
            jobsProperties,
            retryTemplate
        );
    }

    /**
     * Provide the default implementation of {@link JobDirectoryServerService} for serving job directory resources.
     *
     * @param resourceLoader                     The application resource loader used to get references to resources
     * @param dataServices                       The {@link DataServices} instance to use
     * @param agentFileStreamService             The service to request a file from an agent running a job
     * @param archivedJobService                 The {@link ArchivedJobService} implementation to use to get archived
     *                                           job data
     * @param meterRegistry                      The meter registry used to keep track of metrics
     * @param jobFileService                     The service responsible for managing the job working directory on disk
     *                                           for V3 Jobs
     * @param jobDirectoryManifestCreatorService The job directory manifest service
     * @param agentRoutingService                The agent routing service
     * @return An instance of {@link JobDirectoryServerServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(JobDirectoryServerService.class)
    public JobDirectoryServerServiceImpl jobDirectoryServerService(
        final ResourceLoader resourceLoader,
        final DataServices dataServices,
        final AgentFileStreamService agentFileStreamService,
        final ArchivedJobService archivedJobService,
        final MeterRegistry meterRegistry,
        final JobFileService jobFileService,
        final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService,
        final AgentRoutingService agentRoutingService
    ) {
        return new JobDirectoryServerServiceImpl(
            resourceLoader,
            dataServices,
            agentFileStreamService,
            archivedJobService,
            meterRegistry,
            jobFileService,
            jobDirectoryManifestCreatorService,
            agentRoutingService
        );
    }

    /**
     * Provide a {@link JobLaunchService} implementation if one isn't available.
     *
     * @param dataServices       The {@link DataServices} instance to use
     * @param jobResolverService The {@link JobResolverService} implementation to use
     * @param agentLauncher      The {@link AgentLauncher} implementation to use
     * @param registry           The metrics registry to use
     * @return A {@link JobLaunchServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobLaunchService.class)
    public JobLaunchServiceImpl jobLaunchService(
        final DataServices dataServices,
        final JobResolverService jobResolverService,
        final AgentLauncher agentLauncher,
        final MeterRegistry registry
    ) {
        return new JobLaunchServiceImpl(dataServices, jobResolverService, agentLauncher, registry);
    }

    /**
     * Provide a {@link ArchivedJobService} implementation if one hasn't been provided already.
     *
     * @param dataServices   The {@link DataServices} instance to use
     * @param resourceLoader The {@link ResourceLoader} to use
     * @param meterRegistry  The {@link MeterRegistry} implementation to use
     * @return A {@link ArchivedJobServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ArchivedJobService.class)
    public ArchivedJobServiceImpl archivedJobService(
        final DataServices dataServices,
        final ResourceLoader resourceLoader,
        final MeterRegistry meterRegistry
    ) {
        return new ArchivedJobServiceImpl(dataServices, resourceLoader, meterRegistry);
    }
}
