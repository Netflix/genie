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
import com.netflix.genie.common.internal.services.JobArchiveService;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
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
import com.netflix.genie.web.properties.ScriptLoadBalancerProperties;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobFileService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.services.MailService;
import com.netflix.genie.web.services.impl.ArchivedJobServiceImpl;
import com.netflix.genie.web.services.impl.CacheGenieFileTransferService;
import com.netflix.genie.web.services.impl.DiskJobFileServiceImpl;
import com.netflix.genie.web.services.impl.FileSystemAttachmentService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.JobCoordinatorServiceImpl;
import com.netflix.genie.web.services.impl.JobDirectoryServerServiceImpl;
import com.netflix.genie.web.services.impl.JobKillServiceImpl;
import com.netflix.genie.web.services.impl.JobKillServiceV3;
import com.netflix.genie.web.services.impl.JobLaunchServiceImpl;
import com.netflix.genie.web.services.impl.JobResolverServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.services.impl.LocalJobRunner;
import com.netflix.genie.web.services.impl.RandomizedClusterLoadBalancerImpl;
import com.netflix.genie.web.services.loadbalancers.script.ScriptLoadBalancer;
import com.netflix.genie.web.tasks.job.JobCompletionService;
import com.netflix.genie.web.util.ProcessChecker;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.TaskScheduler;

import javax.validation.constraints.NotEmpty;
import java.io.IOException;
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
    }
)
@Slf4j
public class ServicesAutoConfiguration {

    /**
     * The relative order of the {@link ScriptLoadBalancer} if one is enabled relative to other
     * {@link ClusterLoadBalancer} instances that may be in the context. This allows users to fit {@literal 50} more
     * balancer's between the script load balancer and the default {@link RandomizedClusterLoadBalancerImpl}. If
     * the user wants to place a balancer implementation before the script one they only need to subtract from this
     * value.
     */
    public static final int SCRIPT_LOAD_BALANCER_PRECEDENCE = Ordered.LOWEST_PRECEDENCE - 50;

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
     * @param jobSearchService      The job search service to use to locate job information.
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
        final JobSearchService jobSearchService,
        final Executor executor,
        final JobsProperties jobsProperties,
        final GenieEventBus genieEventBus,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final ObjectMapper objectMapper,
        final ProcessChecker.Factory processCheckerFactory
    ) {
        return new JobKillServiceV3(
            genieHostInfo.getHostname(),
            jobSearchService,
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
     * @param jobKillServiceV3      Service to kill V3 jobs.
     * @param jobKillServiceV4      Service to kill V4 jobs.
     * @param jobPersistenceService Job persistence service
     * @return A job kill service instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobKillService.class)
    public JobKillServiceImpl jobKillService(
        final JobKillServiceV3 jobKillServiceV3,
        final JobKillServiceV4 jobKillServiceV4,
        final JobPersistenceService jobPersistenceService
    ) {
        return new JobKillServiceImpl(
            jobKillServiceV3,
            jobKillServiceV4,
            jobPersistenceService
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
     * @param jobPersistenceService Implementation of the job persistence service.
     * @param genieEventBus         The genie event bus implementation to use
     * @param workflowTasks         List of all the workflow tasks to be executed.
     * @param genieWorkingDir       Working directory for genie where it creates jobs directories.
     * @param registry              The metrics registry to use
     * @return An instance of the JobSubmitterService.
     */
    @Bean
    @ConditionalOnMissingBean(JobSubmitterService.class)
    public JobSubmitterService jobSubmitterService(
        final JobPersistenceService jobPersistenceService,
        final GenieEventBus genieEventBus,
        final List<WorkflowTask> workflowTasks,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final MeterRegistry registry
    ) {
        return new LocalJobRunner(
            jobPersistenceService,
            genieEventBus,
            workflowTasks,
            genieWorkingDir,
            registry
        );
    }

    /**
     * Get an instance of the JobCoordinatorService.
     *
     * @param jobPersistenceService         implementation of job persistence service interface
     * @param jobKillService                The job kill service to use
     * @param jobStateService               The running job metrics service to use
     * @param jobSearchService              Implementation of job search service interface
     * @param jobsProperties                The jobs properties to use
     * @param applicationPersistenceService Implementation of application service interface
     * @param clusterPersistenceService     Implementation of cluster service interface
     * @param commandPersistenceService     Implementation of command service interface
     * @param jobResolverService            The job specification service to use
     * @param registry                      The metrics registry to use
     * @param genieHostInfo                 Information about the host the Genie process is running on
     * @return An instance of the JobCoordinatorService.
     */
    @Bean
    @ConditionalOnMissingBean(JobCoordinatorService.class)
    public JobCoordinatorService jobCoordinatorService(
        final JobPersistenceService jobPersistenceService,
        final JobKillService jobKillService,
        @Qualifier("jobMonitoringCoordinator") final JobStateService jobStateService,
        final JobSearchService jobSearchService,
        final JobsProperties jobsProperties,
        final ApplicationPersistenceService applicationPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        final JobResolverService jobResolverService,
        final MeterRegistry registry,
        final GenieHostInfo genieHostInfo
    ) {
        return new JobCoordinatorServiceImpl(
            jobPersistenceService,
            jobKillService,
            jobStateService,
            jobsProperties,
            applicationPersistenceService,
            jobSearchService,
            clusterPersistenceService,
            commandPersistenceService,
            jobResolverService,
            registry,
            genieHostInfo.getHostname()
        );
    }

    /**
     * The attachment service to use.
     *
     * @param jobsProperties All properties related to jobs
     * @return The attachment service to use
     */
    @Bean
    @ConditionalOnMissingBean(AttachmentService.class)
    public FileSystemAttachmentService attachmentService(final JobsProperties jobsProperties) {
        return new FileSystemAttachmentService(jobsProperties.getLocations().getAttachments().toString());
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
     * @param applicationPersistenceService The service to use to manipulate applications
     * @param clusterPersistenceService     The service to use to manipulate clusters
     * @param commandPersistenceService     The service to use to manipulate commands
     * @param jobPersistenceService         The job persistence service instance to use
     * @param clusterLoadBalancerImpls      The load balancer implementations to use
     * @param registry                      The metrics repository to use
     * @param jobsProperties                The properties for running a job set by the user
     * @return A {@link JobResolverServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobResolverService.class)
    public JobResolverServiceImpl jobResolverService(
        final ApplicationPersistenceService applicationPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        final JobPersistenceService jobPersistenceService,
        @NotEmpty final List<ClusterLoadBalancer> clusterLoadBalancerImpls,
        final MeterRegistry registry,
        final JobsProperties jobsProperties
    ) {
        return new JobResolverServiceImpl(
            applicationPersistenceService,
            clusterPersistenceService,
            commandPersistenceService,
            jobPersistenceService,
            clusterLoadBalancerImpls,
            registry,
            jobsProperties
        );
    }

    /**
     * Get an implementation of {@link JobCompletionService} if one hasn't already been defined.
     *
     * @param jobPersistenceService The job persistence service to use
     * @param jobSearchService      The job search service to use
     * @param jobArchiveService     The {@link JobArchiveService} implementation to use
     * @param genieWorkingDir       Working directory for genie where it creates jobs directories.
     * @param mailService           The mail service
     * @param registry              Registry
     * @param jobsProperties        The jobs properties to use
     * @param retryTemplate         The retry template
     * @return an instance of {@link JobCompletionService}
     * @throws GenieException if the bean fails during construction
     */
    @Bean
    @ConditionalOnMissingBean(JobCompletionService.class)
    public JobCompletionService jobCompletionService(
        final JobPersistenceService jobPersistenceService,
        final JobSearchService jobSearchService,
        final JobArchiveService jobArchiveService,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final MailService mailService,
        final MeterRegistry registry,
        final JobsProperties jobsProperties,
        @Qualifier("genieRetryTemplate") final RetryTemplate retryTemplate
    ) throws GenieException {
        return new JobCompletionService(
            jobPersistenceService,
            jobSearchService,
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
     * @param jobPersistenceService              The job persistence service used to get information about a job
     * @param agentFileStreamService             The service to request a file from an agent running a job
     * @param meterRegistry                      The meter registry used to keep track of metrics
     * @param jobFileService                     The service responsible for managing the job working directory on disk
     *                                           for V3 Jobs
     * @param jobDirectoryManifestCreatorService The job directory manifest service
     * @return An instance of {@link JobDirectoryServerServiceImpl}
     */
    @Bean
    @ConditionalOnMissingBean(JobDirectoryServerService.class)
    public JobDirectoryServerServiceImpl jobDirectoryServerService(
        final ResourceLoader resourceLoader,
        final JobPersistenceService jobPersistenceService,
        final AgentFileStreamService agentFileStreamService,
        final MeterRegistry meterRegistry,
        final JobFileService jobFileService,
        final JobDirectoryManifestCreatorService jobDirectoryManifestCreatorService
    ) {
        return new JobDirectoryServerServiceImpl(
            resourceLoader,
            jobPersistenceService,
            agentFileStreamService,
            meterRegistry,
            jobFileService,
            jobDirectoryManifestCreatorService
        );
    }

    /**
     * Produce the {@link ScriptLoadBalancer} instance to use for this Genie node if it was configured by the user.
     *
     * @param asyncTaskExecutor   The asynchronous task executor to use
     * @param taskScheduler       The task scheduler to use
     * @param fileTransferService The file transfer service to use
     * @param environment         The program environment from Spring
     * @param mapper              The JSON object mapper to use
     * @param registry            The meter registry for capturing metrics
     * @return A {@link ScriptLoadBalancer} if one enabled
     */
    @Bean
    @Order(SCRIPT_LOAD_BALANCER_PRECEDENCE)
    @ConditionalOnProperty(value = ScriptLoadBalancerProperties.ENABLED_PROPERTY, havingValue = "true")
    public ScriptLoadBalancer scriptLoadBalancer(
        @Qualifier("genieAsyncTaskExecutor") final AsyncTaskExecutor asyncTaskExecutor,
        @Qualifier("genieTaskScheduler") final TaskScheduler taskScheduler,
        @Qualifier("cacheGenieFileTransferService") final GenieFileTransferService fileTransferService,
        final Environment environment,
        final ObjectMapper mapper,
        final MeterRegistry registry
    ) {
        log.info("Script load balancing is enabled. Creating a ScriptLoadBalancer.");
        return new ScriptLoadBalancer(
            asyncTaskExecutor,
            taskScheduler,
            fileTransferService,
            environment,
            mapper,
            registry
        );
    }

    /**
     * The default cluster load balancer if all others fail.
     * <p>
     * Defaults to {@link Ordered#LOWEST_PRECEDENCE}.
     *
     * @return A {@link RandomizedClusterLoadBalancerImpl} instance
     */
    @Bean
    @Order
    public RandomizedClusterLoadBalancerImpl randomizedClusterLoadBalancer() {
        return new RandomizedClusterLoadBalancerImpl();
    }

    /**
     * Provide a {@link JobLaunchService} implementation if one isn't available.
     *
     * @param jobPersistenceService The {@link JobPersistenceService} implementation to use
     * @param jobResolverService    The {@link JobResolverService} implementation to use
     * @param agentLauncher         The {@link AgentLauncher} implementation to use
     * @param registry              The metrics registry to use
     * @return A {@link JobLaunchServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobLaunchService.class)
    public JobLaunchServiceImpl jobLaunchService(
        final JobPersistenceService jobPersistenceService,
        final JobResolverService jobResolverService,
        final AgentLauncher agentLauncher,
        final MeterRegistry registry
    ) {
        return new JobLaunchServiceImpl(jobPersistenceService, jobResolverService, agentLauncher, registry);
    }

    /**
     * Provide a {@link ArchivedJobService} implementation if one hasn't been provided already.
     *
     * @param jobPersistenceService The {@link JobPersistenceService} implementation to use
     * @param resourceLoader        The {@link ResourceLoader} to use
     * @param meterRegistry         The {@link MeterRegistry} implementation to use
     * @return A {@link ArchivedJobServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(ArchivedJobService.class)
    public ArchivedJobServiceImpl archivedJobService(
        final JobPersistenceService jobPersistenceService,
        final ResourceLoader resourceLoader,
        final MeterRegistry meterRegistry
    ) {
        return new ArchivedJobServiceImpl(jobPersistenceService, resourceLoader, meterRegistry);
    }
}
