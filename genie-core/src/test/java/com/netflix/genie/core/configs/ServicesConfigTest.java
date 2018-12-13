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
package com.netflix.genie.core.configs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.events.GenieEventBus;
import com.netflix.genie.core.events.GenieEventBusImpl;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaFileRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaTagRepository;
import com.netflix.genie.core.jpa.services.JpaApplicationServiceImpl;
import com.netflix.genie.core.jpa.services.JpaClusterServiceImpl;
import com.netflix.genie.core.jpa.services.JpaCommandServiceImpl;
import com.netflix.genie.core.jpa.services.JpaFileServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobPersistenceServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobSearchServiceImpl;
import com.netflix.genie.core.jpa.services.JpaTagServiceImpl;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.properties.JobsUsersActiveLimitProperties;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.FileService;
import com.netflix.genie.core.services.FileTransferFactory;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobMetricsService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobStateService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.services.TagService;
import com.netflix.genie.core.services.impl.FileSystemAttachmentService;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.genie.core.services.impl.JobCoordinatorServiceImpl;
import com.netflix.genie.core.services.impl.JobMetricsServiceImpl;
import com.netflix.genie.core.services.impl.JobStateServiceImpl;
import com.netflix.genie.core.services.impl.LocalJobKillServiceImpl;
import com.netflix.genie.core.services.impl.LocalJobRunner;
import com.netflix.genie.core.services.impl.RandomizedClusterLoadBalancerImpl;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.SimpleApplicationEventMulticaster;
import org.springframework.core.io.Resource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.List;

/**
 * Configuration to create the Service beans for Genie Core Tests.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
public class ServicesConfigTest {

    /**
     * Get JPA based implementation of the ApplicationService.
     *
     * @param tagService            The tag service to use
     * @param tagRepository         The tag repository to use
     * @param fileService           The file service to use
     * @param fileRepository        The file repository to use
     * @param applicationRepository The application repository to use.
     * @param commandRepository     The command repository to use.
     * @return An application service instance.
     */
    @Bean
    public ApplicationService applicationService(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaApplicationServiceImpl(
            tagService,
            tagRepository,
            fileService,
            fileRepository,
            applicationRepository,
            commandRepository
        );
    }

    /**
     * Get JPA based implementation of the ClusterService.
     *
     * @param tagService        The tag service to use
     * @param tagRepository     The tag repository to use
     * @param fileService       The file service to use
     * @param fileRepository    The file repository to use
     * @param clusterRepository The cluster repository to use.
     * @param commandRepository The command repository to use.
     * @return A cluster service instance.
     */
    @Bean
    public ClusterService clusterService(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaClusterServiceImpl(
            tagService,
            tagRepository,
            fileService,
            fileRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * Get JPA based implementation of the CommandService.
     *
     * @param tagService            The tag service to use
     * @param tagRepository         The tag repository to use
     * @param fileService           The file service to use
     * @param fileRepository        The file repository to use
     * @param commandRepository     the command repository to use
     * @param applicationRepository the application repository to use
     * @param clusterRepository     the cluster repository to use
     * @return A command service instance.
     */
    @Bean
    public CommandService commandService(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaCommandRepository commandRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository
    ) {
        return new JpaCommandServiceImpl(
            tagService,
            tagRepository,
            fileService,
            fileRepository,
            commandRepository,
            applicationRepository,
            clusterRepository
        );
    }

    /**
     * Get JPA based implementation of the JobSearchService.
     *
     * @param jobRepository     The repository to use for job entities
     * @param clusterRepository The repository to use for cluster entities
     * @param commandRepository The repository to use for command entities
     * @return A job search service instance.
     */
    @Bean
    public JobSearchService jobSearchService(
        final JpaJobRepository jobRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaJobSearchServiceImpl(
            jobRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * Get JPA based implementation of the JobPersistenceService.
     *
     * @param tagService            The tag service to use
     * @param tagRepository         The tag repository to use
     * @param fileService           The file service to use
     * @param fileRepository        The file repository to use
     * @param jobRepository         The job repository to use
     * @param applicationRepository The application repository to use
     * @param clusterRepository     The cluster repository to use
     * @param commandRepository     The command repository to use
     * @return A job search service instance.
     */
    @Bean
    public JobPersistenceService jobPersistenceService(
        final TagService tagService,
        final JpaTagRepository tagRepository,
        final FileService fileService,
        final JpaFileRepository fileRepository,
        final JpaJobRepository jobRepository,
        final JpaApplicationRepository applicationRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaJobPersistenceServiceImpl(
            tagService,
            tagRepository,
            fileService,
            fileRepository,
            jobRepository,
            applicationRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * Get an local implementation of the JobKillService.
     *
     * @param hostname         The name of the host this Genie node is running on.
     * @param jobSearchService The job search service to use to locate job information.
     * @param executor         The executor to use to run system processes.
     * @param genieEventBus    The Genie event bus to use
     * @param genieWorkingDir  Working directory for genie where it creates jobs directories.
     * @param objectMapper     The Jackson ObjectMapper used to serialize from/to JSON
     * @return A job kill service instance.
     */
    @Bean
    public JobKillService jobKillService(
        final String hostname,
        final JobSearchService jobSearchService,
        final Executor executor,
        final GenieEventBus genieEventBus,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final ObjectMapper objectMapper
    ) {
        return new LocalJobKillServiceImpl(
            hostname,
            jobSearchService,
            executor,
            false,
            genieEventBus,
            genieWorkingDir,
            objectMapper
        );
    }

    /**
     * Get a Randomized Cluster load balancer.
     *
     * @return A randomized cluster load balancer instance.
     */
    @Bean
    public ClusterLoadBalancer clusterLoadBalancer() {
        return new RandomizedClusterLoadBalancerImpl();
    }

    /**
     * Get an instance of the Genie File Transfer service.
     *
     * @param fileTransferFactory file transfer implementation factory
     * @return A singleton for GenieFileTransferService
     * @throws GenieException If there is any problem
     */
    @Bean
    public GenieFileTransferService genieFileTransferService(
        final FileTransferFactory fileTransferFactory
    ) throws GenieException {
        return new GenieFileTransferService(fileTransferFactory);
    }

    /**
     * Get a implementation of the JobSubmitterService that runs jobs locally.
     *
     * @param jobPersistenceService Implementation of the job persistence service.
     * @param genieEventBus         The Genie event bus implementation to use
     * @param workflowTasks         List of all the workflow tasks to be executed.
     * @param genieWorkingDir       Working directory for genie where it creates jobs directories.
     * @param registry              The metrics registry to use
     * @return An instance of the JobSubmitterService.
     */
    @Bean
    public JobSubmitterService jobSubmitterService(
        final JobPersistenceService jobPersistenceService,
        final GenieEventBus genieEventBus,
        final List<WorkflowTask> workflowTasks,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final Registry registry
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
     * The job count service to use.
     *
     * @param jobSearchService The job search implementation to use
     * @param hostName         The host name of this Genie node
     * @return The job count service bean
     */
    @Bean
    public JobMetricsService jobCountService(final JobSearchService jobSearchService, final String hostName) {
        return new JobMetricsServiceImpl(jobSearchService, hostName);
    }

    /**
     * Default task scheduler.
     *
     * @return task scheduler
     */
    @Bean
    public TaskScheduler taskScheduler() {
        return new ThreadPoolTaskScheduler();
    }

    /**
     * The job state service to use.
     *
     * @param jobSubmitterService The job submitter implementation to use
     * @param taskScheduler       The task scheduler to use to register scheduling of job checkers
     * @param genieEventBus       The genie event bus
     * @param registry            The metrics registry
     * @return The job state service bean
     */
    @Bean
    public JobStateService jobStateService(
        final JobSubmitterService jobSubmitterService,
        final TaskScheduler taskScheduler,
        final GenieEventBus genieEventBus,
        final Registry registry
    ) {
        return new JobStateServiceImpl(jobSubmitterService, taskScheduler, genieEventBus, registry);
    }

    /**
     * The async task executor to use.
     *
     * @return The task executor to for launching jobs
     */
    @Bean
    public AsyncTaskExecutor genieAsyncTaskExecutor() {
        return new ThreadPoolTaskExecutor();
    }

    /**
     * The sync task executor to use.
     *
     * @return The task executor to for launching jobs
     */
    @Bean
    public SyncTaskExecutor genieSyncTaskExecutor() {
        return new SyncTaskExecutor();
    }

    /**
     * Genie Event Bus.
     *
     * @param syncTaskExecutor  The synchronous task executor to use
     * @param asyncTaskExecutor The asynchronous task executor to use
     * @return The application event multicaster to use
     */
    @Bean
    public GenieEventBusImpl applicationEventMulticaster(
        final SyncTaskExecutor syncTaskExecutor,
        final AsyncTaskExecutor asyncTaskExecutor
    ) {
        final SimpleApplicationEventMulticaster testSyncMulticaster = new SimpleApplicationEventMulticaster();
        testSyncMulticaster.setTaskExecutor(syncTaskExecutor);

        final SimpleApplicationEventMulticaster testAsyncMulticaster = new SimpleApplicationEventMulticaster();
        testAsyncMulticaster.setTaskExecutor(asyncTaskExecutor);
        return new GenieEventBusImpl(testSyncMulticaster, testAsyncMulticaster);
    }

    /**
     * Registry bean.
     *
     * @return a default registry
     */
    @Bean
    public Registry registry() {
        return new DefaultRegistry();
    }

    /**
     * Get an instance of the JobCoordinatorService.
     *
     * @param jobPersistenceService                  implementation of job persistence service interface.
     * @param jobKillService                         The job kill service to use.
     * @param jobStateService                        implementation of job state service interface
     * @param jobSearchService                       implementation of job search service interface
     * @param jobsProperties                         The jobs properties to use
     * @param jobsUsersActiveLimitPropertiesProvider The user limits dynamic properties provider
     * @param applicationService                     Implementation of application service interface
     * @param clusterService                         Implementation of cluster service interface
     * @param commandService                         Implementation of command service interface
     * @param clusterLoadBalancers                   Implementations of the cluster load balancer interface
     * @param registry                               The registry to use
     * @param hostName                               The host name to use
     * @return An instance of the JobCoordinatorService.
     */
    @Bean
    public JobCoordinatorService jobCoordinatorService(
        final JobPersistenceService jobPersistenceService,
        final JobKillService jobKillService,
        final JobStateService jobStateService,
        final JobSearchService jobSearchService,
        final JobsProperties jobsProperties,
        final ObjectFactory<JobsUsersActiveLimitProperties> jobsUsersActiveLimitPropertiesProvider,
        final ApplicationService applicationService,
        final ClusterService clusterService,
        final CommandService commandService,
        final List<ClusterLoadBalancer> clusterLoadBalancers,
        final Registry registry,
        final String hostName
    ) {
        if (clusterLoadBalancers.isEmpty()) {
            throw new IllegalStateException("Must have at least one active implementation of ClusterLoadBalancer");
        }
        return new JobCoordinatorServiceImpl(
            jobPersistenceService,
            jobKillService,
            jobStateService,
            jobsProperties,
            jobsUsersActiveLimitPropertiesProvider,
            applicationService,
            jobSearchService,
            clusterService,
            commandService,
            clusterLoadBalancers,
            registry,
            hostName
        );
    }

    /**
     * The attachment service to use.
     *
     * @param jobsProperties The various jobs properties including the location of the attachments directory
     * @return The attachment service to use
     */
    @Bean
    public AttachmentService attachmentService(final JobsProperties jobsProperties) {
        return new FileSystemAttachmentService(jobsProperties.getLocations().getAttachments());
    }

    /**
     * FileTransfer factory.
     *
     * @return FileTransfer factory
     */
    @Bean
    public FactoryBean fileTransferFactory() {
        final ServiceLocatorFactoryBean factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(FileTransferFactory.class);
        return factoryBean;
    }

    /**
     * Get the jobs properties to use for the services.
     *
     * @return The jobs properties to use
     */
    @Bean
    @ConfigurationProperties("genie.jobs")
    public JobsProperties jobsProperties() {
        return new JobsProperties();
    }

    /**
     * Create the tag service bean.
     *
     * @param tagRepository The tag repository to use
     * @return The tag service implementation
     */
    @Bean
    public TagService tagService(final JpaTagRepository tagRepository) {
        return new JpaTagServiceImpl(tagRepository);
    }

    /**
     * Create the file service bean.
     *
     * @param fileRepository The file repository to use
     * @return The file service implementation
     */
    @Bean
    public FileService fileService(final JpaFileRepository fileRepository) {
        return new JpaFileServiceImpl(fileRepository);
    }
}
