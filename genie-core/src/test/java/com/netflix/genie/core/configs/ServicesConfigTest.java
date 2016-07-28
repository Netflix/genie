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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.core.jpa.services.JpaApplicationServiceImpl;
import com.netflix.genie.core.jpa.services.JpaClusterServiceImpl;
import com.netflix.genie.core.jpa.services.JpaCommandServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobPersistenceServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobSearchServiceImpl;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.FileTransferFactory;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobCountService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.services.impl.FileSystemAttachmentService;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.genie.core.services.impl.JobCountServiceImpl;
import com.netflix.genie.core.services.impl.LocalJobKillServiceImpl;
import com.netflix.genie.core.services.impl.LocalJobRunner;
import com.netflix.genie.core.services.impl.RandomizedClusterLoadBalancerImpl;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

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
     * @param applicationRepo The application repository to use.
     * @param commandRepo     The command repository to use.
     * @return An application service instance.
     */
    @Bean
    public ApplicationService applicationService(
        final JpaApplicationRepository applicationRepo,
        final JpaCommandRepository commandRepo
    ) {
        return new JpaApplicationServiceImpl(applicationRepo, commandRepo);
    }

    /**
     * Get JPA based implementation of the ClusterService.
     *
     * @param clusterRepo The cluster repository to use.
     * @param commandRepo The command repository to use.
     * @return A cluster service instance.
     */
    @Bean
    public ClusterService clusterService(
        final JpaClusterRepository clusterRepo,
        final JpaCommandRepository commandRepo
    ) {
        return new JpaClusterServiceImpl(clusterRepo, commandRepo);
    }

    /**
     * Get JPA based implementation of the CommandService.
     *
     * @param commandRepo the command repository to use
     * @param appRepo     the application repository to use
     * @param clusterRepo the cluster repository to use
     * @return A command service instance.
     */
    @Bean
    public CommandService commandService(
        final JpaCommandRepository commandRepo,
        final JpaApplicationRepository appRepo,
        final JpaClusterRepository clusterRepo
    ) {
        return new JpaCommandServiceImpl(commandRepo, appRepo, clusterRepo);
    }

    /**
     * Get JPA based implementation of the JobSearchService.
     *
     * @param jobRepository          The repository to use for job entities
     * @param jobRequestRepository   The repository to use for job request entities
     * @param jobExecutionRepository The repository to use for job execution entities
     * @return A job search service instance.
     */
    @Bean
    public JobSearchService jobSearchService(
        final JpaJobRepository jobRepository,
        final JpaJobRequestRepository jobRequestRepository,
        final JpaJobExecutionRepository jobExecutionRepository
    ) {
        return new JpaJobSearchServiceImpl(jobRepository, jobRequestRepository, jobExecutionRepository);
    }

    /**
     * Get JPA based implementation of the JobPersistenceService.
     *
     * @param jobRepo          The job repository to use
     * @param jobRequestRepo   The job request repository to use
     * @param jobExecutionRepo The jobExecution Repository to use
     * @param applicationRepo  The application repository to use
     * @param clusterRepo      The cluster repository to use
     * @param commandRepo      The command repository to use
     * @return A job search service instance.
     */
    @Bean
    public JobPersistenceService jobPersistenceService(
        final JpaJobRepository jobRepo,
        final JpaJobRequestRepository jobRequestRepo,
        final JpaJobExecutionRepository jobExecutionRepo,
        final JpaApplicationRepository applicationRepo,
        final JpaClusterRepository clusterRepo,
        final JpaCommandRepository commandRepo
    ) {
        return new JpaJobPersistenceServiceImpl(
            jobRepo,
            jobRequestRepo,
            jobExecutionRepo,
            applicationRepo,
            clusterRepo,
            commandRepo
        );
    }

    /**
     * Get an local implementation of the JobKillService.
     *
     * @param hostname         The name of the host this Genie node is running on.
     * @param jobSearchService The job search service to use to locate job information.
     * @param executor         The executor to use to run system processes.
     * @param eventPublisher   The event publisher to use
     * @return A job kill service instance.
     */
    @Bean
    public JobKillService jobKillService(
        final String hostname,
        final JobSearchService jobSearchService,
        final Executor executor,
        final ApplicationEventPublisher eventPublisher
    ) {
        return new LocalJobKillServiceImpl(hostname, jobSearchService, executor, false, eventPublisher);
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
     * @param jps                 Implementation of the job persistence service.
     * @param applicationService  Implementation of application service interface.
     * @param clusterService      Implementation of cluster service interface.
     * @param commandService      Implementation of command service interface.
     * @param clusterLoadBalancer Implementation of the cluster load balancer interface.
     * @param aep                 Instance of the event publisher.
     * @param workflowTasks       List of all the workflow tasks to be executed.
     * @param genieWorkingDir     Working directory for genie where it creates jobs directories.
     * @return An instance of the JobSubmitterService.
     */
    @Bean
    public JobSubmitterService jobSubmitterService(
        final JobPersistenceService jps,
        final ApplicationService applicationService,
        final ClusterService clusterService,
        final CommandService commandService,
        final ClusterLoadBalancer clusterLoadBalancer,
        final ApplicationEventPublisher aep,
        final List<WorkflowTask> workflowTasks,
        final Resource genieWorkingDir
    ) {
        return new LocalJobRunner(
            jps,
            applicationService,
            clusterService,
            commandService,
            clusterLoadBalancer,
            aep,
            workflowTasks,
            genieWorkingDir
        );
    }

    /**
     * The job count service to use.
     *
     * @param jobSearchService The job search implementation to use
     * @param hostname         The hostname of this Genie node
     * @return The job count service bean
     */
    @Bean
    public JobCountService jobCountService(final JobSearchService jobSearchService, final String hostname) {
        return new JobCountServiceImpl(jobSearchService, hostname);
    }

    /**
     * The task executor to use.
     *
     * @return The task executor to for launching jobs
     */
    @Bean
    public AsyncTaskExecutor taskExecutor() {
        return new ThreadPoolTaskExecutor();
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
     * @param taskExecutor          implementation of the async task executor interface to use
     * @param jobPersistenceService implementation of job persistence service interface.
     * @param jobSubmitterService   implementation of the job submitter service.
     * @param jobCountService       implementation of job count service interface
     * @param jobKillService        The job kill service to use.
     * @param baseArchiveLocation   The base directory location of where the job dir should be archived.
     * @param maxRunningJobs        The maximum number of running jobs on system
     * @param registry              The registry to use
     * @param eventPublisher        The system event publisher
     * @return An instance of the JobCoordinatorService.
     */
    @Bean
    public JobCoordinatorService jobCoordinatorService(
        final AsyncTaskExecutor taskExecutor,
        final JobPersistenceService jobPersistenceService,
        final JobSubmitterService jobSubmitterService,
        final JobKillService jobKillService,
        final JobCountService jobCountService,
        @Value("${genie.jobs.archive.location}")
        final String baseArchiveLocation,
        @Value("${genie.jobs.max.running:2}")
        final int maxRunningJobs,
        final Registry registry,
        final ApplicationEventPublisher eventPublisher
    ) {
        return new JobCoordinatorService(
            taskExecutor,
            jobPersistenceService,
            jobSubmitterService,
            jobKillService,
            jobCountService,
            baseArchiveLocation,
            maxRunningJobs,
            registry,
            eventPublisher
        );
    }

    /**
     * The attachment service to use.
     *
     * @param attachmentsDirectory The directory to use to store attachments temporarily
     * @return The attachment service to use
     */
    @Bean
    public AttachmentService attachmentService(
        @Value("${genie.jobs.attachments.dir:#{null}}") final String attachmentsDirectory
    ) {
        return new FileSystemAttachmentService(attachmentsDirectory);
    }

    /**
     * FileTransfer factory.
     * @return FileTransfer factory
     */
    @Bean
    public FactoryBean fileTransferFactory() {
        final ServiceLocatorFactoryBean factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(FileTransferFactory.class);
        return factoryBean;
    }
}
