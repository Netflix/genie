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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.events.GenieEventBus;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobMetadataRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import com.netflix.genie.core.jpa.services.JpaApplicationServiceImpl;
import com.netflix.genie.core.jpa.services.JpaClusterServiceImpl;
import com.netflix.genie.core.jpa.services.JpaCommandServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobPersistenceServiceImpl;
import com.netflix.genie.core.jpa.services.JpaJobSearchServiceImpl;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.FileTransferFactory;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.core.services.JobStateService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.core.services.MailService;
import com.netflix.genie.core.services.impl.CacheGenieFileTransferService;
import com.netflix.genie.core.services.impl.DefaultMailServiceImpl;
import com.netflix.genie.core.services.impl.FileSystemAttachmentService;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import com.netflix.genie.core.services.impl.JobCoordinatorServiceImpl;
import com.netflix.genie.core.services.impl.LocalFileTransferImpl;
import com.netflix.genie.core.services.impl.LocalJobKillServiceImpl;
import com.netflix.genie.core.services.impl.LocalJobRunner;
import com.netflix.genie.core.services.impl.MailServiceImpl;
import com.netflix.genie.core.services.impl.RandomizedClusterLoadBalancerImpl;
import com.netflix.spectator.api.Registry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.mail.javamail.JavaMailSender;

import java.util.List;

/**
 * Configuration for all the services.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Configuration
public class ServicesConfig {

    /**
     * Returns a bean for mail service impl using the Spring Mail.
     *
     * @param javaMailSender An implementation of the JavaMailSender interface.
     * @param fromAddress    The from email address for the email.
     * @return An instance of MailService implementation.
     */
    @Bean
    @ConditionalOnProperty("spring.mail.host")
    public MailService getJavaMailSenderMailService(
        final JavaMailSender javaMailSender,
        @Value("${genie.mail.fromAddress}") final String fromAddress
    ) {
        return new MailServiceImpl(javaMailSender, fromAddress);
    }

    /**
     * Get an default implementation of the Mail Service interface if nothing is supplied.
     *
     * @return The mail service implementation that does nothing.
     */
    @Bean
    @ConditionalOnMissingBean
    public MailService getDefaultMailServiceImpl() {
        return new DefaultMailServiceImpl();
    }

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
     * @param clusterRepository      The repository to use for cluster entities
     * @param commandRepository      The repository to use for command entities
     * @return A job search service instance.
     */
    @Bean
    public JobSearchService jobSearchService(
        final JpaJobRepository jobRepository,
        final JpaJobRequestRepository jobRequestRepository,
        final JpaJobExecutionRepository jobExecutionRepository,
        final JpaClusterRepository clusterRepository,
        final JpaCommandRepository commandRepository
    ) {
        return new JpaJobSearchServiceImpl(
            jobRepository,
            jobRequestRepository,
            jobExecutionRepository,
            clusterRepository,
            commandRepository
        );
    }

    /**
     * Get JPA based implementation of the JobPersistenceService.
     *
     * @param jobRepo               The job repository to use
     * @param jobRequestRepo        The job request repository to use
     * @param jobMetadataRepository The job metadata repository to use
     * @param jobExecutionRepo      The job execution repository to use
     * @param applicationRepo       The application repository to use
     * @param clusterRepo           The cluster repository to use
     * @param commandRepo           The command repository to use
     * @return A job search service instance.
     */
    @Bean
    public JobPersistenceService jobPersistenceService(
        final JpaJobRepository jobRepo,
        final JpaJobRequestRepository jobRequestRepo,
        final JpaJobMetadataRepository jobMetadataRepository,
        final JpaJobExecutionRepository jobExecutionRepo,
        final JpaApplicationRepository applicationRepo,
        final JpaClusterRepository clusterRepo,
        final JpaCommandRepository commandRepo
    ) {
        return new JpaJobPersistenceServiceImpl(
            jobRepo,
            jobRequestRepo,
            jobMetadataRepository,
            jobExecutionRepo,
            applicationRepo,
            clusterRepo,
            commandRepo
        );
    }

    /**
     * Get an local implementation of the JobKillService.
     *
     * @param hostName         The name of the host this Genie node is running on.
     * @param jobSearchService The job search service to use to locate job information.
     * @param executor         The executor to use to run system processes.
     * @param jobsProperties   The jobs properties to use
     * @param genieEventBus    The application event bus to use to publish system wide events
     * @param genieWorkingDir  Working directory for genie where it creates jobs directories.
     * @param objectMapper     The Jackson ObjectMapper used to serialize from/to JSON
     * @return A job kill service instance.
     */
    @Bean
    public JobKillService jobKillService(
        final String hostName,
        final JobSearchService jobSearchService,
        final Executor executor,
        final JobsProperties jobsProperties,
        final GenieEventBus genieEventBus,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final ObjectMapper objectMapper
    ) {
        return new LocalJobKillServiceImpl(
            hostName,
            jobSearchService,
            executor,
            jobsProperties.getUsers().isRunAsUserEnabled(),
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
     * Get an instance of the Cache Genie File Transfer service.
     *
     * @param fileTransferFactory file transfer implementation factory
     * @param baseCacheLocation   file cache location
     * @param localFileTransfer   local file transfer service
     * @param registry            Registry
     * @return A singleton for GenieFileTransferService
     * @throws GenieException If there is any problem
     */
    @Bean
    public GenieFileTransferService cacheGenieFileTransferService(
        final FileTransferFactory fileTransferFactory,
        @Value("${genie.file.cache.location}") final String baseCacheLocation,
        final LocalFileTransferImpl localFileTransfer,
        final Registry registry
    ) throws GenieException {
        return new CacheGenieFileTransferService(fileTransferFactory, baseCacheLocation, localFileTransfer, registry);
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
     * Get an instance of the JobCoordinatorService.
     *
     * @param jobPersistenceService implementation of job persistence service interface
     * @param jobKillService        The job kill service to use
     * @param jobStateService       The running job metrics service to use
     * @param jobSearchService      Implementation of job search service interface
     * @param jobsProperties        The jobs properties to use
     * @param applicationService    Implementation of application service interface
     * @param clusterService        Implementation of cluster service interface
     * @param commandService        Implementation of command service interface
     * @param clusterLoadBalancers  Implementations of the cluster load balancer interface in invocation order
     * @param registry              The metrics registry to use
     * @param hostName              The host this Genie instance is running on
     * @return An instance of the JobCoordinatorService.
     */
    @Bean
    public JobCoordinatorService jobCoordinatorService(
        final JobPersistenceService jobPersistenceService,
        final JobKillService jobKillService,
        @Qualifier("jobMonitoringCoordinator") final JobStateService jobStateService,
        final JobSearchService jobSearchService,
        final JobsProperties jobsProperties,
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
     * @param jobsProperties All properties related to jobs
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
}
