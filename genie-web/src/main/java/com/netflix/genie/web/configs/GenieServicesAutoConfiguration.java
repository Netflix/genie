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
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
import com.netflix.genie.web.properties.FileCacheProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.services.impl.CacheGenieFileTransferService;
import com.netflix.genie.web.services.impl.FileSystemAttachmentService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.JobCoordinatorServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.services.impl.LocalJobKillServiceImpl;
import com.netflix.genie.web.services.impl.LocalJobRunner;
import com.netflix.genie.web.services.impl.RandomizedClusterLoadBalancerImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

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
        FileCacheProperties.class
    }
)
public class GenieServicesAutoConfiguration {

    /**
     * Get an local implementation of the JobKillService.
     *
     * @param genieHostInfo    Information about the host the Genie process is running on
     * @param jobSearchService The job search service to use to locate job information.
     * @param executor         The executor to use to run system processes.
     * @param jobsProperties   The jobs properties to use
     * @param genieEventBus    The application event bus to use to publish system wide events
     * @param genieWorkingDir  Working directory for genie where it creates jobs directories.
     * @param objectMapper     The Jackson ObjectMapper used to serialize from/to JSON
     * @return A job kill service instance.
     */
    @Bean
    @ConditionalOnMissingBean(JobKillService.class)
    public JobKillService jobKillService(
        final GenieHostInfo genieHostInfo,
        final JobSearchService jobSearchService,
        final Executor executor,
        final JobsProperties jobsProperties,
        final GenieEventBus genieEventBus,
        @Qualifier("jobsDir") final Resource genieWorkingDir,
        final ObjectMapper objectMapper
    ) {
        return new LocalJobKillServiceImpl(
            genieHostInfo.getHostname(),
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
    @ConditionalOnMissingBean(ClusterLoadBalancer.class)
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
     * @param fileCacheProperties Properties related to the file cache that can be set by the admin
     * @param localFileTransfer   local file transfer service
     * @param registry            Registry
     * @return A singleton for GenieFileTransferService
     * @throws GenieException If there is any problem
     */
    @Bean
    public GenieFileTransferService cacheGenieFileTransferService(
        final FileTransferFactory fileTransferFactory,
        final FileCacheProperties fileCacheProperties,
        final LocalFileTransferImpl localFileTransfer,
        final MeterRegistry registry
    ) throws GenieException {
        return new CacheGenieFileTransferService(
            fileTransferFactory,
            fileCacheProperties.getLocation(),
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
     * @param specificationService          The job specification service to use
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
        final JobSpecificationService specificationService,
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
            specificationService,
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
