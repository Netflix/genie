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
import com.netflix.genie.web.properties.DataServiceRetryProperties;
import com.netflix.genie.web.properties.FileCacheProperties;
import com.netflix.genie.web.properties.HealthProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.AgentJobService;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobFileService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.JobSubmitterService;
import com.netflix.genie.web.services.impl.AgentJobServiceImpl;
import com.netflix.genie.web.services.impl.CacheGenieFileTransferService;
import com.netflix.genie.web.services.impl.DiskJobFileServiceImpl;
import com.netflix.genie.web.services.impl.FileSystemAttachmentService;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.services.impl.JobCoordinatorServiceImpl;
import com.netflix.genie.web.services.impl.JobSpecificationServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.services.impl.LocalJobKillServiceImpl;
import com.netflix.genie.web.services.impl.LocalJobRunner;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

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
        DataServiceRetryProperties.class,
        FileCacheProperties.class,
        HealthProperties.class,
        JobsProperties.class,
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
    @ConditionalOnMissingBean(name = "fileTransferFactory", value = ServiceLocatorFactoryBean.class)
    public ServiceLocatorFactoryBean fileTransferFactory() {
        final ServiceLocatorFactoryBean factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(FileTransferFactory.class);
        return factoryBean;
    }

    /**
     * Get a {@link AgentJobService} instance if there isn't already one.
     *
     * @param jobPersistenceService   The persistence service to use
     * @param jobSpecificationService The specification service to use
     * @param meterRegistry           The metrics registry to use
     * @return An {@link AgentJobServiceImpl} instance.
     */
    @Bean
    @ConditionalOnMissingBean(AgentJobService.class)
    public AgentJobService agentJobService(
        final JobPersistenceService jobPersistenceService,
        final JobSpecificationService jobSpecificationService,
        final MeterRegistry meterRegistry
    ) {
        return new AgentJobServiceImpl(
            jobPersistenceService,
            jobSpecificationService,
            meterRegistry
        );
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
    public JobFileService jobFileService(@Qualifier("jobsDir") final Resource jobsDir) throws IOException {
        return new DiskJobFileServiceImpl(jobsDir);
    }

    /**
     * Get an implementation of {@link JobSpecificationService} if one hasn't already been defined.
     *
     * @param applicationPersistenceService The service to use to manipulate applications
     * @param clusterPersistenceService     The service to use to manipulate clusters
     * @param commandPersistenceService     The service to use to manipulate commands
     * @param clusterLoadBalancers          The load balancer implementations to use
     * @param registry                      The metrics repository to use
     * @param jobsProperties                The properties for running a job set by the user
     * @return A {@link JobSpecificationServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobSpecificationService.class)
    public JobSpecificationService jobSpecificationService(
        final ApplicationPersistenceService applicationPersistenceService,
        final ClusterPersistenceService clusterPersistenceService,
        final CommandPersistenceService commandPersistenceService,
        @NotEmpty final List<ClusterLoadBalancer> clusterLoadBalancers,
        final MeterRegistry registry,
        final JobsProperties jobsProperties
    ) {
        return new JobSpecificationServiceImpl(
            applicationPersistenceService,
            clusterPersistenceService,
            commandPersistenceService,
            clusterLoadBalancers,
            registry,
            jobsProperties
        );
    }
}
