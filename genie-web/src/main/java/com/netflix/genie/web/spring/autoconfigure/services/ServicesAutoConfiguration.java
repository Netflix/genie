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

import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.properties.AttachmentServiceProperties;
import com.netflix.genie.web.properties.JobsActiveLimitProperties;
import com.netflix.genie.web.properties.JobsForwardingProperties;
import com.netflix.genie.web.properties.JobsLocationsProperties;
import com.netflix.genie.web.properties.JobsMemoryProperties;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.properties.JobsUsersProperties;
import com.netflix.genie.web.selectors.AgentLauncherSelector;
import com.netflix.genie.web.selectors.ClusterSelector;
import com.netflix.genie.web.selectors.CommandSelector;
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.RequestForwardingService;
import com.netflix.genie.web.services.impl.ArchivedJobServiceImpl;
import com.netflix.genie.web.services.impl.JobDirectoryServerServiceImpl;
import com.netflix.genie.web.services.impl.JobLaunchServiceImpl;
import com.netflix.genie.web.services.impl.JobResolverServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileSystemAttachmentServiceImpl;
import com.netflix.genie.web.services.impl.RequestForwardingServiceImpl;
import com.netflix.genie.web.services.impl.S3AttachmentServiceImpl;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.client.RestTemplate;

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
        JobsForwardingProperties.class,
        JobsLocationsProperties.class,
        JobsMemoryProperties.class,
        JobsUsersProperties.class,
        JobsActiveLimitProperties.class,
        AttachmentServiceProperties.class
    }
)
@Slf4j
public class ServicesAutoConfiguration {

    /**
     * Collection of properties related to job execution.
     *
     * @param forwarding  forwarding properties
     * @param locations   locations properties
     * @param memory      memory properties
     * @param users       users properties
     * @param activeLimit active limit properties
     * @return a {@code JobsProperties} instance
     */
    @Bean
    public JobsProperties jobsProperties(
        final JobsForwardingProperties forwarding,
        final JobsLocationsProperties locations,
        final JobsMemoryProperties memory,
        final JobsUsersProperties users,
        final JobsActiveLimitProperties activeLimit
    ) {
        return new JobsProperties(
            forwarding,
            locations,
            memory,
            users,
            activeLimit
        );
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
     * Get an implementation of {@link JobResolverService} if one hasn't already been defined.
     *
     * @param dataServices      The {@link DataServices} encapsulation instance to use
     * @param clusterSelectors  The {@link ClusterSelector} implementations to use
     * @param commandSelector   The {@link CommandSelector} implementation to use
     * @param registry          The metrics repository to use
     * @param jobsProperties    The properties for running a job set by the user
     * @param environment       The Spring application {@link Environment} for dynamic property resolution
     * @param tracingComponents The {@link BraveTracingComponents} to use
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
        final Environment environment,
        final BraveTracingComponents tracingComponents
    ) {
        return new JobResolverServiceImpl(
            dataServices,
            clusterSelectors,
            commandSelector,
            registry,
            jobsProperties,
            environment,
            tracingComponents
        );
    }

    /**
     * Provide the default implementation of {@link JobDirectoryServerService} for serving job directory resources.
     *
     * @param resourceLoader         The application resource loader used to get references to resources
     * @param dataServices           The {@link DataServices} instance to use
     * @param agentFileStreamService The service to request a file from an agent running a job
     * @param archivedJobService     The {@link ArchivedJobService} implementation to use to get archived
     *                               job data
     * @param meterRegistry          The meter registry used to keep track of metrics
     * @param agentRoutingService    The agent routing service
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
        final AgentRoutingService agentRoutingService
    ) {
        return new JobDirectoryServerServiceImpl(
            resourceLoader,
            dataServices,
            agentFileStreamService,
            archivedJobService,
            meterRegistry,
            agentRoutingService
        );
    }

    /**
     * Provide a {@link JobLaunchService} implementation if one isn't available.
     *
     * @param dataServices          The {@link DataServices} instance to use
     * @param jobResolverService    The {@link JobResolverService} implementation to use
     * @param agentLauncherSelector The {@link AgentLauncherSelector} implementation to use
     * @param tracingComponents     The {@link BraveTracingComponents} instance to use
     * @param registry              The metrics registry to use
     * @return A {@link JobLaunchServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(JobLaunchService.class)
    public JobLaunchServiceImpl jobLaunchService(
        final DataServices dataServices,
        final JobResolverService jobResolverService,
        final AgentLauncherSelector agentLauncherSelector,
        final BraveTracingComponents tracingComponents,
        final MeterRegistry registry
    ) {
        return new JobLaunchServiceImpl(
            dataServices,
            jobResolverService,
            agentLauncherSelector,
            tracingComponents,
            registry
        );
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

    /**
     * Provide a default implementation of {@link RequestForwardingService} for use by other services.
     *
     * @param genieRestTemplate        The {@link RestTemplate} configured to be used to call other Genie nodes
     * @param hostInfo                 The {@link GenieHostInfo} instance containing introspection information about
     *                                 the current node
     * @param jobsForwardingProperties The properties for forwarding requests between Genie nodes
     * @return A {@link RequestForwardingServiceImpl} instance
     */
    @Bean
    @ConditionalOnMissingBean(RequestForwardingService.class)
    public RequestForwardingServiceImpl requestForwardingService(
        @Qualifier("genieRestTemplate") final RestTemplate genieRestTemplate,
        final GenieHostInfo hostInfo,
        final JobsForwardingProperties jobsForwardingProperties
    ) {
        return new RequestForwardingServiceImpl(genieRestTemplate, hostInfo, jobsForwardingProperties);
    }
}
