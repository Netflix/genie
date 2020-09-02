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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import com.netflix.genie.common.internal.aws.s3.S3ClientFactory;
import com.netflix.genie.common.internal.services.JobDirectoryManifestCreatorService;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentFileStreamService;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
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
import com.netflix.genie.web.services.ArchivedJobService;
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobFileService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.impl.JobKillServiceV3;
import com.netflix.genie.web.services.impl.LocalFileSystemAttachmentServiceImpl;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.services.impl.S3AttachmentServiceImpl;
import com.netflix.genie.web.util.ProcessChecker;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unit Tests for {@link ServicesAutoConfiguration} class.
 *
 * @author amsharma
 * @since 3.0.0
 */
class ServicesAutoConfigurationTest {
    //TODO update this test class to use ContextRunner, like the rest of configuration tests

    private ServicesAutoConfiguration servicesAutoConfiguration;

    @BeforeEach
    void setUp() {
        this.servicesAutoConfiguration = new ServicesAutoConfiguration();
    }

    @Test
    void canGetJobPropertiesBean() {
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobsProperties(
                    Mockito.mock(JobsCleanupProperties.class),
                    Mockito.mock(JobsForwardingProperties.class),
                    Mockito.mock(JobsLocationsProperties.class),
                    Mockito.mock(JobsMaxProperties.class),
                    Mockito.mock(JobsMemoryProperties.class),
                    Mockito.mock(JobsUsersProperties.class),
                    Mockito.mock(ExponentialBackOffTriggerProperties.class),
                    Mockito.mock(JobsActiveLimitProperties.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetJobKillServiceV3Bean() {
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(Mockito.mock(PersistenceService.class));
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobKillServiceV3(
                    new GenieHostInfo("localhost"),
                    dataServices,
                    Mockito.mock(Executor.class),
                    JobsProperties.getJobsPropertiesDefaults(),
                    Mockito.mock(GenieEventBus.class),
                    Mockito.mock(FileSystemResource.class),
                    GenieObjectMapper.getMapper(),
                    Mockito.mock(ProcessChecker.Factory.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetJobKillServiceBean() {
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(Mockito.mock(PersistenceService.class));
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobKillService(
                    Mockito.mock(JobKillServiceV3.class),
                    Mockito.mock(JobKillServiceV4.class),
                    dataServices
                )
            )
            .isNotNull();
    }

    @Test
    void canGetGenieFileTransferServiceBean() throws GenieException {
        Assertions.assertThat(this.servicesAutoConfiguration.genieFileTransferService(scheme -> null)).isNotNull();
    }

    @Test
    void canGetCacheGenieFileTransferServiceBean(@TempDir final Path tmpDir) throws GenieException {
        final FileCacheProperties cacheProperties = Mockito.mock(FileCacheProperties.class);
        Mockito.when(cacheProperties.getLocation()).thenReturn(tmpDir.toFile().toURI());
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.cacheGenieFileTransferService(
                    Mockito.mock(FileTransferFactory.class),
                    cacheProperties,
                    Mockito.mock(LocalFileTransferImpl.class),
                    Mockito.mock(MeterRegistry.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetJobSubmitterServiceBean() {
        final PersistenceService persistenceService = Mockito.mock(PersistenceService.class);
        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(persistenceService);
        final GenieEventBus genieEventBus = Mockito.mock(GenieEventBus.class);
        final Resource resource = Mockito.mock(Resource.class);
        final List<WorkflowTask> workflowTasks = new ArrayList<>();

        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobSubmitterService(
                    dataServices,
                    genieEventBus,
                    workflowTasks,
                    resource,
                    Mockito.mock(MeterRegistry.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetJobCoordinatorServiceBean() {
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobCoordinatorService(
                    Mockito.mock(DataServices.class),
                    Mockito.mock(JobKillService.class),
                    Mockito.mock(JobStateService.class),
                    JobsProperties.getJobsPropertiesDefaults(),
                    Mockito.mock(JobResolverService.class),
                    Mockito.mock(MeterRegistry.class),
                    new GenieHostInfo(UUID.randomUUID().toString())
                )
            )
            .isNotNull();
    }

    @Test
    void canGetJobDirectoryServerServiceBean() {
        Assertions
            .assertThat(
                this.servicesAutoConfiguration.jobDirectoryServerService(
                    Mockito.mock(ResourceLoader.class),
                    Mockito.mock(DataServices.class),
                    Mockito.mock(AgentFileStreamService.class),
                    Mockito.mock(ArchivedJobService.class),
                    Mockito.mock(MeterRegistry.class),
                    Mockito.mock(JobFileService.class),
                    Mockito.mock(JobDirectoryManifestCreatorService.class),
                    Mockito.mock(AgentRoutingService.class)
                )
            )
            .isNotNull();
    }

    @Test
    void canGetS3AttachmentServiceServiceBean() throws IOException {

        final AttachmentServiceProperties properties = new AttachmentServiceProperties();

        Assertions
            .assertThat(
                this.servicesAutoConfiguration.attachmentService(
                    Mockito.mock(S3ClientFactory.class),
                    properties,
                    Mockito.mock(MeterRegistry.class)
                )
            )
            .isInstanceOf(LocalFileSystemAttachmentServiceImpl.class);

        properties.setLocationPrefix(URI.create("s3://foo/bar/genie/attachments"));

        Assertions
            .assertThat(
                this.servicesAutoConfiguration.attachmentService(
                    Mockito.mock(S3ClientFactory.class),
                    properties,
                    Mockito.mock(MeterRegistry.class)
                )
            )
            .isInstanceOf(S3AttachmentServiceImpl.class);
    }
}
