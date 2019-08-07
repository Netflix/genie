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
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.common.util.GenieObjectMapper;
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
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.impl.JobKillServiceV3;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import com.netflix.genie.web.util.ProcessChecker;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unit Tests for {@link ServicesAutoConfiguration} class.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class ServicesAutoConfigurationTest {

    /**
     * Temporary folder for tests.
     */
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private ServicesAutoConfiguration servicesAutoConfiguration;

    /**
     * Setup to run before each test.
     */
    @Before
    public void setUp() {
        this.servicesAutoConfiguration = new ServicesAutoConfiguration();
    }


    /**
     * Can get jobs properties bean.
     */
    @Test
    public void canGetJobPropertiesBean() {
        Assert.assertNotNull(
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
        );
    }

    /**
     * Can get a bean for killing V3 jobs.
     */
    @Test
    public void canGetJobKillServiceV3Bean() {
        Assert.assertNotNull(
            this.servicesAutoConfiguration.jobKillServiceV3(
                new GenieHostInfo("localhost"),
                Mockito.mock(JobSearchService.class),
                Mockito.mock(Executor.class),
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(GenieEventBus.class),
                Mockito.mock(FileSystemResource.class),
                GenieObjectMapper.getMapper(),
                Mockito.mock(ProcessChecker.Factory.class)
            )
        );
    }

    /**
     * Can get a bean for Job Kill Service.
     */
    @Test
    public void canGetJobKillServiceBean() {
        Assert.assertNotNull(
            this.servicesAutoConfiguration.jobKillService(
                Mockito.mock(JobKillServiceV3.class),
                Mockito.mock(JobKillServiceV4.class),
                Mockito.mock(JobPersistenceService.class)
            )
        );
    }


    /**
     * Confirm we can get a GenieFileTransfer instance.
     *
     * @throws GenieException If there is any problem.
     */
    @Test
    public void canGetGenieFileTransferServiceBean() throws GenieException {
        Assert.assertNotNull(this.servicesAutoConfiguration.genieFileTransferService(scheme -> null));
    }

    /**
     * Confirm we can get a GenieFileTransfer instance.
     *
     * @throws GenieException If there is any problem.
     * @throws IOException    On error creating temporary folder
     */
    @Test
    public void canGetCacheGenieFileTransferServiceBean() throws GenieException, IOException {
        final FileCacheProperties cacheProperties = Mockito.mock(FileCacheProperties.class);
        Mockito.when(cacheProperties.getLocation()).thenReturn(this.temporaryFolder.newFolder().toURI());
        Assert.assertNotNull(
            this.servicesAutoConfiguration.cacheGenieFileTransferService(
                Mockito.mock(FileTransferFactory.class),
                cacheProperties,
                Mockito.mock(LocalFileTransferImpl.class),
                Mockito.mock(MeterRegistry.class)
            )
        );
    }

    /**
     * Can get a bean for Job Submitter Service.
     */
    @Test
    public void canGetJobSubmitterServiceBean() {
        final JobPersistenceService jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        final GenieEventBus genieEventBus = Mockito.mock(GenieEventBus.class);
        final Resource resource = Mockito.mock(Resource.class);
        final List<WorkflowTask> workflowTasks = new ArrayList<>();

        Assert.assertNotNull(
            this.servicesAutoConfiguration.jobSubmitterService(
                jobPersistenceService,
                genieEventBus,
                workflowTasks,
                resource,
                Mockito.mock(MeterRegistry.class)
            )
        );
    }

    /**
     * Can get a bean for Job Coordinator Service.
     */
    @Test
    public void canGetJobCoordinatorServiceBean() {
        Assert.assertNotNull(
            this.servicesAutoConfiguration.jobCoordinatorService(
                Mockito.mock(JobPersistenceService.class),
                Mockito.mock(JobKillService.class),
                Mockito.mock(JobStateService.class),
                Mockito.mock(JobSearchService.class),
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(ApplicationPersistenceService.class),
                Mockito.mock(ClusterPersistenceService.class),
                Mockito.mock(CommandPersistenceService.class),
                Mockito.mock(JobResolverService.class),
                Mockito.mock(MeterRegistry.class),
                new GenieHostInfo(UUID.randomUUID().toString())
            )
        );
    }
}
