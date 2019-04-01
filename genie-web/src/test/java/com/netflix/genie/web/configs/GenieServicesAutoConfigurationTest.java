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

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.common.util.GenieObjectMapper;
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
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.FileTransferFactory;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobKillServiceV4;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.impl.JobKillServiceV3;
import com.netflix.genie.web.services.impl.LocalFileTransferImpl;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unit Tests for ServicesConfig class.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class GenieServicesAutoConfigurationTest {

    private GenieServicesAutoConfiguration genieServicesAutoConfiguration;

    /**
     * Setup to run before each test.
     */
    @Before
    public void setUp() {
        this.genieServicesAutoConfiguration = new GenieServicesAutoConfiguration();
    }


    /**
     * Can get jobs properties bean.
     */
    @Test
    public void canGetJobPropertiesBean() {
        Assert.assertNotNull(
            this.genieServicesAutoConfiguration.jobsProperties(
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
            this.genieServicesAutoConfiguration.jobKillServiceV3(
                new GenieHostInfo("localhost"),
                Mockito.mock(JobSearchService.class),
                Mockito.mock(Executor.class),
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(GenieEventBus.class),
                Mockito.mock(FileSystemResource.class),
                GenieObjectMapper.getMapper()
            )
        );
    }

    /**
     * Can get a bean for Job Kill Service.
     */
    @Test
    public void canGetJobKillServiceBean() {
        Assert.assertNotNull(
            this.genieServicesAutoConfiguration.jobKillService(
                Mockito.mock(JobKillServiceV3.class),
                Mockito.mock(JobKillServiceV4.class),
                Mockito.mock(JobPersistenceService.class)
            )
        );
    }

    /**
     * Can get the fallback V4 Kill service.
     *
     * @throws GenieException in case of error
     */
    @Test(expected = NotImplementedException.class)
    public void canGetFallbackJobKillServiceV4Bean() throws GenieException {
        final JobKillServiceV4 service = this.genieServicesAutoConfiguration.fallbackJobKillServiceV4();
        Assert.assertNotNull(service);

        service.killJob(UUID.randomUUID().toString(), "test");
        Assert.fail("Expected exception");
    }


    /**
     * Confirm we can get a GenieFileTransfer instance.
     *
     * @throws GenieException If there is any problem.
     */
    @Test
    public void canGetGenieFileTransferServiceBean() throws GenieException {
        Assert.assertNotNull(this.genieServicesAutoConfiguration.genieFileTransferService(scheme -> null));
    }

    /**
     * Confirm we can get a GenieFileTransfer instance.
     *
     * @throws GenieException If there is any problem.
     */
    @Test
    public void canGetCacheGenieFileTransferServiceBean() throws GenieException {
        final FileCacheProperties cacheProperties = Mockito.mock(FileCacheProperties.class);
        Mockito.when(cacheProperties.getLocation()).thenReturn(".");
        Assert.assertNotNull(
            this.genieServicesAutoConfiguration.cacheGenieFileTransferService(
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
            this.genieServicesAutoConfiguration.jobSubmitterService(
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
            this.genieServicesAutoConfiguration.jobCoordinatorService(
                Mockito.mock(JobPersistenceService.class),
                Mockito.mock(JobKillService.class),
                Mockito.mock(JobStateService.class),
                Mockito.mock(JobSearchService.class),
                JobsProperties.getJobsPropertiesDefaults(),
                Mockito.mock(ApplicationPersistenceService.class),
                Mockito.mock(ClusterPersistenceService.class),
                Mockito.mock(CommandPersistenceService.class),
                Mockito.mock(JobSpecificationService.class),
                Mockito.mock(MeterRegistry.class),
                new GenieHostInfo(UUID.randomUUID().toString())
            )
        );
    }
}
