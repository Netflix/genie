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
import com.netflix.genie.common.util.GenieObjectMapper;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.services.JobStateService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.exec.Executor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.mail.javamail.JavaMailSender;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Unit Tests for ServicesConfig class.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class ServicesConfigUnitTests {

    private JobSearchService jobSearchService;
    private ServicesConfig servicesConfig;

    /**
     * Setup to run before each test.
     */
    @Before
    public void setUp() {
        this.jobSearchService = Mockito.mock(JobSearchService.class);

        this.servicesConfig = new ServicesConfig();
    }

    /**
     * Confirm we can get a cluster load balancer.
     */
    @Test
    public void canGetClusterLoadBalancer() {
        Assert.assertNotNull(this.servicesConfig.clusterLoadBalancer());
    }

    /**
     * Confirm we can get a GenieFileTransfer instance.
     *
     * @throws GenieException If there is any problem.
     */
    @Test
    public void canGetGenieFileTransfer() throws GenieException {
        Assert.assertNotNull(this.servicesConfig.genieFileTransferService(scheme -> null));
    }

    /**
     * Confirm we can get a default mail service implementation.
     */
    @Test
    public void canGetDefaultMailServiceImpl() {
        Assert.assertNotNull(this.servicesConfig.getDefaultMailServiceImpl());
    }

    /**
     * Confirm we can get a mail service implementation using JavaMailSender.
     */
    @Test
    public void canGetMailServiceImpl() {
        final JavaMailSender javaMailSender = Mockito.mock(JavaMailSender.class);
        Assert.assertNotNull(this.servicesConfig.getJavaMailSenderMailService(javaMailSender, "fromAddress"));
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
            this.servicesConfig.jobSubmitterService(
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
            this.servicesConfig.jobCoordinatorService(
                Mockito.mock(JobPersistenceService.class),
                Mockito.mock(JobKillService.class),
                Mockito.mock(JobStateService.class),
                Mockito.mock(JobSearchService.class),
                new JobsProperties(),
                Mockito.mock(ApplicationPersistenceService.class),
                Mockito.mock(ClusterPersistenceService.class),
                Mockito.mock(CommandPersistenceService.class),
                Mockito.mock(JobSpecificationService.class),
                Mockito.mock(MeterRegistry.class),
                UUID.randomUUID().toString()
            )
        );
    }

    /**
     * Can get a bean for Job Kill Service.
     */
    @Test
    public void canGetJobKillServiceBean() {
        Assert.assertNotNull(
            this.servicesConfig.jobKillService(
                "localhost",
                this.jobSearchService,
                Mockito.mock(Executor.class),
                new JobsProperties(),
                Mockito.mock(GenieEventBus.class),
                Mockito.mock(FileSystemResource.class),
                GenieObjectMapper.getMapper()
            )
        );
    }
}
