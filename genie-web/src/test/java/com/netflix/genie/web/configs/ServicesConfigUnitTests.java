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
import com.netflix.genie.web.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.web.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.web.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.web.jpa.repositories.JpaFileRepository;
import com.netflix.genie.web.jpa.repositories.JpaJobRepository;
import com.netflix.genie.web.jpa.repositories.JpaTagRepository;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.ApplicationPersistenceService;
import com.netflix.genie.web.services.ClusterPersistenceService;
import com.netflix.genie.web.services.CommandPersistenceService;
import com.netflix.genie.web.services.FilePersistenceService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobSpecificationService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.services.TagPersistenceService;
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

    private TagPersistenceService tagPersistenceService;
    private JpaTagRepository tagRepository;
    private FilePersistenceService filePersistenceService;
    private JpaFileRepository fileRepository;
    private JpaApplicationRepository applicationRepository;
    private JpaClusterRepository clusterRepository;
    private JpaCommandRepository commandRepository;
    private JpaJobRepository jobRepository;
    private JobSearchService jobSearchService;
    private ServicesConfig servicesConfig;

    /**
     * Setup to run before each test.
     */
    @Before
    public void setUp() {
        this.tagPersistenceService = Mockito.mock(TagPersistenceService.class);
        this.tagRepository = Mockito.mock(JpaTagRepository.class);
        this.filePersistenceService = Mockito.mock(FilePersistenceService.class);
        this.fileRepository = Mockito.mock(JpaFileRepository.class);
        this.applicationRepository = Mockito.mock(JpaApplicationRepository.class);
        this.clusterRepository = Mockito.mock(JpaClusterRepository.class);
        this.commandRepository = Mockito.mock(JpaCommandRepository.class);
        this.jobRepository = Mockito.mock(JpaJobRepository.class);
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
     * Can get a bean for Application Service.
     */
    @Test
    public void canGetApplicationServiceBean() {
        Assert.assertNotNull(
            this.servicesConfig.applicationService(
                this.tagPersistenceService,
                this.tagRepository,
                this.filePersistenceService,
                this.fileRepository,
                this.applicationRepository,
                this.commandRepository
            )
        );
    }

    /**
     * Can get a bean for Command Service.
     */
    @Test
    public void canGetCommandServiceBean() {

        Assert.assertNotNull(
            this.servicesConfig.commandService(
                this.tagPersistenceService,
                this.tagRepository,
                this.filePersistenceService,
                this.fileRepository,
                this.commandRepository,
                this.applicationRepository,
                this.clusterRepository
            )
        );
    }

    /**
     * Can get a bean for Cluster Service.
     */
    @Test
    public void canGetClusterServiceBean() {
        Assert.assertNotNull(
            this.servicesConfig.clusterService(
                this.tagPersistenceService,
                this.tagRepository,
                this.filePersistenceService,
                this.fileRepository,
                this.clusterRepository,
                this.commandRepository
            )
        );
    }

    /**
     * Can get a bean for Job Search Service.
     */
    @Test
    public void canGetJobSearchServiceBean() {
        Assert.assertNotNull(
            this.servicesConfig.jobSearchService(
                this.jobRepository,
                Mockito.mock(JpaClusterRepository.class),
                Mockito.mock(JpaCommandRepository.class)
            )
        );
    }

    /**
     * Can get a bean for Job Persistence Service.
     */
    @Test
    public void canGetJobPersistenceServiceBean() {
        Assert.assertNotNull(
            this.servicesConfig.jobPersistenceService(
                this.tagPersistenceService,
                this.tagRepository,
                this.filePersistenceService,
                this.fileRepository,
                this.jobRepository,
                this.applicationRepository,
                this.clusterRepository,
                this.commandRepository
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

    /**
     * Make sure a tag service bean can be created.
     */
    @Test
    public void canGetTagServiceBean() {
        Assert.assertNotNull(this.servicesConfig.tagService(this.tagRepository));
    }

    /**
     * Make sure a tag service bean can be created.
     */
    @Test
    public void canGetFileServiceBean() {
        Assert.assertNotNull(this.servicesConfig.fileService(this.fileRepository));
    }
}
