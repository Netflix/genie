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
package com.netflix.genie.core.services.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.core.events.JobScheduledEvent;
import com.netflix.genie.core.properties.JobsProperties;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobKillService;
import com.netflix.genie.core.services.JobMetricsService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.springframework.context.ApplicationEventPublisher;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Unit tests for JobCoordinatorServiceImpl.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class JobCoordinatorServiceImplUnitTests {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String JOB_1_USER = "einstein";
    private static final String JOB_1_VERSION = "1.0";
    private static final String BASE_ARCHIVE_LOCATION = "file://baselocation";
    private static final String HOST_NAME = UUID.randomUUID().toString();
    private static final int MEMORY = 1_512;

    private JobCoordinatorServiceImpl jobCoordinatorService;
    private JobPersistenceService jobPersistenceService;
    private JobKillService jobKillService;
    private JobMetricsService jobMetricsService;
    private ApplicationEventPublisher eventPublisher;
    private ApplicationService applicationService;
    private ClusterService clusterService;
    private CommandService commandService;
    private ClusterLoadBalancer clusterLoadBalancer;
    private JobsProperties jobsProperties;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.jobKillService = Mockito.mock(JobKillService.class);
        this.jobMetricsService = Mockito.mock(JobMetricsService.class);
        this.eventPublisher = Mockito.mock(ApplicationEventPublisher.class);
        this.jobsProperties = new JobsProperties();
        this.jobsProperties.getLocations().setArchives(BASE_ARCHIVE_LOCATION);
        this.jobsProperties.getMemory().setDefaultJobMemory(MEMORY);
        this.applicationService = Mockito.mock(ApplicationService.class);
        this.clusterService = Mockito.mock(ClusterService.class);
        this.commandService = Mockito.mock(CommandService.class);
        this.clusterLoadBalancer = Mockito.mock(ClusterLoadBalancer.class);

        final Registry registry = Mockito.mock(Registry.class);
        Mockito.when(registry.timer(Mockito.anyString())).thenReturn(Mockito.mock(Timer.class));

        this.jobCoordinatorService = new JobCoordinatorServiceImpl(
            this.jobPersistenceService,
            this.jobKillService,
            this.jobMetricsService,
            jobsProperties,
            this.applicationService,
            this.clusterService,
            this.commandService,
            this.clusterLoadBalancer,
            registry,
            this.eventPublisher,
            HOST_NAME
        );
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canCoordinateJob() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final List<Cluster> clusters = Lists.newArrayList(cluster);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));

        Mockito
            .when(this.clusterService.chooseClusterForJobRequest(jobRequest))
            .thenReturn(clusters);

        Mockito.when(this.clusterLoadBalancer.selectCluster(clusters)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        final Set<String> commandTags = Sets.newHashSet(UUID.randomUUID().toString());
        commandTags.addAll(commandCriteria);
        Mockito.when(command.getTags()).thenReturn(commandTags);

        Mockito
            .when(
                this.clusterService.getCommandsForCluster(Mockito.eq(clusterId), Mockito.anySetOf(CommandStatus.class))
            )
            .thenReturn(Lists.newArrayList(command));

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito.when(this.jobMetricsService.getUsedMemory()).thenReturn(0);

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .createJob(
                Mockito.any(JobRequest.class),
                Mockito.any(JobMetadata.class),
                Mockito.any(Job.class),
                Mockito.any(JobExecution.class)
            );

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .updateJobWithRuntimeEnvironment(JOB_1_ID, clusterId, commandId, Lists.newArrayList(applicationId), MEMORY);

        Mockito.verify(this.eventPublisher, Mockito.times(1)).publishEvent(Mockito.any(JobScheduledEvent.class));
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canCoordinateJobWithSubmittedApplications() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );
        final String applicationId = UUID.randomUUID().toString();

        final JobRequest jobRequest
            = this.getJobRequest(true, commandCriteria, null, Lists.newArrayList(applicationId));
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final List<Cluster> clusters = Lists.newArrayList(cluster);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));

        Mockito
            .when(this.clusterService.chooseClusterForJobRequest(jobRequest))
            .thenReturn(clusters);

        Mockito.when(this.clusterLoadBalancer.selectCluster(clusters)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        final Set<String> commandTags = Sets.newHashSet(UUID.randomUUID().toString());
        commandTags.addAll(commandCriteria);
        Mockito.when(command.getTags()).thenReturn(commandTags);

        Mockito
            .when(
                this.clusterService.getCommandsForCluster(Mockito.eq(clusterId), Mockito.anySetOf(CommandStatus.class))
            )
            .thenReturn(Lists.newArrayList(command));

        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));

        Mockito.when(this.applicationService.getApplication(applicationId)).thenReturn(application);

        Mockito.when(this.jobMetricsService.getUsedMemory()).thenReturn(0);

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .createJob(
                Mockito.any(JobRequest.class),
                Mockito.any(JobMetadata.class),
                Mockito.any(Job.class),
                Mockito.any(JobExecution.class)
            );

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .updateJobWithRuntimeEnvironment(JOB_1_ID, clusterId, commandId, Lists.newArrayList(applicationId), MEMORY);

        Mockito.verify(this.eventPublisher, Mockito.times(1)).publishEvent(Mockito.any(JobScheduledEvent.class));
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCoordinateJobIfTooMuchMemoryRequested() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final int jobMemory = this.jobsProperties.getMemory().getMaxJobMemory() + 1;
        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, jobMemory, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final List<Cluster> clusters = Lists.newArrayList(cluster);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));

        Mockito
            .when(this.clusterService.chooseClusterForJobRequest(jobRequest))
            .thenReturn(clusters);

        Mockito.when(this.clusterLoadBalancer.selectCluster(clusters)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        final Set<String> commandTags = Sets.newHashSet(UUID.randomUUID().toString());
        commandTags.addAll(commandCriteria);
        Mockito.when(command.getTags()).thenReturn(commandTags);

        Mockito
            .when(
                this.clusterService.getCommandsForCluster(Mockito.eq(clusterId), Mockito.anySetOf(CommandStatus.class))
            )
            .thenReturn(Lists.newArrayList(command));

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobMetricsService, Mockito.never()).getUsedMemory();
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerUnavailableException.class)
    public void cantCoordinateJobIfNotEnoughMemoryAvailable() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final List<Cluster> clusters = Lists.newArrayList(cluster);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));

        Mockito
            .when(this.clusterService.chooseClusterForJobRequest(jobRequest))
            .thenReturn(clusters);

        Mockito.when(this.clusterLoadBalancer.selectCluster(clusters)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Set<String> commandTags = Sets.newHashSet(UUID.randomUUID().toString());
        commandTags.addAll(commandCriteria);
        Mockito.when(command.getTags()).thenReturn(commandTags);

        Mockito
            .when(
                this.clusterService.getCommandsForCluster(Mockito.eq(clusterId), Mockito.anySetOf(CommandStatus.class))
            )
            .thenReturn(Lists.newArrayList(command));

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito
            .when(this.jobMetricsService.getUsedMemory())
            .thenReturn(this.jobsProperties.getMemory().getMaxSystemMemory());

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobMetricsService, Mockito.times(1)).getUsedMemory();
        Mockito
            .verify(this.jobPersistenceService, Mockito.times(1))
            .updateJobStatus(Mockito.eq(JOB_1_ID), Mockito.eq(JobStatus.FAILED), Mockito.anyString());
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = RuntimeException.class)
    public void cantCoordinateJobIfTaskDoesntLaunch() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final List<Cluster> clusters = Lists.newArrayList(cluster);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));

        Mockito
            .when(this.clusterService.chooseClusterForJobRequest(jobRequest))
            .thenReturn(clusters);

        Mockito.when(this.clusterLoadBalancer.selectCluster(clusters)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Set<String> commandTags = Sets.newHashSet(UUID.randomUUID().toString());
        commandTags.addAll(commandCriteria);
        Mockito.when(command.getTags()).thenReturn(commandTags);

        Mockito
            .when(
                this.clusterService.getCommandsForCluster(Mockito.eq(clusterId), Mockito.anySetOf(CommandStatus.class))
            )
            .thenReturn(Lists.newArrayList(command));

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);
        Mockito.doThrow(new RuntimeException()).when(eventPublisher).publishEvent(Mockito.any());
        Mockito
            .when(this.jobMetricsService.getUsedMemory())
            .thenReturn(0);

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobMetricsService, Mockito.times(1)).getUsedMemory();
        Mockito
            .verify(this.jobPersistenceService, Mockito.times(1))
            .updateJobStatus(Mockito.eq(JOB_1_ID), Mockito.eq(JobStatus.FAILED), Mockito.anyString());
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCoordinateJobIfNoCommand() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final List<Cluster> clusters = Lists.newArrayList(cluster);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));

        Mockito
            .when(this.clusterService.chooseClusterForJobRequest(jobRequest))
            .thenReturn(clusters);

        Mockito.when(this.clusterLoadBalancer.selectCluster(clusters)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Set<String> commandTags = Sets.newHashSet(UUID.randomUUID().toString());
        Mockito.when(command.getTags()).thenReturn(commandTags);

        Mockito
            .when(
                this.clusterService.getCommandsForCluster(Mockito.eq(clusterId), Mockito.anySetOf(CommandStatus.class))
            )
            .thenReturn(Lists.newArrayList(command));

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
    }

    /**
     * Make sure if the job request doesn't have an id the method throws exception.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieServerException.class)
    public void cantCoordinateIfNoId() throws GenieException {
        final JobRequest request = Mockito.mock(JobRequest.class);
        Mockito.when(request.getId()).thenReturn(Optional.empty());
        this.jobCoordinatorService.coordinateJob(request, Mockito.mock(JobMetadata.class));
    }

    /**
     * Test killing a job without throwing an exception.
     *
     * @throws GenieException On any error
     */
    @Test
    public void canKillJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.doNothing().when(this.jobKillService).killJob(id);
        this.jobCoordinatorService.killJob(id);
    }

    /**
     * Test killing a job without throwing an exception.
     *
     * @throws GenieException On any error
     */
    @Test(expected = GenieException.class)
    public void cantKillJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.doThrow(new GenieException(123, "fake")).when(this.jobKillService).killJob(id);
        this.jobCoordinatorService.killJob(id);
    }

    private JobRequest getJobRequest(
        final boolean disableLogArchival,
        final Set<String> commandCriteria,
        final Integer memory,
        final List<String> applications
    ) {
        final String email = "name@domain.com";
        final String setupFile = "setupFilePath";
        final String group = "group";
        final String description = "job description";
        final Set<String> tags = new HashSet<>();
        tags.add("foo");
        tags.add("bar");

        return new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            null,
            null,
            commandCriteria
        ).withId(JOB_1_ID)
            .withDescription(description)
            .withEmail(email)
            .withSetupFile(setupFile)
            .withGroup(group)
            .withTags(tags)
            .withDisableLogArchival(disableLogArchival)
            .withMemory(memory)
            .withApplications(applications)
            .build();
    }

    private JobMetadata getJobMetadata() {
        return new JobMetadata
            .Builder()
            .withClientHost("localhost")
            .withUserAgent(UUID.randomUUID().toString())
            .withNumAttachments(2)
            .withTotalSizeOfAttachments(28080L)
            .build();
    }
}
