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
package com.netflix.genie.web.services.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.controllers.DtoAdapters;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.ApplicationService;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.services.ClusterService;
import com.netflix.genie.web.services.CommandService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private static final String KILL_REASON = "Killed by test";
    private static final boolean ACTIVE_JOBS_LIMIT_ENABLED = false;
    private static final Set<Tag> SUCCESS_TIMER_TAGS = MetricsUtils.newSuccessTagsSet();

    private JobCoordinatorServiceImpl jobCoordinatorService;
    private JobPersistenceService jobPersistenceService;
    private JobKillService jobKillService;
    private JobStateService jobStateService;
    private JobSearchService jobSearchService;
    private ApplicationService applicationService;
    private ClusterService clusterService;
    private CommandService commandService;
    private ClusterLoadBalancer clusterLoadBalancer1;
    private ClusterLoadBalancer clusterLoadBalancer2;
    private ClusterLoadBalancer clusterLoadBalancer3;
    private ClusterLoadBalancer clusterLoadBalancer4;
    private JobsProperties jobsProperties;
    @Captor
    private ArgumentCaptor<Set<Tag>> tagsCaptor;
    private MeterRegistry registry;
    private Timer coordinationTimer;
    private Timer selectClusterTimer;
    private Timer selectCommandTimer;
    private Timer selectApplicationsTimer;
    private Timer setJobEnvironmentTimer;
    private Timer clusterCommandQueryTimer;
    private Counter noClusterSelectedCounter;
    private Counter noMatchingClusterCounter;
    private Counter loadBalancerCounter;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.jobKillService = Mockito.mock(JobKillService.class);
        this.jobStateService = Mockito.mock(JobStateService.class);
        this.jobSearchService = Mockito.mock(JobSearchService.class);
        this.jobsProperties = new JobsProperties();
        this.jobsProperties.getLocations().setArchives(BASE_ARCHIVE_LOCATION);
        this.jobsProperties.getMemory().setDefaultJobMemory(MEMORY);
        this.jobsProperties.getUsers().getActiveLimit().setEnabled(ACTIVE_JOBS_LIMIT_ENABLED);
        this.applicationService = Mockito.mock(ApplicationService.class);
        this.clusterService = Mockito.mock(ClusterService.class);
        this.commandService = Mockito.mock(CommandService.class);
        this.clusterLoadBalancer1 = Mockito.mock(ClusterLoadBalancer.class);
        this.clusterLoadBalancer2 = Mockito.mock(ClusterLoadBalancer.class);
        this.clusterLoadBalancer3 = Mockito.mock(ClusterLoadBalancer.class);
        this.clusterLoadBalancer4 = Mockito.mock(ClusterLoadBalancer.class);

        this.registry = Mockito.mock(MeterRegistry.class);
        this.coordinationTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.coordinationTimer);
        this.selectClusterTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.selectClusterTimer);
        this.selectCommandTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.selectCommandTimer);
        this.selectApplicationsTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.selectApplicationsTimer);
        this.setJobEnvironmentTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.setJobEnvironmentTimer);
        this.noClusterSelectedCounter = Mockito.mock(Counter.class);
        Mockito
            .when(this.registry.counter("genie.jobs.submit.selectCluster.noneSelected.counter"))
            .thenReturn(this.noClusterSelectedCounter);
        this.noMatchingClusterCounter = Mockito.mock(Counter.class);
        Mockito
            .when(this.registry.counter("genie.jobs.submit.selectCluster.noneFound.counter"))
            .thenReturn(this.noMatchingClusterCounter);
        this.loadBalancerCounter = Mockito.mock(Counter.class);
        Mockito
            .when(
                this.registry.counter(
                    Mockito.eq(JobCoordinatorServiceImpl.SELECT_LOAD_BALANCER_COUNTER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.loadBalancerCounter);
        this.clusterCommandQueryTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.CLUSTER_COMMAND_QUERY_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.clusterCommandQueryTimer);

        this.jobCoordinatorService = new JobCoordinatorServiceImpl(
            this.jobPersistenceService,
            this.jobKillService,
            this.jobStateService,
            this.jobsProperties,
            this.applicationService,
            this.jobSearchService,
            this.clusterService,
            this.commandService,
            Lists.newArrayList(
                this.clusterLoadBalancer1,
                this.clusterLoadBalancer2,
                this.clusterLoadBalancer3,
                this.clusterLoadBalancer4
            ),
            this.registry,
            HOST_NAME
        );
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCoordinateJobIfNoClustersFound() throws GenieException {

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(Maps.newHashMap());

        Mockito.verifyNoMoreInteractions(
            this.selectApplicationsTimer,
            this.selectClusterTimer,
            this.setJobEnvironmentTimer
        );

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GeniePreconditionException("test"))
                );
            Mockito
                .verify(this.noMatchingClusterCounter, Mockito.times(1))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GeniePreconditionException("test"))
                );
        }
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCoordinateJobIfNoClustersSelected() throws GenieException {

        final Set<String> commandCriteria = Sets.newHashSet(
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final Map<Cluster, String> clusterCommandMap = Maps.newHashMap();
        clusterCommandMap.put(Mockito.mock(Cluster.class), UUID.randomUUID().toString());
        clusterCommandMap.put(Mockito.mock(Cluster.class), UUID.randomUUID().toString());

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clusterCommandMap);

        Mockito.verifyNoMoreInteractions(
            this.selectApplicationsTimer,
            this.selectClusterTimer,
            this.setJobEnvironmentTimer
        );

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GeniePreconditionException("test"))
                );
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(1))
                .increment();
            Mockito
                .verify(this.registry, Mockito.times(4))
                .counter(
                    Mockito.eq(JobCoordinatorServiceImpl.SELECT_LOAD_BALANCER_COUNTER_NAME),
                    this.tagsCaptor.capture()
                );
            final Set<Tag> finalTags = this.tagsCaptor.getAllValues().get(3);
            Assert.assertThat(finalTags.size(), Matchers.is(2));
            for (final Tag tag : finalTags) {
                if (tag.getKey().equals(MetricsConstants.TagKeys.CLASS_NAME)) {
                    Assert.assertThat(
                        tag.getValue(),
                        Matchers.startsWith("com.netflix.genie.web.services.ClusterLoadBalancer")
                    );
                } else if (tag.getKey().equals(MetricsConstants.TagKeys.STATUS)) {
                    Assert.assertThat(tag.getValue(), Matchers.is("no preference"));
                } else {
                    Assert.fail();
                }
            }
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(4))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GeniePreconditionException("test"))
                );
            Mockito
                .verify(this.clusterCommandQueryTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        }
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canCoordinateJobForSingleCluster() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final String commandId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        final Command command = Mockito.mock(Command.class);
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito.when(this.jobStateService.getUsedMemory()).thenReturn(0);

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .createJob(
                Mockito.any(JobRequest.class),
                Mockito.any(JobMetadata.class),
                Mockito.any(Job.class),
                Mockito.any(JobExecution.class)
            );

        Mockito.verify(
            this.jobPersistenceService,
            Mockito.times(1)
        ).updateJobWithRuntimeEnvironment(JOB_1_ID, clusterId, commandId, Lists.newArrayList(applicationId), MEMORY);

        Mockito.verify(
            this.jobStateService,
            Mockito.times(1)
        ).schedule(JOB_1_ID, jobRequest, cluster, command, applications, MEMORY);

        Mockito.verify(this.clusterLoadBalancer1, Mockito.never()).selectCluster(Mockito.any(), Mockito.any());
        Mockito.verify(this.clusterLoadBalancer2, Mockito.never()).selectCluster(Mockito.any(), Mockito.any());
        Mockito.verify(this.clusterLoadBalancer3, Mockito.never()).selectCluster(Mockito.any(), Mockito.any());
        Mockito.verify(this.clusterLoadBalancer4, Mockito.never()).selectCluster(Mockito.any(), Mockito.any());

        Mockito
            .verify(this.coordinationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(0))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationsTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canCoordinateJobForMultipleClusters() throws GenieException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        final Cluster cluster2 = Mockito.mock(Cluster.class);
        Mockito.when(cluster1.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster1, commandId);
        clustersCommandsMap.put(cluster2, UUID.randomUUID().toString());

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(null);
        Mockito
            .when(this.clusterLoadBalancer2.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenThrow(new RuntimeException());
        Mockito
            .when(this.clusterLoadBalancer3.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster1);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito.when(this.jobStateService.getUsedMemory()).thenReturn(0);

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

        Mockito.verify(this.jobStateService, Mockito.times(1)).schedule(JOB_1_ID, jobRequest, cluster1,
            command, applications, MEMORY);

        Mockito.verify(this.clusterLoadBalancer1, Mockito.times(1)).selectCluster(Mockito.any(), Mockito.any());
        Mockito.verify(this.clusterLoadBalancer2, Mockito.times(1)).selectCluster(Mockito.any(), Mockito.any());
        Mockito.verify(this.clusterLoadBalancer3, Mockito.times(1)).selectCluster(Mockito.any(), Mockito.any());
        Mockito.verify(this.clusterLoadBalancer4, Mockito.never()).selectCluster(Mockito.any(), Mockito.any());

        Mockito
            .verify(this.coordinationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(3))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationsTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
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
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final com.netflix.genie.common.dto.v4.Application v4Application
            = new com.netflix.genie.common.dto.v4.Application(
            applicationId,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ApplicationMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                ApplicationStatus.ACTIVE
            ).build()
        );

        Mockito.when(this.applicationService.getApplication(applicationId)).thenReturn(v4Application);
        final Application application = DtoAdapters.toV3Application(v4Application);

        Mockito.when(this.jobStateService.getUsedMemory()).thenReturn(0);

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito.verify(this.jobPersistenceService, Mockito.times(1))
            .createJob(
                Mockito.any(JobRequest.class),
                Mockito.any(JobMetadata.class),
                Mockito.any(Job.class),
                Mockito.any(JobExecution.class)
            );

        Mockito
            .verify(this.jobPersistenceService, Mockito.times(1))
            .updateJobWithRuntimeEnvironment(JOB_1_ID, clusterId, commandId, Lists.newArrayList(applicationId), MEMORY);

        Mockito
            .verify(this.jobStateService, Mockito.times(1))
            .schedule(JOB_1_ID, jobRequest, cluster, command, Lists.newArrayList(application), MEMORY);

        Mockito
            .verify(this.coordinationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(0))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationsTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
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
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito.verify(this.jobStateService, Mockito.never()).getUsedMemory();

            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GeniePreconditionException("test"))
                );
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationsTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
        }
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
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito
            .when(this.jobStateService.getUsedMemory())
            .thenReturn(this.jobsProperties.getMemory().getMaxSystemMemory());

        Mockito
            .when(this.jobStateService.jobExists(Mockito.any()))
            .thenReturn(true);

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito.
                verify(this.jobStateService, Mockito.times(1))
                .getUsedMemory();

            Mockito
                .verify(this.jobPersistenceService, Mockito.times(1))
                .updateJobStatus(Mockito.eq(JOB_1_ID), Mockito.eq(JobStatus.FAILED), Mockito.anyString());

            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GenieServerUnavailableException("test"))
                );
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationsTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
        }
    }

    /**
     * Test the coordinate job method allows a job through if the job user limit is exceeded but the limit itself is
     * disabled.
     *
     * @throws GenieException If there is any problem
     */
    @Test
    public void canCoordinateJobUserJobLimitIsDisabled() throws GenieException {
        final int userActiveJobsLimit = 5;
        this.jobsProperties.getUsers().getActiveLimit().setEnabled(false);
        this.jobsProperties.getUsers().getActiveLimit().setCount(userActiveJobsLimit);

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito
            .when(this.jobSearchService.getActiveJobCountForUser(Mockito.any(String.class)))
            .thenReturn(Long.valueOf(userActiveJobsLimit));

        this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);

        Mockito
            .verify(this.coordinationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(0))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationsTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
    }

    /**
     * Test the coordinate job method reject to accept a job if the user has reached the limit of allowed active jobs.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieUserLimitExceededException.class)
    public void cantCoordinateJobUserJobLimitIsExceeded() throws GenieException {
        final int userActiveJobsLimit = 5;
        this.jobsProperties.getUsers().getActiveLimit().setEnabled(true);
        this.jobsProperties.getUsers().getActiveLimit().setCount(userActiveJobsLimit);

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);

        Mockito
            .when(this.jobSearchService.getActiveJobCountForUser(Mockito.any(String.class)))
            .thenReturn(Long.valueOf(userActiveJobsLimit));

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(
                        new GenieUserLimitExceededException("test", "test", "test")
                    )
                );
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationsTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
        }
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieServerException.class)
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
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito.when(this.commandService.getCommand(commandId)).thenReturn(command);

        final String applicationId = UUID.randomUUID().toString();
        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));
        final List<Application> applications = Lists.newArrayList(application);

        Mockito.when(this.commandService.getApplicationsForCommand(commandId)).thenReturn(applications);
        Mockito.doThrow(new RuntimeException()).when(jobStateService).schedule(JOB_1_ID, jobRequest, cluster,
            command, applications, 1);
        Mockito
            .when(this.jobStateService.getUsedMemory())
            .thenReturn(0);
        Mockito
            .when(this.jobStateService.jobExists(Mockito.any()))
            .thenReturn(true);

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.jobStateService, Mockito.times(1))
                .getUsedMemory();
            Mockito
                .verify(this.jobPersistenceService, Mockito.times(1))
                .updateJobStatus(Mockito.eq(JOB_1_ID), Mockito.eq(JobStatus.FAILED), Mockito.any());

            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new RuntimeException("test"))
                );
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationsTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_APPLICATIONS_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
        }
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieException If there is any problem
     */
    @Test(expected = GenieNotFoundException.class)
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
        Mockito.when(cluster.getId()).thenReturn(Optional.of(clusterId));
        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(Optional.of(commandId));
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        final Map<Cluster, String> clustersCommandsMap = Maps.newHashMap();
        clustersCommandsMap.put(cluster, commandId);

        Mockito
            .when(this.clusterService.findClustersAndCommandsForJob(jobRequest))
            .thenReturn(clustersCommandsMap);

        Mockito
            .when(this.clusterLoadBalancer1.selectCluster(clustersCommandsMap.keySet(), jobRequest))
            .thenReturn(cluster);

        Mockito
            .when(this.commandService.getCommand(commandId))
            .thenThrow(new GenieNotFoundException("No command with id " + commandId));

        Mockito.verifyNoMoreInteractions(
            this.selectApplicationsTimer,
            this.setJobEnvironmentTimer
        );

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GenieNotFoundException("test"))
                );
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SELECT_CLUSTER_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.SELECT_COMMAND_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GenieNotFoundException("test"))
                );
        }
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
        Mockito.verifyNoMoreInteractions(
            this.coordinationTimer,
            this.selectApplicationsTimer,
            this.setJobEnvironmentTimer,
            this.selectClusterTimer,
            this.selectCommandTimer,
            this.loadBalancerCounter
        );
        this.jobCoordinatorService.coordinateJob(request, Mockito.mock(JobMetadata.class));
    }

    /**
     * Make sure if the job with id already exists.
     *
     * @throws GenieException On error
     */
    @Test(expected = GenieConflictException.class)
    public void cantCoordinateIfJobAlreadyExists() throws GenieException {
        final JobRequest request = getJobRequest(false, Sets.newHashSet(), null, null);
        final JobMetadata metadata = Mockito.mock(JobMetadata.class);
        Mockito.doThrow(GenieConflictException.class).when(jobPersistenceService)
            .createJob(Mockito.eq(request), Mockito.eq(metadata),
                Mockito.any(Job.class),
                Mockito.any(JobExecution.class));

        Mockito.verifyNoMoreInteractions(
            this.selectApplicationsTimer,
            this.setJobEnvironmentTimer,
            this.selectClusterTimer,
            this.selectCommandTimer,
            this.loadBalancerCounter
        );

        try {
            this.jobCoordinatorService.coordinateJob(request, metadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(
                    JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME,
                    MetricsUtils.newFailureTagsSetForException(new GenieConflictException("test"))
                );
        }
    }

    /**
     * Test killing a job without throwing an exception.
     *
     * @throws GenieException On any error
     */
    @Test
    public void canKillJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.doNothing().when(this.jobKillService).killJob(id, KILL_REASON);
        this.jobCoordinatorService.killJob(id, KILL_REASON);
    }

    /**
     * Test killing a job without throwing an exception.
     *
     * @throws GenieException On any error
     */
    @Test(expected = GenieException.class)
    public void cantKillJob() throws GenieException {
        final String id = UUID.randomUUID().toString();
        Mockito.doThrow(new GenieException(123, "fake")).when(this.jobKillService).killJob(id, KILL_REASON);
        this.jobCoordinatorService.killJob(id, KILL_REASON);
    }

    private JobRequest getJobRequest(
        final boolean disableLogArchival,
        final Set<String> commandCriteria,
        @Nullable final Integer memory,
        @Nullable final List<String> applications
    ) {
        final String email = "name@domain.com";
        final String setupFile = "setupFilePath";
        final String group = "group";
        final String description = "job description";
        final Set<String> tags = Sets.newHashSet("foo", "bar");

        return new JobRequest.Builder(
            JOB_1_NAME,
            JOB_1_USER,
            JOB_1_VERSION,
            Lists.newArrayList(),
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
