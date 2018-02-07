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
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.test.categories.UnitTest;
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
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
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
    private static final Map<String, String> SUCCESS_TIMER_TAGS = MetricsUtils.newSuccessTagsMap();

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
    private ArgumentCaptor<Map<String, String>> tagsCaptor;
    private Id coordinationTimerId;
    private Timer coordinationTimer;
    private Id selectClusterTimerId;
    private Timer selectClusterTimer;
    private Counter noClusterSelectedCounter;
    private Counter noMatchingClusterCounter;
    private Id loadBalancerCounterId;
    private Counter loadBalancerCounter;
    private Id selectCommandTimerId;
    private Timer selectCommandTimer;
    private Id selectApplicationTimerId;
    private Timer selectApplicationTimer;
    private Id setJobEnvironmentTimerId;
    private Timer setJobEnvironmentTimer;
    private Timer clusterCommandQueryTimer;

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

        final Registry registry = Mockito.mock(Registry.class);
        this.coordinationTimerId = Mockito.mock(Id.class);
        this.coordinationTimer = Mockito.mock(Timer.class);
        Mockito
            .when(registry.createId("genie.jobs.coordination.timer"))
            .thenReturn(this.coordinationTimerId);
        Mockito
            .when(this.coordinationTimerId.withTags(Mockito.anyMap()))
            .thenReturn(this.coordinationTimerId);
        Mockito
            .when(registry.timer(Mockito.eq(this.coordinationTimerId)))
            .thenReturn(this.coordinationTimer);
        this.selectClusterTimerId = Mockito.mock(Id.class);
        this.selectClusterTimer = Mockito.mock(Timer.class);
        Mockito
            .when(registry.createId("genie.jobs.submit.localRunner.selectCluster.timer"))
            .thenReturn(this.selectClusterTimerId);
        Mockito
            .when(this.selectClusterTimerId.withTags(Mockito.anyMap()))
            .thenReturn(this.selectClusterTimerId);
        Mockito
            .when(registry.timer(Mockito.eq(this.selectClusterTimerId)))
            .thenReturn(this.selectClusterTimer);
        this.selectCommandTimerId = Mockito.mock(Id.class);
        this.selectCommandTimer = Mockito.mock(Timer.class);
        Mockito
            .when(registry.createId("genie.jobs.submit.localRunner.selectCommand.timer"))
            .thenReturn(this.selectCommandTimerId);
        Mockito
            .when(this.selectCommandTimerId.withTags(Mockito.anyMap()))
            .thenReturn(this.selectCommandTimerId);
        Mockito
            .when(registry.timer(Mockito.eq(this.selectCommandTimerId)))
            .thenReturn(this.selectCommandTimer);
        this.selectApplicationTimerId = Mockito.mock(Id.class);
        this.selectApplicationTimer = Mockito.mock(Timer.class);
        Mockito
            .when(registry.createId("genie.jobs.submit.localRunner.selectApplications.timer"))
            .thenReturn(this.selectApplicationTimerId);
        Mockito
            .when(this.selectApplicationTimerId.withTags(Mockito.anyMap()))
            .thenReturn(this.selectApplicationTimerId);
        Mockito
            .when(registry.timer(Mockito.eq(selectApplicationTimerId)))
            .thenReturn(selectApplicationTimer);
        this.setJobEnvironmentTimerId = Mockito.mock(Id.class);
        this.setJobEnvironmentTimer = Mockito.mock(Timer.class);
        Mockito
            .when(registry.createId("genie.jobs.submit.localRunner.setJobEnvironment.timer"))
            .thenReturn(this.setJobEnvironmentTimerId);
        Mockito
            .when(this.setJobEnvironmentTimerId.withTags(Mockito.anyMap()))
            .thenReturn(this.setJobEnvironmentTimerId);
        Mockito
            .when(registry.timer(Mockito.eq(this.setJobEnvironmentTimerId)))
            .thenReturn(this.setJobEnvironmentTimer);
        this.noClusterSelectedCounter = Mockito.mock(Counter.class);
        Mockito
            .when(registry.counter("genie.jobs.submit.selectCluster.noneSelected.counter"))
            .thenReturn(this.noClusterSelectedCounter);
        this.noMatchingClusterCounter = Mockito.mock(Counter.class);
        Mockito
            .when(registry.counter("genie.jobs.submit.selectCluster.noneFound.counter"))
            .thenReturn(this.noMatchingClusterCounter);
        this.loadBalancerCounterId = Mockito.mock(Id.class);
        this.loadBalancerCounter = Mockito.mock(Counter.class);
        Mockito
            .when(registry.createId("genie.jobs.submit.selectCluster.loadBalancer.counter"))
            .thenReturn(this.loadBalancerCounterId);
        Mockito
            .when(this.loadBalancerCounterId.withTags(Mockito.anyMap()))
            .thenReturn(this.loadBalancerCounterId);
        Mockito
            .when(registry.counter(Mockito.eq(this.loadBalancerCounterId)))
            .thenReturn(this.loadBalancerCounter);

        final Id clusterCommandQueryTimerId = Mockito.mock(Id.class);
        this.clusterCommandQueryTimer = Mockito.mock(Timer.class);
        Mockito
            .when(registry.createId("genie.jobs.coordination.clusterCommandQuery.timer"))
            .thenReturn(clusterCommandQueryTimerId);
        Mockito
            .when(clusterCommandQueryTimerId.withTags(Mockito.anyMap()))
            .thenReturn(clusterCommandQueryTimerId);
        Mockito
            .when(registry.timer(clusterCommandQueryTimerId))
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
            registry,
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
            this.selectApplicationTimer,
            this.selectApplicationTimerId,
            this.selectClusterTimer,
            this.selectClusterTimerId,
            this.setJobEnvironmentTimer,
            this.setJobEnvironmentTimerId
        );

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GeniePreconditionException("test")));
            Mockito
                .verify(this.noMatchingClusterCounter, Mockito.times(1))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GeniePreconditionException("test")));
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
            this.selectApplicationTimer,
            this.selectApplicationTimerId,
            this.selectClusterTimer,
            this.selectClusterTimerId,
            this.setJobEnvironmentTimer,
            this.setJobEnvironmentTimerId
        );

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GeniePreconditionException("test")));
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(1)).increment();
            Mockito
                .verify(this.loadBalancerCounterId, Mockito.times(4))
                .withTags(this.tagsCaptor.capture());
            final String className = this.tagsCaptor.getValue().get(MetricsConstants.TagKeys.CLASS_NAME);
            Assert.assertNotNull(className);
            Assert.assertTrue(className.startsWith("com.netflix.genie.web.services.ClusterLoadBalancer"));
            final String status = this.tagsCaptor.getValue().get(MetricsConstants.TagKeys.STATUS);
            Assert.assertEquals("no preference", status);
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(4))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GeniePreconditionException("test")));
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
            .verify(this.coordinationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(0))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectClusterTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectCommandTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectApplicationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
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
            .verify(this.coordinationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(3))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectClusterTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectCommandTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectApplicationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
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

        final Application application = Mockito.mock(Application.class);
        Mockito.when(application.getId()).thenReturn(Optional.of(applicationId));

        Mockito.when(this.applicationService.getApplication(applicationId)).thenReturn(application);

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

        Mockito.verify(jobStateService, Mockito.times(1)).schedule(JOB_1_ID, jobRequest, cluster,
            command, Lists.newArrayList(application), MEMORY);

        Mockito
            .verify(this.coordinationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.coordinationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(0))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectClusterTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectCommandTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectApplicationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
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
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GeniePreconditionException("test")));
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectCommandTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectApplicationTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
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
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GenieServerUnavailableException("test")));
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectCommandTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectApplicationTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
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
            .verify(this.coordinationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
        Mockito
            .verify(this.loadBalancerCounter, Mockito.times(0))
            .increment();
        Mockito
            .verify(this.selectClusterTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectClusterTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectCommandTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectCommandTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.selectApplicationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.selectApplicationTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
        Mockito
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
            .withTags(SUCCESS_TIMER_TAGS);
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
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(
                    new GenieUserLimitExceededException("test", "test", "test"))
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
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectCommandTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectApplicationTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
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
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new RuntimeException("test")));
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectCommandTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectApplicationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectApplicationTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.setJobEnvironmentTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
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
            this.selectApplicationTimer,
            this.selectApplicationTimerId,
            this.setJobEnvironmentTimer,
            this.setJobEnvironmentTimerId
        );

        try {
            this.jobCoordinatorService.coordinateJob(jobRequest, jobMetadata);
        } finally {
            Mockito
                .verify(this.coordinationTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GenieNotFoundException("test")));
            Mockito
                .verify(this.noClusterSelectedCounter, Mockito.times(0)).increment();
            Mockito
                .verify(this.loadBalancerCounter, Mockito.times(0))
                .increment();
            Mockito
                .verify(this.selectClusterTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectClusterTimerId, Mockito.times(1))
                .withTags(SUCCESS_TIMER_TAGS);
            Mockito
                .verify(this.selectCommandTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.selectCommandTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GenieNotFoundException("test")));
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
            this.selectApplicationTimer,
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
            this.selectApplicationTimer,
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
                .verify(this.coordinationTimerId, Mockito.times(1))
                .withTags(MetricsUtils.newFailureTagsMapForException(new GenieConflictException("test")));
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
