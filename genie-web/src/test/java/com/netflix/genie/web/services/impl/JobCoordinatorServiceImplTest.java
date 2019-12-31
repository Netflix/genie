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
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieConflictException;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException;
import com.netflix.genie.common.internal.dtos.v4.Application;
import com.netflix.genie.common.internal.dtos.v4.Cluster;
import com.netflix.genie.common.internal.dtos.v4.Command;
import com.netflix.genie.common.internal.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dtos.v4.JobEnvironment;
import com.netflix.genie.common.internal.dtos.v4.JobSpecification;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.checked.GenieJobResolutionException;
import com.netflix.genie.web.data.services.ApplicationPersistenceService;
import com.netflix.genie.web.data.services.ClusterPersistenceService;
import com.netflix.genie.web.data.services.CommandPersistenceService;
import com.netflix.genie.web.data.services.JobPersistenceService;
import com.netflix.genie.web.data.services.JobSearchService;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobResolverService;
import com.netflix.genie.web.services.JobStateService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.util.List;
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
public class JobCoordinatorServiceImplTest {

    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String JOB_1_USER = "einstein";
    private static final String JOB_1_VERSION = "1.0";
    private static final URI BASE_ARCHIVE_LOCATION = URI.create("file:///baselocation");
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
    private ApplicationPersistenceService applicationPersistenceService;
    private ClusterPersistenceService clusterPersistenceService;
    private CommandPersistenceService commandPersistenceService;
    private JobResolverService specificationService;
    private JobsProperties jobsProperties;
    private MeterRegistry registry;
    private Timer coordinationTimer;
    private Timer setJobEnvironmentTimer;
    private com.netflix.genie.common.internal.dtos.v4.JobMetadata v4JobMetadata;

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
        this.jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        this.jobsProperties.getLocations().setArchives(BASE_ARCHIVE_LOCATION);
        this.jobsProperties.getMemory().setDefaultJobMemory(MEMORY);
        this.jobsProperties.getActiveLimit().setEnabled(ACTIVE_JOBS_LIMIT_ENABLED);
        this.applicationPersistenceService = Mockito.mock(ApplicationPersistenceService.class);
        this.clusterPersistenceService = Mockito.mock(ClusterPersistenceService.class);
        this.commandPersistenceService = Mockito.mock(CommandPersistenceService.class);
        this.specificationService = Mockito.mock(JobResolverService.class);
        this.v4JobMetadata = Mockito.mock(com.netflix.genie.common.internal.dtos.v4.JobMetadata.class);

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
        this.setJobEnvironmentTimer = Mockito.mock(Timer.class);
        Mockito
            .when(
                this.registry.timer(
                    Mockito.eq(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME),
                    Mockito.anySet()
                )
            )
            .thenReturn(this.setJobEnvironmentTimer);

        this.jobCoordinatorService = new JobCoordinatorServiceImpl(
            this.jobPersistenceService,
            this.jobKillService,
            this.jobStateService,
            this.jobsProperties,
            this.applicationPersistenceService,
            this.jobSearchService,
            this.clusterPersistenceService,
            this.commandPersistenceService,
            this.specificationService,
            this.registry,
            HOST_NAME
        );
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCoordinateJobIfJobSpecificationResolutionFails() throws GenieException, GenieCheckedException {

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenThrow(new GenieJobResolutionException());

        Mockito.verifyNoMoreInteractions(this.setJobEnvironmentTimer);

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
        }
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test
    public void canCoordinateJob() throws GenieException, GenieCheckedException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(true, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(clusterId);
        Mockito.when(this.clusterPersistenceService.getCluster(clusterId)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(commandId);
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        Mockito.when(this.commandPersistenceService.getCommand(commandId)).thenReturn(command);

        final String application0Id = UUID.randomUUID().toString();
        final Application application0 = Mockito.mock(Application.class);
        Mockito.when(application0.getId()).thenReturn(application0Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application0Id)).thenReturn(application0);

        final String application1Id = UUID.randomUUID().toString();
        final Application application1 = Mockito.mock(Application.class);
        Mockito.when(application1.getId()).thenReturn(application1Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application1Id)).thenReturn(application1);

        final String archiveLocation = UUID.randomUUID().toString();

        final JobSpecification jobSpecification = new JobSpecification(
            null,
            null,
            new JobSpecification.ExecutionResource(
                jobRequest.getId().orElseThrow(IllegalArgumentException::new),
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(null, null, null)
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    new ExecutionEnvironment(null, null, null)
                )
            ),
            null,
            false,
            new File("/tmp/genie/jobs/" + JOB_1_ID),
            archiveLocation,
            null
        );
        final JobEnvironment jobEnvironment = Mockito.mock(JobEnvironment.class);
        final ResolvedJob resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, v4JobMetadata);

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenReturn(resolvedJob);

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
        ).updateJobWithRuntimeEnvironment(
            JOB_1_ID,
            clusterId,
            commandId,
            Lists.newArrayList(application0Id, application1Id),
            MEMORY
        );

        Mockito.verify(
            this.jobStateService,
            Mockito.times(1)
        ).schedule(
            JOB_1_ID,
            jobRequest,
            cluster,
            command,
            Lists.newArrayList(application0, application1),
            MEMORY
        );

        Mockito
            .verify(this.coordinationTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.OVERALL_COORDINATION_TIMER_NAME, SUCCESS_TIMER_TAGS);
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
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test(expected = GeniePreconditionException.class)
    public void cantCoordinateJobIfTooMuchMemoryRequested() throws GenieException, GenieCheckedException {
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
        Mockito.when(cluster.getId()).thenReturn(clusterId);
        Mockito.when(this.clusterPersistenceService.getCluster(clusterId)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(commandId);
        Mockito.when(command.getMemory()).thenReturn(Optional.empty());
        Mockito.when(this.commandPersistenceService.getCommand(commandId)).thenReturn(command);

        final String application0Id = UUID.randomUUID().toString();
        final Application application0 = Mockito.mock(Application.class);
        Mockito.when(application0.getId()).thenReturn(application0Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application0Id)).thenReturn(application0);

        final String application1Id = UUID.randomUUID().toString();
        final Application application1 = Mockito.mock(Application.class);
        Mockito.when(application1.getId()).thenReturn(application1Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application1Id)).thenReturn(application1);

        final String archiveLocation = UUID.randomUUID().toString();

        final JobSpecification jobSpecification = new JobSpecification(
            null,
            null,
            new JobSpecification.ExecutionResource(
                jobRequest.getId().orElseThrow(IllegalArgumentException::new),
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(null, null, null)
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    new ExecutionEnvironment(null, null, null)
                )
            ),
            null,
            false,
            new File("/tmp/genie/jobs/" + JOB_1_ID),
            archiveLocation,
            null
        );
        final JobEnvironment jobEnvironment = Mockito.mock(JobEnvironment.class);
        final ResolvedJob resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, v4JobMetadata);

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenReturn(resolvedJob);

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
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test(expected = GenieServerUnavailableException.class)
    public void cantCoordinateJobIfNotEnoughMemoryAvailable() throws GenieException, GenieCheckedException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(clusterId);
        Mockito.when(this.clusterPersistenceService.getCluster(clusterId)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(commandId);
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        Mockito.when(this.commandPersistenceService.getCommand(commandId)).thenReturn(command);

        final String application0Id = UUID.randomUUID().toString();
        final Application application0 = Mockito.mock(Application.class);
        Mockito.when(application0.getId()).thenReturn(application0Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application0Id)).thenReturn(application0);

        final String application1Id = UUID.randomUUID().toString();
        final Application application1 = Mockito.mock(Application.class);
        Mockito.when(application1.getId()).thenReturn(application1Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application1Id)).thenReturn(application1);

        final String archiveLocation = UUID.randomUUID().toString();

        final JobSpecification jobSpecification = new JobSpecification(
            null,
            null,
            new JobSpecification.ExecutionResource(
                jobRequest.getId().orElseThrow(IllegalArgumentException::new),
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(null, null, null)
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    new ExecutionEnvironment(null, null, null)
                )
            ),
            null,
            false,
            new File("/tmp/genie/jobs/" + JOB_1_ID),
            archiveLocation,
            null
        );
        final JobEnvironment jobEnvironment = Mockito.mock(JobEnvironment.class);
        final ResolvedJob resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, v4JobMetadata);

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenReturn(resolvedJob);

        Mockito
            .when(this.jobStateService.getUsedMemory())
            .thenReturn(this.jobsProperties.getMemory().getMaxSystemMemory());

        Mockito
            .when(this.jobStateService.jobExists(Mockito.anyString()))
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
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test
    public void canCoordinateIfJobUserJobLimitIsDisabled() throws GenieException, GenieCheckedException {
        final int userActiveJobsLimit = 5;
        this.jobsProperties.getActiveLimit().setEnabled(false);
        this.jobsProperties.getActiveLimit().setCount(userActiveJobsLimit);

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(clusterId);
        Mockito.when(this.clusterPersistenceService.getCluster(clusterId)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(commandId);
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        Mockito.when(this.commandPersistenceService.getCommand(commandId)).thenReturn(command);

        final String application0Id = UUID.randomUUID().toString();
        final Application application0 = Mockito.mock(Application.class);
        Mockito.when(application0.getId()).thenReturn(application0Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application0Id)).thenReturn(application0);

        final String application1Id = UUID.randomUUID().toString();
        final Application application1 = Mockito.mock(Application.class);
        Mockito.when(application1.getId()).thenReturn(application1Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application1Id)).thenReturn(application1);

        final String archiveLocation = UUID.randomUUID().toString();

        final JobSpecification jobSpecification = new JobSpecification(
            null,
            null,
            new JobSpecification.ExecutionResource(
                jobRequest.getId().orElseThrow(IllegalArgumentException::new),
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(null, null, null)
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    new ExecutionEnvironment(null, null, null)
                )
            ),
            null,
            false,
            new File("/tmp/genie/jobs/" + JOB_1_ID),
            archiveLocation,
            null
        );
        final JobEnvironment jobEnvironment = Mockito.mock(JobEnvironment.class);
        final ResolvedJob resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, v4JobMetadata);

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenReturn(resolvedJob);

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
            .verify(this.setJobEnvironmentTimer, Mockito.times(1))
            .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
        Mockito
            .verify(this.registry, Mockito.times(1))
            .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
    }

    /**
     * Test the coordinate job method reject to accept a job if the user has reached the limit of allowed active jobs.
     *
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test(expected = GenieUserLimitExceededException.class)
    public void cantCoordinateJobUserJobLimitIsExceeded() throws GenieException, GenieCheckedException {
        final int userActiveJobsLimit = 5;
        this.jobsProperties.getActiveLimit().setEnabled(true);
        this.jobsProperties.getActiveLimit().setCount(userActiveJobsLimit);

        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(clusterId);
        Mockito.when(this.clusterPersistenceService.getCluster(clusterId)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(commandId);
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        Mockito.when(this.commandPersistenceService.getCommand(commandId)).thenReturn(command);

        final String application0Id = UUID.randomUUID().toString();
        final Application application0 = Mockito.mock(Application.class);
        Mockito.when(application0.getId()).thenReturn(application0Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application0Id)).thenReturn(application0);

        final String application1Id = UUID.randomUUID().toString();
        final Application application1 = Mockito.mock(Application.class);
        Mockito.when(application1.getId()).thenReturn(application1Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application1Id)).thenReturn(application1);

        final String archiveLocation = UUID.randomUUID().toString();

        final JobSpecification jobSpecification = new JobSpecification(
            null,
            null,
            new JobSpecification.ExecutionResource(
                jobRequest.getId().orElseThrow(IllegalArgumentException::new),
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(null, null, null)
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    new ExecutionEnvironment(null, null, null)
                )
            ),
            null,
            false,
            new File("/tmp/genie/jobs/" + JOB_1_ID),
            archiveLocation,
            null
        );
        final JobEnvironment jobEnvironment = Mockito.mock(JobEnvironment.class);
        final ResolvedJob resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, v4JobMetadata);

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenReturn(resolvedJob);

        Mockito
            .when(this.jobSearchService.getActiveJobCountForUser(Mockito.any(String.class)))
            .thenReturn(Long.valueOf(userActiveJobsLimit));

        final Counter limitExceededCounter = Mockito.mock(Counter.class);

        Mockito
            .when(this.registry.counter(
                Mockito.eq(JobCoordinatorServiceImpl.USER_JOB_LIMIT_EXCEEDED_COUNTER_NAME),
                Mockito.eq(MetricsConstants.TagKeys.USER),
                Mockito.eq(jobRequest.getUser()),
                Mockito.eq(MetricsConstants.TagKeys.JOBS_USER_LIMIT),
                Mockito.eq(String.valueOf(userActiveJobsLimit))
            ))
            .thenReturn(limitExceededCounter);

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
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
            Mockito
                .verify(limitExceededCounter, Mockito.times(1))
                .increment();
        }
    }

    /**
     * Test the coordinate job method.
     *
     * @throws GenieCheckedException If there is any problem
     * @throws GenieException        If there is any problem
     */
    @Test(expected = GenieServerException.class)
    public void cantCoordinateJobIfTaskDoesntLaunch() throws GenieException, GenieCheckedException {
        final Set<String> commandCriteria = Sets.newHashSet(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString()
        );

        final JobRequest jobRequest = this.getJobRequest(false, commandCriteria, null, null);
        final JobMetadata jobMetadata = this.getJobMetadata();

        final String clusterId = UUID.randomUUID().toString();
        final Cluster cluster = Mockito.mock(Cluster.class);
        Mockito.when(cluster.getId()).thenReturn(clusterId);
        Mockito.when(this.clusterPersistenceService.getCluster(clusterId)).thenReturn(cluster);

        final String commandId = UUID.randomUUID().toString();
        final Command command = Mockito.mock(Command.class);
        Mockito.when(command.getId()).thenReturn(commandId);
        Mockito.when(command.getMemory()).thenReturn(Optional.of(1));
        Mockito.when(this.commandPersistenceService.getCommand(commandId)).thenReturn(command);

        final String application0Id = UUID.randomUUID().toString();
        final Application application0 = Mockito.mock(Application.class);
        Mockito.when(application0.getId()).thenReturn(application0Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application0Id)).thenReturn(application0);

        final String application1Id = UUID.randomUUID().toString();
        final Application application1 = Mockito.mock(Application.class);
        Mockito.when(application1.getId()).thenReturn(application1Id);
        Mockito.when(this.applicationPersistenceService.getApplication(application1Id)).thenReturn(application1);

        final String archiveLocation = UUID.randomUUID().toString();

        final JobSpecification jobSpecification = new JobSpecification(
            null,
            null,
            new JobSpecification.ExecutionResource(
                jobRequest.getId().orElseThrow(IllegalArgumentException::new),
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                clusterId,
                new ExecutionEnvironment(null, null, null)
            ),
            new JobSpecification.ExecutionResource(
                commandId,
                new ExecutionEnvironment(null, null, null)
            ),
            Lists.newArrayList(
                new JobSpecification.ExecutionResource(
                    application0Id,
                    new ExecutionEnvironment(null, null, null)
                ),
                new JobSpecification.ExecutionResource(
                    application1Id,
                    new ExecutionEnvironment(null, null, null)
                )
            ),
            null,
            false,
            new File("/tmp/genie/jobs/" + JOB_1_ID),
            archiveLocation,
            null
        );
        final JobEnvironment jobEnvironment = Mockito.mock(JobEnvironment.class);
        final ResolvedJob resolvedJob = new ResolvedJob(jobSpecification, jobEnvironment, v4JobMetadata);

        Mockito
            .when(
                this.specificationService.resolveJob(
                    Mockito.anyString(),
                    Mockito.any(com.netflix.genie.common.internal.dtos.v4.JobRequest.class),
                    Mockito.eq(true)
                )
            )
            .thenReturn(resolvedJob);

        Mockito
            .doThrow(new RuntimeException())
            .when(jobStateService)
            .schedule(
                JOB_1_ID,
                jobRequest,
                cluster,
                command,
                Lists.newArrayList(application0, application1),
                1
            );
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
                .verify(this.setJobEnvironmentTimer, Mockito.times(1))
                .record(Mockito.anyLong(), Mockito.eq(TimeUnit.NANOSECONDS));
            Mockito
                .verify(this.registry, Mockito.times(1))
                .timer(JobCoordinatorServiceImpl.SET_JOB_ENVIRONMENT_TIMER_NAME, SUCCESS_TIMER_TAGS);
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
            this.setJobEnvironmentTimer
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
        Mockito
            .doThrow(GenieConflictException.class)
            .when(jobPersistenceService)
            .createJob(
                Mockito.eq(request),
                Mockito.eq(metadata),
                Mockito.any(Job.class),
                Mockito.any(JobExecution.class)
            );

        Mockito.verifyNoMoreInteractions(this.setJobEnvironmentTimer);

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
