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
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.test.categories.UnitTest;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Unit Tests for the Local Job Submitter Impl class.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class LocalJobRunnerUnitTests {

    private static final String BASE_WORKING_DIR = "file://workingdir";
    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String USER = "einstien";
    private static final String VERSION = "1.0";

    private static final String CLUSTER_ID = "clusterid";
    private static final String CLUSTER_NAME = "clustername";

    private static final String COMMAND_ID = "commandid";
    private static final String COMMAND_NAME = "commandname";

    private JobPersistenceService jobPersistenceService;
    private ClusterService clusterService;
    private ClusterLoadBalancer clusterLoadBalancer;
    private JobSubmitterService jobSubmitterService;
    private ApplicationService applicationService;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.applicationService = Mockito.mock(ApplicationService.class);
        this.clusterService = Mockito.mock(ClusterService.class);
        final CommandService commandService = Mockito.mock(CommandService.class);
        this.clusterLoadBalancer = Mockito.mock(ClusterLoadBalancer.class);
        final ApplicationEventPublisher applicationEventPublisher = Mockito.mock(ApplicationEventPublisher.class);
        final GenieFileTransferService fileTransferService = Mockito.mock(GenieFileTransferService.class);
        final WorkflowTask task1 = Mockito.mock(WorkflowTask.class);
        final WorkflowTask task2 = Mockito.mock(WorkflowTask.class);

        final List<WorkflowTask> jobWorkflowTasks = new ArrayList<>();
        jobWorkflowTasks.add(task1);
        jobWorkflowTasks.add(task2);

        final Resource baseWorkingDirResource = new DefaultResourceLoader().getResource(BASE_WORKING_DIR);

        this.jobSubmitterService = new LocalJobRunner(
            this.jobPersistenceService,
            this.applicationService,
            this.clusterService,
            commandService,
            this.clusterLoadBalancer,
            fileTransferService,
            applicationEventPublisher,
            jobWorkflowTasks,
            baseWorkingDirResource
        );
    }

    /**
     * Test the submitJob method where the request does not resolve to any cluster.
     *
     * @throws GenieException If there is any problem.
     */
    @SuppressWarnings("unchecked")
    @Test(expected = GeniePreconditionException.class)
    public void testSubmitJobNoClusterFound() throws GenieException {
        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            USER,
            VERSION,
            null,
            null,
            null
        ).
            withId(JOB_1_ID)
            .build();

        final List<Cluster> emptyList = new ArrayList<>();

        Mockito.when(this.clusterService.chooseClusterForJobRequest(Mockito.eq(jobRequest))).thenReturn(emptyList);

        Mockito.when(this.clusterLoadBalancer.selectCluster(emptyList)).thenThrow(GeniePreconditionException.class);

        this.jobSubmitterService.submitJob(jobRequest);
    }

    /**
     * Test the submitJob method where the request does not resolve to any commands.
     *
     * @throws GenieException If there is any problem.
     */
    @Test(expected = GeniePreconditionException.class)
    public void testSubmitJobNoCommandFound() throws GenieException {

        final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
        enumStatuses.add(CommandStatus.ACTIVE);

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            USER,
            VERSION,
            null,
            null,
            null
        ).
            withId(JOB_1_ID)
            .build();

        final Cluster cluster = new Cluster.Builder(
            CLUSTER_NAME,
            USER,
            VERSION,
            ClusterStatus.UP
        )
            .withId(CLUSTER_ID)
            .build();


        final List<Cluster> cList = new ArrayList<>();
        cList.add(cluster);

        final List<Command> emptyList = new ArrayList<>();

        Mockito.when(this.clusterService.chooseClusterForJobRequest(Mockito.eq(jobRequest))).
            thenReturn(cList);
        Mockito.when(this.clusterLoadBalancer.selectCluster(Mockito.eq(cList))).
            thenReturn(cluster);
        Mockito.when(this.clusterService.getCommandsForCluster(Mockito.eq(CLUSTER_ID), Mockito.eq(enumStatuses)))
            .thenReturn(emptyList);

        this.jobSubmitterService.submitJob(jobRequest);
    }

    /**
     * Test the submitJob method to check cluster/command info updated for jobs and exception if
     * workflow executor returns false.
     *
     * @throws GenieException If there is any problem.
     */
    @SuppressWarnings("unchecked")
    @Test(expected = GenieServerException.class)
    public void testSubmitJob() throws GenieException {

        final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
        enumStatuses.add(CommandStatus.ACTIVE);

        final String app1 = UUID.randomUUID().toString();
        final String app2 = UUID.randomUUID().toString();
        final String app3 = UUID.randomUUID().toString();
        final List<String> applications = Lists.newArrayList(app3, app1, app2);

        final String placeholder = UUID.randomUUID().toString();
        Mockito
            .when(this.applicationService.getApplication(app3))
            .thenReturn(
                new Application.Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                    .withId(app3)
                    .build()
            );
        Mockito
            .when(this.applicationService.getApplication(app1))
            .thenReturn(
                new Application.Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                    .withId(app1)
                    .build()
            );
        Mockito
            .when(this.applicationService.getApplication(app2))
            .thenReturn(
                new Application.Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                    .withId(app2)
                    .build()
            );

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            USER,
            VERSION,
            null,
            null,
            null
        )
            .withId(JOB_1_ID)
            .withApplications(applications)
            .build();

        final Cluster cluster = new Cluster.Builder(
            CLUSTER_NAME,
            USER,
            VERSION,
            ClusterStatus.UP
        )
            .withId(CLUSTER_ID)
            .build();


        final List<Cluster> clusterList = new ArrayList<>();
        clusterList.add(cluster);

        final Command command = new Command.Builder(
            COMMAND_NAME,
            USER,
            VERSION,
            CommandStatus.ACTIVE,
            "foo",
            5000L
        )
            .withId(COMMAND_ID)
            .build();

        final List<Command> commandList = new ArrayList<>();
        commandList.add(command);

        Mockito.when(this.clusterService.chooseClusterForJobRequest(jobRequest)).thenReturn(clusterList);
        Mockito.when(this.clusterLoadBalancer.selectCluster(clusterList)).thenReturn(cluster);
        Mockito
            .when(this.clusterService.getCommandsForCluster(CLUSTER_ID, enumStatuses))
            .thenReturn(commandList);

        final ArgumentCaptor<String> jobId1 = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> clusterId = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> commandId = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<List<String>> applicationIds = ArgumentCaptor.forClass((Class) List.class);

        this.jobSubmitterService.submitJob(jobRequest);

        Mockito
            .verify(this.jobPersistenceService)
            .updateJobWithRuntimeEnvironment(
                jobId1.capture(),
                clusterId.capture(),
                commandId.capture(),
                applicationIds.capture()
            );

        Assert.assertThat(jobId1.getValue(), Matchers.is(JOB_1_ID));
        Assert.assertThat(clusterId.getValue(), Matchers.is(CLUSTER_ID));
        Assert.assertThat(commandId.getValue(), Matchers.is(COMMAND_ID));
        Assert.assertThat(applicationIds.getValue().get(0), Matchers.is(app3));
        Assert.assertThat(applicationIds.getValue().get(1), Matchers.is(app1));
        Assert.assertThat(applicationIds.getValue().get(2), Matchers.is(app2));
    }
}
