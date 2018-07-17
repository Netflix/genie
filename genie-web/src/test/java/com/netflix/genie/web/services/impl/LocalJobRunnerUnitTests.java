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
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.ApplicationMetadata;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.ClusterMetadata;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.CommandMetadata;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.test.categories.UnitTest;
import com.netflix.genie.web.events.GenieEventBus;
import com.netflix.genie.web.jobs.workflow.WorkflowTask;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSubmitterService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
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

    private static final String JOB_1_ID = "job1";
    private static final String JOB_1_NAME = "relativity";
    private static final String USER = "einstein";
    private static final String VERSION = "1.0";

    private static final String CLUSTER_ID = "clusterId";
    private static final String CLUSTER_NAME = "clusterName";

    private static final String COMMAND_ID = "commandId";
    private static final String COMMAND_NAME = "commandName";

    /**
     * Temporary directory for these tests.
     */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private JobSubmitterService jobSubmitterService;
    private WorkflowTask task2;
    private File tmpFolder;

    /**
     * Setup for the tests.
     *
     * @throws IOException on error creating temporary dir
     */
    @Before
    public void setup() throws IOException {
        final GenieEventBus genieEventBus = Mockito.mock(GenieEventBus.class);
        final WorkflowTask task1 = Mockito.mock(WorkflowTask.class);
        this.task2 = Mockito.mock(WorkflowTask.class);

        final List<WorkflowTask> jobWorkflowTasks = new ArrayList<>();
        jobWorkflowTasks.add(task1);
        jobWorkflowTasks.add(this.task2);

        this.tmpFolder = this.folder.newFolder();
        final Resource baseWorkingDirResource = Mockito.mock(Resource.class);
        Mockito.when(baseWorkingDirResource.getFile()).thenReturn(this.tmpFolder);

        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        Mockito.when(registry.timer(Mockito.anyString())).thenReturn(Mockito.mock(Timer.class));

        this.jobSubmitterService = new LocalJobRunner(
            Mockito.mock(JobPersistenceService.class),
            genieEventBus,
            jobWorkflowTasks,
            baseWorkingDirResource,
            registry
        );
    }

    /**
     * Test the submitJob method to check cluster/command info updated for jobs and exception if
     * workflow executor returns false.
     *
     * @throws GenieException If there is any problem.
     * @throws IOException    when there is any IO problem
     */
    @SuppressWarnings("unchecked")
    @Test(expected = GenieServerException.class)
    public void testSubmitJob() throws GenieException, IOException {

        final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
        enumStatuses.add(CommandStatus.ACTIVE);

        final String placeholder = UUID.randomUUID().toString();
        final String app1 = UUID.randomUUID().toString();
        final String app2 = UUID.randomUUID().toString();
        final String app3 = UUID.randomUUID().toString();
        final List<Application> applications = Lists.newArrayList(
            new Application(
                app3,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ApplicationMetadata
                    .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                    .build()
            ),
            new Application(
                app1,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ApplicationMetadata
                    .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                    .build()
            ),
            new Application(
                app2,
                Instant.now(),
                Instant.now(),
                new ExecutionEnvironment(null, null, null),
                new ApplicationMetadata
                    .Builder(placeholder, placeholder, placeholder, ApplicationStatus.ACTIVE)
                    .build()
            )
        );

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_1_NAME,
            USER,
            VERSION,
            Lists.newArrayList(),
            Sets.newHashSet()
        )
            .withId(JOB_1_ID)
            .withApplications(Lists.newArrayList(app3, app1, app2))
            .build();

        final Cluster cluster = new Cluster(
            CLUSTER_ID,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new ClusterMetadata.Builder(CLUSTER_NAME, USER, VERSION, ClusterStatus.UP).build()
        );

        final Command command = new Command(
            COMMAND_ID,
            Instant.now(),
            Instant.now(),
            new ExecutionEnvironment(null, null, null),
            new CommandMetadata.Builder(COMMAND_NAME, USER, VERSION, CommandStatus.ACTIVE).build(),
            Lists.newArrayList("foo"),
            null,
            5000L
        );

        final int memory = 2438;

        Mockito.doThrow(new IOException("something bad")).when(this.task2).executeTask(Mockito.anyMap());

        try {
            this.jobSubmitterService.submitJob(jobRequest, cluster, command, applications, memory);
        } catch (Throwable t) {
            final File jobDirectory = new File(tmpFolder, JOB_1_ID);
            Assert.assertTrue(jobDirectory.exists());
            final File initFailureFile = new File(jobDirectory, JobConstants.GENIE_INIT_FAILURE_MESSAGE_FILE_NAME);
            Assert.assertTrue(initFailureFile.exists());
            Assert.assertTrue(initFailureFile.length() > 0);
            throw t;
        }
    }
}
