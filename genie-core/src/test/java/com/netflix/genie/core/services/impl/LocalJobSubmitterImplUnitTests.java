package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.workflow.WorkflowExecutor;
import com.netflix.genie.core.jobs.workflow.WorkflowTask;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.GenieFileTransferService;
import com.netflix.genie.core.services.JobPersistenceService;
import com.netflix.genie.core.services.JobSubmitterService;
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.context.ApplicationEventPublisher;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Unit Tests for the Local Job Submitter Impl class.
 *
 * @author amsharma
 */
@Category(UnitTest.class)
public class LocalJobSubmitterImplUnitTests {

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
    private CommandService commandService;
    private ClusterLoadBalancer clusterLoadBalancer;
    private ApplicationEventPublisher applicationEventPublisher;
    private GenieFileTransferService fileTransferService;
    private WorkflowExecutor wfExecutor;
    private JobSubmitterService jobSubmitterService;
    private WorkflowTask task1;
    private WorkflowTask task2;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.jobPersistenceService = Mockito.mock(JobPersistenceService.class);
        this.clusterService = Mockito.mock(ClusterService.class);
        this.commandService = Mockito.mock(CommandService.class);
        this.clusterLoadBalancer = Mockito.mock(ClusterLoadBalancer.class);
        this.applicationEventPublisher = Mockito.mock(ApplicationEventPublisher.class);
        this.fileTransferService = Mockito.mock(GenieFileTransferService.class);
        this.wfExecutor = Mockito.mock(WorkflowExecutor.class);
        this.task1 = Mockito.mock(WorkflowTask.class);
        this.task2 = Mockito.mock(WorkflowTask.class);

        final List<WorkflowTask> jobWorkflowTasks = new ArrayList<>();
        jobWorkflowTasks.add(task1);
        jobWorkflowTasks.add(task2);

        this.jobSubmitterService = new LocalJobSubmitterImpl(
            this.jobPersistenceService,
            this.clusterService,
            this.commandService,
            this.clusterLoadBalancer,
            this.fileTransferService,
            this.wfExecutor,
            this.applicationEventPublisher,
            jobWorkflowTasks,
            this.BASE_WORKING_DIR
        );
    }

    /**
     * Test the submitJob method where the request does not resolve to any cluster.
     *
     * @throws GenieException If there is any problem.
     */
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

        Mockito.when(this.clusterService.chooseClusterForJobRequest(Mockito.eq(jobRequest))).
            thenReturn(emptyList);

        Mockito.when(this.clusterLoadBalancer.selectCluster(Mockito.eq(emptyList))).
            thenThrow(GeniePreconditionException.class);

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
            ClusterStatus.UP,
            null
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
    @Test(expected = GenieServerException.class)
    public void testSubmitJob() throws GenieException {

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
            ClusterStatus.UP,
            null
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
            "foo"
        )
            .withId(COMMAND_ID)
            .build();

        final List<Command> commandList = new ArrayList<>();
        commandList.add(command);

        Mockito.when(this.clusterService.chooseClusterForJobRequest(Mockito.eq(jobRequest))).
            thenReturn(clusterList);
        Mockito.when(this.clusterLoadBalancer.selectCluster(Mockito.eq(clusterList))).
            thenReturn(cluster);
        Mockito.when(this.clusterService.getCommandsForCluster(Mockito.eq(CLUSTER_ID), Mockito.eq(enumStatuses)))
            .thenReturn(commandList);
        //Mockito.when(this.wfExecutor.executeWorkflow(Mockito.any(), Mockito.anyMap())).thenReturn(true);

        final ArgumentCaptor<String> jobId1 = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> jobId2 = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> clusterId = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> commandId = ArgumentCaptor.forClass(String.class);

        this.jobSubmitterService.submitJob(jobRequest);

        Mockito.verify(this.jobPersistenceService).updateClusterForJob(jobId1.capture(), clusterId.capture());
        Mockito.verify(this.jobPersistenceService).updateCommandForJob(jobId2.capture(), commandId.capture());

        Assert.assertEquals(JOB_1_ID, jobId1.getValue());
        Assert.assertEquals(JOB_1_ID, jobId2.getValue());
        Assert.assertEquals(CLUSTER_ID, clusterId.getValue());
        Assert.assertEquals(COMMAND_ID, commandId.getValue());
    }
}
