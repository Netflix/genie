/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.genie.client.configs.GenieNetworkConfiguration;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.dto.search.JobSearchResult;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Integration tests for Genie Job Client.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
@Ignore
public class JobClientIntegrationTests extends GenieClientsIntegrationTestsBase {

    private static final String JOB_NAME = "List Directories bash job";
    private static final String JOB_USER = "genie";
    private static final String JOB_VERSION = "1.0";
    private static final String JOB_DESCRIPTION = "Genie 3 Test Job";
    private static final String CLUSTER_NAME = "Cluster Name";
    private static final String COMMAND_NAME = "Command Name";

    private ClusterClient clusterClient;
    private CommandClient commandClient;
    //private ApplicationClient applicationClient;
    private JobClient jobClient;
    private ResourceLoader resourceLoader;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        this.resourceLoader = new DefaultResourceLoader();
        clusterClient = new ClusterClient(getBaseUrl(), null, null);
        commandClient = new CommandClient(getBaseUrl(), null, null);
        //applicationClient = new ApplicationClient(getBaseUrl());
        final GenieNetworkConfiguration genieNetworkConfiguration = new GenieNetworkConfiguration();
        genieNetworkConfiguration.setReadTimeout(20000);
        jobClient = new JobClient(getBaseUrl(), null, genieNetworkConfiguration);
    }

    /**
     * Method to test submitting a job.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void canSubmitJob() throws Exception {

        createClusterAndCommandForTest();

        final String jobId = UUID.randomUUID().toString();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final String configFile1
            = this.resourceLoader.getResource("/config1").getFile().getAbsolutePath();
        final Set<String> configs = Sets.newHashSet(configFile1);

        final String depFile1
            = this.resourceLoader.getResource("/dep1").getFile().getAbsolutePath();
        final Set<String> dependencies = Sets.newHashSet(depFile1);

        final String setUpFile = this.resourceLoader.getResource("/setupfile").getFile().getAbsolutePath();

        final List<String> commandArgs = Lists.newArrayList(
            "-c",
            "'echo HELLO WORLD!!!'"
        );

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .build();

        final String id = jobClient.submitJob(jobRequest);

        final JobStatus jobStatus = jobClient.waitForCompletion(jobId, 600000);

        Assert.assertEquals(JobStatus.SUCCEEDED, jobStatus);
        final Job job = jobClient.getJob(id);

        Assert.assertEquals(jobId, job.getId().orElseThrow(IllegalArgumentException::new));

        final JobRequest jobRequest1 = jobClient.getJobRequest(jobId);
        Assert.assertEquals(jobId, jobRequest1.getId().orElseThrow(IllegalArgumentException::new));
        Assert.assertTrue(jobRequest1.getDependencies().contains(depFile1));

        final JobExecution jobExecution = jobClient.getJobExecution(jobId);
        Assert.assertEquals(jobId, jobExecution.getId().orElseThrow(IllegalArgumentException::new));

        final JobMetadata jobMetadata = this.jobClient.getJobMetadata(jobId);
        Assert.assertEquals(jobId, jobMetadata.getId().orElseThrow(IllegalArgumentException::new));

        Assert.assertEquals(CLUSTER_NAME, jobClient.getJobCluster(jobId).getName());
        Assert.assertEquals(COMMAND_NAME, jobClient.getJobCommand(jobId).getName());
    }

    /**
     * Method to test submitting/killing a job.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void submitAndKillJob() throws Exception {

        createClusterAndCommandForTest();

        final String jobId = UUID.randomUUID().toString();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final String depFile1
            = this.resourceLoader.getResource("/dep1").getFile().getAbsolutePath();
        final Set<String> dependencies = Sets.newHashSet(depFile1);

        final String setUpFile = this.resourceLoader.getResource("/setupfile").getFile().getAbsolutePath();

        final List<String> commandArgs = Lists.newArrayList(
            "-c",
            "'echo HELLO WORLD!!!'"
        );

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .withSetupFile(setUpFile)
            .withDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .build();

        final ExecutorService executors = Executors.newFixedThreadPool(2);
        final Future<String> jobFuture;
        try {
            jobFuture = executors.submit(() -> jobClient.submitJob(jobRequest));
            executors.submit(() -> {
                boolean result = true;
                while (result) {
                    try {
                        jobClient.getJob(jobId);
                        jobClient.killJob(jobId);
                        Thread.sleep(1000);
                        result = false;
                    } catch (Exception ignored) {
                        result = true;
                    }
                }
            });
        } finally {
            executors.shutdown();
            executors.awaitTermination(Integer.MAX_VALUE, TimeUnit.HOURS);
        }
        final Job job = jobClient.getJob(jobId);
        Assert.assertEquals(jobId, jobFuture.get());
        Assert.assertEquals(JobStatus.KILLED, job.getStatus());
    }

    /**
     * Method to test submitting a job using attachments.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testJobSubmissionWithAttachmentsFiles() throws Exception {

        try (
            InputStream att1 = new FileInputStream(this.resourceLoader.getResource("/setupfile")
                .getFile().getAbsolutePath());
            InputStream att2 = new FileInputStream(this.resourceLoader.getResource("/data.txt")
                .getFile().getAbsolutePath())
        ) {

            createClusterAndCommandForTest();
            final String jobId = UUID.randomUUID().toString();

            final List<ClusterCriteria> clusterCriteriaList
                = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

            final Set<String> commandCriteria = Sets.newHashSet("bash");

            final JobRequest jobRequest = new JobRequest.Builder(
                JOB_NAME,
                JOB_USER,
                JOB_VERSION,
                clusterCriteriaList,
                commandCriteria
            )
                .withId(jobId)
                .withCommandArgs(Lists.newArrayList("-c", "'cat data.txt'"))
                .withDisableLogArchival(true)
                .build();

            final Map<String, InputStream> attachments = new HashMap<>();
            attachments.put("setupfile", att1);
            attachments.put("data.txt", att2);
            final String id = jobClient.submitJobWithAttachments(jobRequest, attachments);

            final JobStatus jobStatus = jobClient.waitForCompletion(jobId, 600000, 5000);

            Assert.assertEquals(JobStatus.SUCCEEDED, jobStatus);
            final Job job = jobClient.getJob(id);

            Assert.assertEquals(jobId, job.getId().orElseThrow(IllegalArgumentException::new));

            final JobRequest jobRequest1 = jobClient.getJobRequest(jobId);
            Assert.assertEquals(jobId, jobRequest1.getId().orElseThrow(IllegalArgumentException::new));
        }
    }

    /**
     * Method to test submitting a job using attachments.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testJobSubmissionWithAttachmentsByteArray() throws Exception {

        createClusterAndCommandForTest();
        final String jobId = UUID.randomUUID().toString();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withCommandArgs(Lists.newArrayList("-c", "'cat attachmentfile.txt'"))
            .withDisableLogArchival(true)
            .build();

        try (ByteArrayInputStream bis = new ByteArrayInputStream("ATTACHMENT DATA".getBytes(StandardCharsets.UTF_8))) {
            final Map<String, InputStream> attachments = new HashMap<>();

            attachments.put("attachmentfile.txt", bis);
            jobClient.submitJobWithAttachments(jobRequest, attachments);
        }

        final JobStatus jobStatus = jobClient.waitForCompletion(jobId, 600000, 5000);
        Assert.assertEquals(JobStatus.SUCCEEDED, jobStatus);
        final Job job = jobClient.getJob(jobId);

        Assert.assertEquals(jobId, job.getId().orElseThrow(IllegalArgumentException::new));

        final InputStream inputStream1
            = jobClient.getJobStdout(jobRequest.getId().orElseThrow(IllegalArgumentException::new));
        final BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream1, StandardCharsets.UTF_8));
        final StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader1.readLine()) != null) {
            sb.append(line);
        }

        reader1.close();
        inputStream1.close();

        Assert.assertEquals("ATTACHMENT DATA", sb.toString());
    }

    /**
     * Method to test killing a job.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testJobKill() throws Exception {
        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final JobRequest jobRequest1 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(Lists.newArrayList("-c", "'sleep 60'"))
            .withDisableLogArchival(true)
            .build();

        jobClient.submitJob(jobRequest1);
        Thread.sleep(2000);
        jobClient.killJob(jobRequest1.getId().orElseThrow(IllegalArgumentException::new));

        final JobStatus jobStatus
            = jobClient.waitForCompletion(jobRequest1.getId().orElseThrow(IllegalArgumentException::new), 600000, 5000);

        Assert.assertEquals(JobStatus.KILLED, jobStatus);
        Assert.assertEquals(
            JobStatusMessages.JOB_KILLED_BY_USER,
            jobClient.getJob(jobRequest1.getId().orElseThrow(IllegalArgumentException::new))
                .getStatusMsg().orElseThrow(IllegalArgumentException::new)
        );

    }

    /**
     * Method to test job getting killed by JobMonitor due to timeout.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testJobKillDueToTimeout() throws Exception {
        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final JobRequest jobRequest1 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(Lists.newArrayList("-c", "'sleep 60'"))
            .withDisableLogArchival(true)
            .withTimeout(1)
            .build();

        jobClient.submitJob(jobRequest1);
        Thread.sleep(2000);

        final JobStatus jobStatus
            = jobClient.waitForCompletion(jobRequest1.getId().orElseThrow(IllegalArgumentException::new), 600000, 5000);

        Assert.assertEquals(JobStatus.KILLED, jobStatus);
        Assert.assertEquals(
            "Job exceeded timeout.",
            jobClient.getJob(jobRequest1.getId().orElseThrow(IllegalArgumentException::new))
                .getStatusMsg().orElseThrow(IllegalArgumentException::new)
        );
    }

    /**
     * Method to test getJobs function.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testCanGetJobs() throws Exception {

        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final List<String> commandArgs = Lists.newArrayList(
            "-c",
            "'echo HELLO WORLD!!!'"
        );

        final JobRequest jobRequest1 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        this.jobClient.submitJob(jobRequest1);

        final JobRequest jobRequest2 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        this.jobClient.submitJob(jobRequest2);

        final List<JobSearchResult> jobs = jobClient.getJobs();
        Assert.assertTrue(jobs.size() >= 2);
    }

    /**
     * Method to test getJobs with params function.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testCanGetJobsUsingParams() throws Exception {

        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final List<String> commandArgs = Lists.newArrayList(
            "-c",
            "'echo HELLO WORLD!!!'"
        );

        final JobRequest jobRequest1 = new JobRequest.Builder(
            "job1",
            "user1",
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withTags(Sets.newHashSet("foo", "bar"))
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        jobClient.submitJob(jobRequest1);

        final JobRequest jobRequest2 = new JobRequest.Builder(
            "job2",
            "user2",
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withTags(Sets.newHashSet("foo", "pi"))
            .withCommandArgs(Lists.newArrayList("-c", "'ls blah'"))
            .withId(UUID.randomUUID().toString())
            .withDisableLogArchival(true)
            .build();

        jobClient.submitJob(jobRequest2);

        jobClient.waitForCompletion(jobRequest1.getId().orElseThrow(IllegalArgumentException::new), 60000, 5000);
        jobClient.waitForCompletion(jobRequest2.getId().orElseThrow(IllegalArgumentException::new), 60000, 5000);

        // Get jobs using id
        Assert.assertEquals(
            jobRequest1.getId().orElseThrow(IllegalArgumentException::new),
            jobClient.getJobs(
                jobRequest1.getId().orElseThrow(IllegalArgumentException::new),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ).get(0).getId()
        );

        // Get jobs using user
        Assert.assertEquals(
            jobRequest1.getId().orElseThrow(IllegalArgumentException::new),
            jobClient.getJobs(
                null,
                "job1",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ).get(0).getId()
        );

        // Get jobs using name
        Assert.assertEquals(
            jobRequest2.getId().orElseThrow(IllegalArgumentException::new),
            jobClient.getJobs(
                null,
                null,
                "user2",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ).get(0).getId());

        // Get jobs using status
        Assert.assertEquals(
            jobRequest1.getId().orElseThrow(IllegalArgumentException::new),
            jobClient.getJobs(
                null,
                null,
                null,
                Arrays.stream(new String[]{"SUCCEEDED"}).collect(Collectors.toSet()),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ).get(0).getId());

        // Get jobs using status
        Assert.assertEquals(
            jobRequest2.getId().orElseThrow(IllegalArgumentException::new),
            jobClient.getJobs(
                null,
                null,
                null,
                Arrays.stream(new String[]{"FAILED"}).collect(Collectors.toSet()),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ).get(0).getId());

        // Get jobs using tags
        Assert.assertEquals(2, jobClient.getJobs(
            null,
            null,
            null,
            null,
            Arrays.stream(new String[]{"foo"}).collect(Collectors.toSet()),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ).size());
    }

    /**
     * Method to test get stdout function.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testCanGetJobStdout() throws Exception {

        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final List<String> commandArgs = Lists.newArrayList(
            "-c",
            "'echo HELLO WORLD!!!'"
        );

        final JobRequest jobRequest1 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        this.jobClient.submitJob(jobRequest1);
        this.jobClient.waitForCompletion(jobRequest1.getId().orElseThrow(IllegalArgumentException::new), 60000, 5000);

        try (
            InputStream inputStream1
                = this.jobClient.getJobStdout(jobRequest1.getId().orElseThrow(IllegalArgumentException::new));
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream1, StandardCharsets.UTF_8))
        ) {
            final StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader1.readLine()) != null) {
                sb.append(line);
            }

            Assert.assertEquals("HELLO WORLD!!!", sb.toString());
        }
    }

    /**
     * Method to test get stdout function.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testCanGetJobStderr() throws Exception {

        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("laptop")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final List<String> commandArgs = Lists.newArrayList(
            "-c",
            "'ls foo'"
        );

        final JobRequest jobRequest1 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withId(UUID.randomUUID().toString())
            .withCommandArgs(commandArgs)
            .withDisableLogArchival(true)
            .build();

        this.jobClient.submitJob(jobRequest1);
        this.jobClient.waitForCompletion(jobRequest1.getId().orElseThrow(IllegalArgumentException::new), 60000, 5000);

        try (
            InputStream inputStream1
                = this.jobClient.getJobStderr(jobRequest1.getId().orElseThrow(IllegalArgumentException::new));
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream1, StandardCharsets.UTF_8))
        ) {
            final StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader1.readLine()) != null) {
                sb.append(line);
            }

            Assert.assertEquals("ls: foo: No such file or directory", sb.toString());
        }
    }

    /**
     * Method to test get stdout function.
     *
     * @throws Exception If there is a problem.
     */
    @Test(expected = GenieClientException.class)
    public void testPreconditionFailedException() throws Exception {

        createClusterAndCommandForTest();

        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet("foo")));

        final Set<String> commandCriteria = Sets.newHashSet("bash");

        final JobRequest jobRequest1 = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            clusterCriteriaList,
            commandCriteria
        )
            .withCommandArgs(Lists.newArrayList("-c", "'ls foo'"))
            .withDisableLogArchival(true)
            .build();

        jobClient.submitJob(jobRequest1);
    }

    /**
     * Helper method to create a cluster/command combination for all tests.
     *
     * @throws Exception If it fails to create the cluster/command combination.
     */
    private void createClusterAndCommandForTest() throws Exception {

        final Set<String> tags = Sets.newHashSet("laptop");

        final Cluster cluster = new Cluster.Builder(
            CLUSTER_NAME,
            "user",
            "1.0",
            ClusterStatus.UP
        ).withTags(tags)
            .build();

        final String clusterId = clusterClient.createCluster(cluster);

        tags.clear();
        tags.add("bash");

        final Command command = new Command.Builder(
            COMMAND_NAME,
            "user",
            "version",
            CommandStatus.ACTIVE,
            "bash",
            1000
        )
            .withTags(tags).build();

        final String commandId = commandClient.createCommand(command);

        clusterClient.addCommandsToCluster(clusterId, Lists.newArrayList(commandId));
    }
}
