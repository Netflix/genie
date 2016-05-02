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
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Integration tests for Genie Job Client.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobClientIntegrationTests extends GenieClientsIntegrationTestsBase {

    private static final String JOB_NAME = "List Directories bash job";
    private static final String JOB_USER = "genie";
    private static final String JOB_VERSION = "1.0";
    private static final String JOB_DESCRIPTION = "Genie 3 Test Job";

    private ClusterClient clusterClient;
    private CommandClient commandClient;
    //private ApplicationClient applicationClient;
    private JobClient jobClient;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        clusterClient = new ClusterClient(getBaseUrl());
        commandClient = new CommandClient(getBaseUrl());
        //applicationClient = new ApplicationClient(getBaseUrl());
        jobClient = new JobClient(getBaseUrl());
    }

    /**
     * Method to test submitting a job.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void canSubmitJob() throws Exception {

        final String jobId = UUID.randomUUID().toString();
        final Set<String> tags = new HashSet<>();
        tags.add("laptop");

        final Cluster cluster = new Cluster.Builder(
            "name",
            "user",
            "1.0",
            ClusterStatus.UP
        ).withTags(tags)
            .withId("cluster")
            .build();

        clusterClient.createCluster(cluster);

        tags.clear();
        tags.add("bash");

        final Command command = new Command.Builder(
            "name",
            "user",
            "version",
            CommandStatus.ACTIVE,
            "bash",
            1000
        ).withId("command").withTags(tags).build();

        commandClient.createCommand(command);

        clusterClient.addCommandsToCluster(cluster.getId(), Arrays.asList(command.getId()));

        final String clusterTag = "laptop";
        final List<ClusterCriteria> clusterCriteriaList
            = Lists.newArrayList(new ClusterCriteria(Sets.newHashSet(clusterTag)));

//        final String depFile1
//            = this.resourceLoader.getResource(BASE_DIR + "job" + FILE_DELIMITER + "dep1").getFile().getAbsolutePath();
//        final Set<String> dependencies = Sets.newHashSet(depFile1);

        final String commandTag = "bash";
        final Set<String> commandCriteria = Sets.newHashSet(commandTag);

        final JobRequest jobRequest = new JobRequest.Builder(
            JOB_NAME,
            JOB_USER,
            JOB_VERSION,
            "-c 'echo HELLO WORLD!!!'",
            clusterCriteriaList,
            commandCriteria
        )
            .withId(jobId)
            .withDisableLogArchival(true)
//            .withSetupFile(setUpFile)
//            .withDependencies(dependencies)
            .withDescription(JOB_DESCRIPTION)
            .build();

        final String id = jobClient.submitJob(jobRequest);

        final JobStatus jobStatus = jobClient.waitForCompletion(jobId, 10000, 1);

        Assert.assertEquals(JobStatus.SUCCEEDED, jobStatus);
        final Job job = jobClient.getJob(id);

        Assert.assertEquals(jobId, job.getId());

        final JobRequest jobRequest1 = jobClient.getJobRequest(jobId);
        Assert.assertEquals(jobId, jobRequest1.getId());

        final JobExecution jobExecution = jobClient.getJobExecution(jobId);
        Assert.assertEquals(jobId, jobExecution.getId());

        final InputStream inputStream1 = jobClient.getJobStdout(jobId);
        final BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream1, "UTF-8"));


        final StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader1.readLine()) != null) {
           sb.append(line);
        }
        reader1.close();
        inputStream1.close();
        
        Assert.assertEquals("HELLO WORLD!!!", sb.toString());
    }
}
