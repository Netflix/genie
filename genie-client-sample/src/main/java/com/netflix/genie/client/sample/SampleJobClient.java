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
package com.netflix.genie.client.sample;

import com.google.common.collect.Sets;
import com.netflix.genie.client.JobClient;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Sample class that exhibits how to use the JobClient class to run and monitor a job.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public final class SampleJobClient {

    /**
     * Default private constructor.
     *
     */
    private SampleJobClient() {
        // private constructor
    }

    /**
     * Main method that uses the JobClient.
     *
     * @param args The args to the main application.
     *
     * @throws Exception For all other issues.
     */
    public static void main(final String[] args) throws Exception {

        log.debug("Starting Execution.");

        final JobClient jobClient = new JobClient("http://localhost:8080");

        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final String clusterTag = "laptop";
        final ClusterCriteria clusterCriteria = new ClusterCriteria(Sets.newHashSet(clusterTag));
        clusterCriteriaList.add(clusterCriteria);

        final HashSet<String> tags = new HashSet<>();
        tags.add("foo");
        tags.add("bar");

//        final String setUpFile = this.resourceLoader.getResource(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile")
//            .getFile()
//            .getAbsolutePath();
//
//        final Set<String> dependencies = new HashSet<>();
//        final String depFile1 = this.resourceLoader.
// getResource(BASE_DIR + "job" + FILE_DELIMITER + "dep1").getFile()
//            .getAbsolutePath();
//        dependencies.add(depFile1);

        final String commandTag = "bash";
        final Set<String> commandCriteria = Sets.newHashSet(commandTag);
        final JobRequest jobRequest = new JobRequest.Builder(
            "Genie 3.0 Test job",
            "amsharma",
            "1.0",
            commandArgs,
            clusterCriteriaList,
            commandCriteria
        )
            .withDisableLogArchival(true)
            .withTags(tags)
//            .withSetupFile(setUpFile)
//            .withDependencies(dependencies)
//            .withDescription(JOB_DESCRIPTION)
            .build();

//        final ArrayList<String> files = new ArrayList<>();
//        files.add("file path");
//        files.add("file path");

          //final String jobId  = jobClient.submitJobWithAttachments(jobRequest, files);

        final String jobId  = jobClient.submitJob(jobRequest);
        log.info(jobClient.getJob(jobId).toString());
        log.info(jobClient.getJobRequest(jobId).toString());
        log.info(jobClient.getJobCluster(jobId).toString());
        log.info(jobClient.getJobCommand(jobId).toString());
        //log.info(jobClient.getJobApplications(jobId).toString());
        log.info(jobClient.getJobExecution(jobId).toString());
//
////        final InputStream inputStream1 = jobClient.getJobStdout(jobId);
////        final BufferedReader reader1 = new BufferedReader(new InputStreamReader(inputStream1));
////
////        String line;
////        while ((line = reader1.readLine()) != null) {
////           log.info(line);
////        }
////
////
////        final InputStream inputStream2 = jobClient.getJobStderr(jobId);
////        final BufferedReader reader2 = new BufferedReader(new InputStreamReader(inputStream2));
////
////        while ((line = reader2.readLine()) != null) {
////            log.info(line);
////        }
//
//        final Map<String, String> tagMap = new HashMap<>();
//        tagMap.put("tag", "bar,foo");
//        log.info(jobClient.getJobs(tagMap).toString());
        //jobClient.getJobs().forEach(job -> log.info(job.toString()));
        //log.info(jobClient.getJobs().toString());
    }
}
