package com.netflix.genie.client.sample;

import com.google.common.collect.Sets;
import com.netflix.genie.client.JobClient;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import groovy.util.logging.Slf4j;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 * Sample class that exhibits how to use the JobClient class to run and monitor a job.
 *
 * @author amsharma
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
     * @throws GenieException If there is any problem.
     */
    public static void main(final String[] args) throws GenieException {

        final JobClient jobClient = new JobClient("http://localhost:8080");

        final String commandArgs = "-c 'echo hello world'";

        final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
        final String clusterTag = "laptop";
        final ClusterCriteria clusterCriteria = new ClusterCriteria(Sets.newHashSet(clusterTag));
        clusterCriteriaList.add(clusterCriteria);

//        final String setUpFile = this.resourceLoader.getResource(BASE_DIR + "job" + FILE_DELIMITER + "jobsetupfile")
//            .getFile()
//            .getAbsolutePath();
//
//        final Set<String> dependencies = new HashSet<>();
//        final String depFile1 = this.resourceLoader.getResource(BASE_DIR + "job" + FILE_DELIMITER + "dep1").getFile()
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
//            .withSetupFile(setUpFile)
//            .withDependencies(dependencies)
//            .withDescription(JOB_DESCRIPTION)
            .build();

        final String jobId  = jobClient.submitJob(jobRequest);

        System.out.println("Job Id:" + jobId);
        System.out.println(jobClient.getJob("Job Details: " + jobId));
        System.out.println(jobClient.getJobs());
    }
}
