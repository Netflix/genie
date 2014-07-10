/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.client.ExecutionServiceClient;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.JobStatus;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.activation.DataHandler;
import javax.activation.FileDataSource;

/**
 * A sample client demonstrating usage of the Execution Service Client.
 *
 * @author skrishnan
 * @author tgianos
 */
public final class ExecutionServiceSampleClient {

    private ExecutionServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code .
     *
     * @param args
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {

        // Initialize Eureka, if it is being used
        // System.out.println("Initializing Eureka");
        // ExecutionServiceClient.initEureka("test");
        System.out.println("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genieClient.ribbon.listOfServers",
                "localhost:7001");

        System.out.println("Initializing ExecutionServiceClient");
        ExecutionServiceClient client = ExecutionServiceClient.getInstance();

        final String userName = "genietest";
        System.out.println("Getting jobInfos using specified filter criteria");
        final Multimap<String, String> params = ArrayListMultimap.create();
        params.put("userName", userName);
        params.put("status", JobStatus.FAILED.name());
        params.put("limit", "3");
        for (final Job ji : client.getJobs(params)) {
            System.out.println("Job Info: {id, status, finishTime} - {"
                    + ji.getId() + ", " + ji.getStatus() + ", "
                    + ji.getFinishTime() + "}");
        }

        System.out.println("Running Hive job");
        final Set<String> criteriaTags = new HashSet<String>();
        criteriaTags.add("prod");
        final ClusterCriteria criteria = new ClusterCriteria(criteriaTags);
        final List<ClusterCriteria> criterias = new ArrayList<ClusterCriteria>();
        criterias.add(criteria);
        Job job = new Job(
                userName,
                CommandServiceSampleClient.ID,
                null,
                "-f hive.q",
                criterias);
        job.setDescription("This is a test");
        // send the query as an attachment
        File query = File.createTempFile("hive", ".q");
        PrintWriter pw = new PrintWriter(query, "UTF-8");
        pw.println("select count(*) from counters where dateint=20120430 and hour=10;");
        pw.close();
        final Set<FileAttachment> attachments = new HashSet<FileAttachment>();
        final FileAttachment attachment = new FileAttachment();
        attachment.setName("hive.q");
        // Ensure that file exists, because the FileDataSource constructor doesn't
        attachment.setData(new DataHandler(new FileDataSource(query.getAbsolutePath())));
        attachments.add(attachment);
        job.setAttachments(attachments);
        job = client.submitJob(job);

        String jobID = job.getId();
        String outputURI = job.getOutputURI();
        System.out.println("Job ID: " + jobID);
        System.out.println("Output URL: " + outputURI);

        System.out.println("Getting jobInfo by jobID");
        job = client.getJob(jobID);
        System.out.println("Job status: " + job.getStatus());

        System.out.println("Waiting for job to finish");
        job = client.waitForCompletion(jobID, 600000, 5000);
        System.out.println("Job status: " + job.getStatus());

        System.out.println("Killing jobs using jobID");
        Job killedJob = client.killJob(jobID);
        System.out.println("Job status: " + killedJob.getStatus());

        System.out.println("Done");
    }
}
