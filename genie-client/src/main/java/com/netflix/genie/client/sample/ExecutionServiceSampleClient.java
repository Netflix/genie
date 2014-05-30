/*
 *
 *  Copyright 2013 Netflix, Inc.
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

import java.io.File;
import java.io.PrintWriter;

import javax.activation.DataHandler;
import javax.activation.FileDataSource;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.client.ExecutionServiceClient;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;

/**
 * A sample client demonstrating usage of the Execution Service Client.
 *
 * @author skrishnan
 *
 */
public final class ExecutionServiceSampleClient {

    private ExecutionServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code
     * .
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

        String userName = "genietest";
        System.out.println("Getting jobInfos using specified filter criteria");
        Multimap<String, String> params = ArrayListMultimap.create();
        params.put("userName", userName);
        params.put("status", JobStatus.FAILED.name());
        params.put("limit", "3");
        for (Job ji : client.getJobs(params)) {
            System.out.println("Job Info: {id, status, finishTime} - {"
                    + ji.getId() + ", " + ji.getStatus() + ", "
                    + ji.getFinishTime() + "}");
        }

        System.out.println("Running Hive job");
        Job jobInfo = new Job();
        jobInfo.setUserName(userName);
        jobInfo.setDescription("This is a test");
//        jobInfo.setConfiguration(Configuration.TEST.name());
//        jobInfo.setSchedule(Schedule.ADHOC.name());
        // send the query as an attachment
        File query = File.createTempFile("hive", ".q");
        PrintWriter pw = new PrintWriter(query, "UTF-8");
        pw.println("select count(*) from counters where dateint=20120430 and hour=10;");
        pw.close();
        FileAttachment[] attachments = new FileAttachment[1];
        attachments[0] = new FileAttachment();
        attachments[0].setName("hive.q");
        // Ensure that file exists, because the FileDataSource constructor doesn't
        attachments[0].setData(new DataHandler(new FileDataSource(query.getAbsolutePath())));
        jobInfo.setAttachments(attachments);
        jobInfo.setCmdArgs("-f hive.q");
        jobInfo = client.submitJob(jobInfo);

        String jobID = jobInfo.getId();
        String outputURI = jobInfo.getOutputURI();
        System.out.println("Job ID: " + jobID);
        System.out.println("Output URL: " + outputURI);

        System.out.println("Getting jobInfo by jobID");
        jobInfo = client.getJob(jobID);
        System.out.println("Job status: " + jobInfo.getStatus());

        System.out.println("Waiting for job to finish");
        jobInfo = client.waitForCompletion(jobID, 600000, 5000);
        System.out.println("Job status: " + jobInfo.getStatus());

        System.out.println("Killing jobs using jobID");
        JobStatusResponse jobStatus = client.killJob(jobID);
        System.out.println("Message from server: " + jobStatus.getMessage());
        System.out.println("Job status: " + jobStatus.getStatus());

        System.out.println("Done");
    }
}
