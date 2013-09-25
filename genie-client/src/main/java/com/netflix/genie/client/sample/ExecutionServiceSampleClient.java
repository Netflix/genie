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

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.client.ExecutionServiceClient;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.FileAttachment;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.common.model.Types.Configuration;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.common.model.Types.JobType;
import com.netflix.genie.common.model.Types.Schedule;

import com.sun.jersey.core.util.MultivaluedMapImpl;

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
     * Main for running client code.
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
        MultivaluedMapImpl params = new MultivaluedMapImpl();
        params.add("userName", userName);
        params.add("jobType", JobType.HIVE.name());
        params.add("status", JobStatus.FAILED.name());
        params.add("limit", 3);
        JobInfoElement[] responses = client.getJobs(params);
        for (JobInfoElement ji : responses) {
            System.out.println("Job Info: {id, status, finishTime} - {"
                    + ji.getJobID() + ", " + ji.getStatus() + ", "
                    + ji.getFinishTime() + "}");
        }

        System.out.println("Running Hive job");
        JobInfoElement jobInfo = new JobInfoElement();
        jobInfo.setUserName(userName);
        jobInfo.setJobType(JobType.HIVE.name());
        jobInfo.setDescription("This is a test");
        jobInfo.setConfiguration(Configuration.TEST.name());
        jobInfo.setSchedule(Schedule.ADHOC.name());
        // send the query as an attachment
        File query = File.createTempFile("hive", ".q");
        PrintWriter pw = new PrintWriter(query);
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

        String jobID = jobInfo.getJobID();
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
