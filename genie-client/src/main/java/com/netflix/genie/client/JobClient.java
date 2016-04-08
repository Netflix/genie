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
package com.netflix.genie.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.apache.commons.configuration2.Configuration;
import retrofit2.Call;
import retrofit2.Response;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Client library for the Job Service.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobClient extends BaseGenieClient {

    /**
     * Constructor.
     *
     * @param configuration The configuration object containing all information for instantiating the client.
     *
     * @throws GenieException If there is any problem.
     */
    public JobClient(
        final Configuration configuration
    ) throws GenieException {
        super(configuration);
    }

    /**
     * Submit a job to genie using the jobRequest provided.
     *
     * @param jobRequest A job request containing all the details for running a job.
     *
     * @return jobId The id of the job submitted.
     * @throws GenieException If there is any problem.
     */
    public String submitJob(
        final JobRequest jobRequest
        ) throws GenieException {

        try {
            final Call<Void> post = genieService.submitJob(jobRequest);

            final Response<Void> response = post.execute();
            if (response.isSuccessful()) {
                return getIdFromLocation(response.headers().get("location"));
            } else {
                throw new GenieServerException("Could not submit job");
            }
        } catch (Exception e) {
            throw new GenieServerException("Could not submit job", e);
        }
    }

    /**
     * Submit a job to genie using the jobRequest and attachments provided.
     *
     * @param jobRequest A job request containing all the details for running a job.
     * @param files A list of uri's to the files needed to be sent to the server.
     *
     * @return jobId The id of the job submitted.
     * @throws GenieException If there is any problem.
     */
    public String submitJobWithAttachments(
        final JobRequest jobRequest,
        final List<URI> files
    ) throws GenieException {
        try {

            final MultipartBody.Part part = MultipartBody.Part.createFormData("attachment", "foo1.txt",
                    RequestBody.create(MediaType.parse("application/octet-stream"), new File("/Users/amsharma/foo.txt")));

            final MultipartBody.Part part1 = MultipartBody.Part.createFormData("attachment", "foo2.txt",
                RequestBody.create(MediaType.parse("application/octet-stream"), new File("/Users/amsharma/foo.txt")));

            final ArrayList<MultipartBody.Part> attachmentFiles = new ArrayList<>();
            attachmentFiles.add(part);
            attachmentFiles.add(part1);

            final Call<Void> post = genieService.submitJobWithAttachments(jobRequest, attachmentFiles);

            final Response<Void> response = post.execute();
            if (response.isSuccessful()) {
                return getIdFromLocation(response.headers().get("location"));
            } else {
                throw new GenieServerException("Could not submit job");
            }
        } catch (Exception e) {
            throw new GenieServerException("Could not submit job", e);
        }
    }

    /**
     * Method to get a list of all the jobs from Genie for the query parameters specified.
     *
     * @return A list of jobs.
     * @throws GenieException If there is any problem.
     */
    public List<Job> getJobs() throws GenieException {
        final Call<JsonNode> jobsCallable = genieService.getJobs();
        try {
            final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            final Response<JsonNode> response = jobsCallable.execute();
            if (response.isSuccessful()) {
                final JsonNode content = response.body();
                final ObjectMapper mapper = new ObjectMapper();
                final ArrayList<Map<String, String>> jobs = (ArrayList<Map<String, String>>)
                    mapper.readValue(content.get("_embedded").get("jobSearchResultList").toString(),
                        List.class);

                final ArrayList<Job> jobList = new ArrayList<>();
                for (Map<String, String> job: jobs) {
                    jobList.add(
                        new Job.Builder(
                            job.get("name"),
                            job.get("user"),
                            job.get("version"),
                            job.get("commandArgs")
                        )
                            .withId(job.get("id"))
                            .withStatus(JobStatus.parse(job.get("status")))
                            .withClusterName(job.get("clusterName"))
                            .withCommandName(job.get("commandName"))
                            .withStarted(dateFormatter.parse(job.get("started")))
                            .withFinished(dateFormatter.parse(job.get("finished")))
                            .build());
                }

                return jobList;
            } else {
                throw new GenieServerException("Could not fetch jobs due to error code: "
                    + response.code() + " Error Message:  " + response.message());
            }

        } catch (Exception e) {
            throw new GenieServerException("Could not fetch jobs.", e);
        }
    }

    /**
     * Method to get a job from Genie.
     *
     * @param jobId The id of the job to get.
     * @return The job details.
     * @throws GenieException If there is any problem.
     */
    public Job getJob(
        final String jobId
    ) throws GenieException {

        final Call<Job> jobCall = genieService.getJob(jobId);
        try {
            final Response<Job> response = jobCall.execute();
            // Check for 404 vs 5xx before throwing exception
            return response.body();
        } catch (Exception e) {
            throw  new GenieServerException("Could not fetch jobs.", e);
        }
    }
}
