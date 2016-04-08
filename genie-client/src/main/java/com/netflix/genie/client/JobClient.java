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
import retrofit2.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
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


    private static final String ATTACHMENTS = "attachment";

    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

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
     * @throws IOException If there is any problem.
     */
    public String submitJob(
        final JobRequest jobRequest
        ) throws IOException {

        final Response response = genieService.submitJob(jobRequest).execute();
        return getIdFromLocation(response.headers().get("location"));
    }

    /**
     * Submit a job to genie using the jobRequest and attachments provided.
     *
     * @param jobRequest A job request containing all the details for running a job.
     * @param filePaths A list of filesPaths needed to be sent to the server as attachments.
     *
     * @return jobId The id of the job submitted.
     * @throws IOException If there is any problem.
     */
    public String submitJobWithAttachments(
        final JobRequest jobRequest,
        final List<String> filePaths
    ) throws IOException {
            final ArrayList<MultipartBody.Part> attachmentFiles = new ArrayList<>();

            for (String path: filePaths) {
                final String fileName = path.substring(path.lastIndexOf(FILE_PATH_DELIMITER) + 1);
                final MultipartBody.Part part = MultipartBody.Part.createFormData(
                    ATTACHMENTS,
                    fileName,
                    RequestBody.create(MediaType.parse(APPLICATION_OCTET_STREAM), new File(path)));

                attachmentFiles.add(part);
            }

            final Response response = genieService.submitJobWithAttachments(jobRequest, attachmentFiles).execute();
            return getIdFromLocation(response.headers().get("location"));
    }

    /**
     * Method to get a list of all the jobs from Genie for the query parameters specified.
     *
     * @return A list of jobs.
     * @throws GenieException If there is a Genie server issue.
     * @throws IOException If there is a problem with the request.
     */
    public List<Job> getJobs() throws IOException, GenieException {

        try {
            final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            final Response<JsonNode> response = genieService.getJobs().execute();
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

        } catch (ParseException e) {
            throw new GenieServerException("Could not fetch jobs.");
        }
    }

    /**
     * Method to get a job from Genie.
     *
     * @param jobId The id of the job to get.
     * @return The job details.
     * @throws IOException If there is any problem.
     */
    public Job getJob(
        final String jobId
    ) throws IOException {
        return genieService.getJob(jobId).execute().body();

    }

    /**
     * Method to fetch the stdout of a job from Genie.
     *
     * @param jobId The id of the job whose output is desired.
     *
     * @return An inputstream to the output contents.
     * @throws IOException If there is any problem.
     */
    public InputStream getJobOutput(
        final String jobId
    ) throws IOException {
        // TODO make sure Job is successful before returning stdout
        return genieService.getJobOutput(jobId).execute().body().byteStream();
    }
}
