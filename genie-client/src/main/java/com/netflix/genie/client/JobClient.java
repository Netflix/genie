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
import com.netflix.genie.client.apis.JobService;
import com.netflix.genie.client.security.SecurityInterceptor;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Response;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Client library for the Job Service.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobClient extends BaseGenieClient {

    private static final String ATTACHMENT = "attachment";
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    private final JobService jobService;

    /**
     * Constructor.
     *
     * @param url The url of the Genie Service.
     * @param securityInterceptor An implementation of the Security Interceptor.
     *
     * @throws GenieException If there is any problem.
     */
    public JobClient(
        final String url,
        final SecurityInterceptor securityInterceptor
    ) throws GenieException {
        super(url, securityInterceptor);
        jobService = retrofit.create(JobService.class);
     }

    /**
     * Constructor that takes only the URL.
     *
     * @param url The url of the Genie Service.
     * @throws GenieException If there is any problem.
     */
    // TODO Can we get rid of one constructor in either BaseGenieClient or JobClient.
    public JobClient(
        final String url
    ) throws GenieException {
        super(url, null);
        jobService = retrofit.create(JobService.class);
    }

    /**
     * Submit a job to genie using the jobRequest provided.
     *
     * @param jobRequest A job request containing all the details for running a job.
     *
     * @return jobId The id of the job submitted.
     *
     * @throws GenieException For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public String submitJob(
        final JobRequest jobRequest
    ) throws IOException, GenieException {
        if (jobRequest == null) {
            throw new GeniePreconditionException("Job Request cannot be null.");
        }
        return getIdFromLocation(jobService.submitJob(jobRequest).execute().headers().get("location"));
    }

    /**
     * Submit a job to genie using the jobRequest and attachments provided.
     *
     * @param jobRequest A job request containing all the details for running a job.
     * @param filePaths A list of filesPaths needed to be sent to the server as attachments.
     *
     * @return jobId The id of the job submitted.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public String submitJobWithAttachments(
        final JobRequest jobRequest,
        final List<String> filePaths
    ) throws IOException, GenieException {
        if (jobRequest == null) {
            throw new GeniePreconditionException("Job Request cannot be null.");
        }

        final ArrayList<MultipartBody.Part> attachmentFiles = new ArrayList<>();

        for (String path: filePaths) {
            final String fileName = path.substring(path.lastIndexOf(FILE_PATH_DELIMITER) + 1);
            final MultipartBody.Part part = MultipartBody.Part.createFormData(
                ATTACHMENT,
                fileName,
                RequestBody.create(MediaType.parse(APPLICATION_OCTET_STREAM), new File(path)));

            attachmentFiles.add(part);
        }

        final Response response = jobService.submitJobWithAttachments(jobRequest, attachmentFiles).execute();
        return getIdFromLocation(response.headers().get("location"));
    }

    /**
     * Method to get a list of all the jobs.
     *
     * @return A list of jobs.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public List<JobSearchResult> getJobs() throws IOException, GenieException {
        return this.getJobs(Collections.emptyMap());
    }

    /**
     * Method to get a list of all the jobs from Genie for the query parameters specified.
     *
     * @param options A list of query options
     *
     * @return A list of jobs.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public List<JobSearchResult> getJobs(final Map<String, String> options) throws IOException, GenieException {

        final JsonNode jnode =  jobService.getJobs(options).execute().body()
            .get("_embedded")
            .get("jobSearchResultList");

        final List<JobSearchResult> jobList = new ArrayList<>();
        for (final JsonNode objNode : jnode) {
            final JobSearchResult jobSearchResult  = mapper.treeToValue(objNode, JobSearchResult.class);
            jobList.add(jobSearchResult);
        }

        return jobList;
    }

    /**
     * Method to get a job from Genie.
     *
     * @param jobId The id of the job to get.
     * @return The job details.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public Job getJob(
        final String jobId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }
        return jobService.getJob(jobId).execute().body();
    }

    /**
     * Method to get the cluster on which the job executes.
     *
     * @param jobId The id of the job.
     * @return The cluster object.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public Cluster getJobCluster(
        final String jobId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }
        return jobService.getJobCluster(jobId).execute().body();
    }

    /**
     * Method to get the command on which the job executes.
     *
     * @param jobId The id of the job.
     * @return The command object.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public Command getJobCommand(
        final String jobId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }
        return jobService.getJobCommand(jobId).execute().body();
    }

    /**
     * Method to get the Job Request for the job.
     *
     * @param jobId The id of the job.
     * @return The command object.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public JobRequest getJobRequest(
        final String jobId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }
        return jobService.getJobRequest(jobId).execute().body();
    }

    /**
     * Method to get the Job Execution information for the job.
     *
     * @param jobId The id of the job.
     * @return The command object.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public JobExecution getJobExecution(
        final String jobId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }
        return jobService.getJobExecution(jobId).execute().body();
    }

    /**
     * Method to get the Applications for the job.
     *
     * @param jobId The id of the job.
     * @return The list of Applications.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public List<Application> getJobApplications(
        final String jobId
    ) throws IOException, GenieException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }
        return jobService.getJobApplications(jobId).execute().body();
    }

    /**
     * Method to fetch the stdout of a job from Genie.
     *
     * @param jobId The id of the job whose output is desired.
     *
     * @return An inputstream to the output contents.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public InputStream getJobStdout(
        final String jobId
    ) throws IOException, GenieException {
        if (!this.getJobStatus(jobId).equals(JobStatus.SUCCEEDED)) {
            throw new GenieException(400, "Cannot request output of a job whose status is not SUCCEEDED.");
        }
        return jobService.getJobStdout(jobId).execute().body().byteStream();
    }

    /**
     * Method to fetch the stderr of a job from Genie.
     *
     * @param jobId The id of the job whose stderr is desired.
     *
     * @return An inputstream to the stderr contents.
     *
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public InputStream getJobStderr(
        final String jobId
    ) throws IOException, GenieException {
        if (!this.getJobStatus(jobId).equals(JobStatus.SUCCEEDED)) {
            throw new GenieException(400, "Cannot request output of a job whose status is not SUCCEEDED.");
        }
        return jobService.getJobStderr(jobId).execute().body().byteStream();
    }

    /**
     * Method to fetch the status of a job.
     *
     * @param jobId The id of the job.
     *
     * @return The status of the Job.
     * @throws GenieException       For any other error.
     * @throws IOException If the response received is not 2xx.
     */
    public JobStatus getJobStatus(
        final String jobId
    ) throws IOException, GenieException {
        final JsonNode jsonNode = jobService.getJobStatus(jobId).execute().body();
        return JobStatus.parse(jsonNode.get(STATUS).asText());
    }

    /**
     * Method to send a kill job request to Genie.
     *
     * @param jobId The id of the job.
     * @throws IOException If there is a problem while sending the request.
     */
    public void killJob(final String jobId) throws IOException {
        jobService.killJob(jobId).execute();
    }

    /**
     * Wait for job to complete, until the given timeout.
     *
     * @param jobId           the Genie job ID to wait for completion
     * @param blockTimeout the time to block for (in ms), after which a
     *                     GenieException will be thrown
     * @param pollTime     the time to sleep between polling for job status
     * @return the jobInfo for the job after completion
     * @throws GenieException       For any other error.
     * @throws InterruptedException on timeout/thread errors
     * @throws IOException If the response received is not 2xx.
     */
    public JobStatus waitForCompletion(final String jobId, final long blockTimeout, final long pollTime)
        throws GenieException, InterruptedException, IOException {
        if (StringUtils.isEmpty(jobId)) {
            throw new GeniePreconditionException("Missing required parameter: jobId.");
        }

        final long startTime = System.currentTimeMillis();

        // wait for job to finish
        while (true) {

            final JobStatus status = this.getJobStatus(jobId);

            if (status == JobStatus.FAILED || status == JobStatus.KILLED || status == JobStatus.SUCCEEDED) {
                return status;
            }

            // block until timeout
            if (System.currentTimeMillis() - startTime < blockTimeout) {
                Thread.sleep(pollTime);
            } else {
                throw new InterruptedException("Timed out waiting for job to finish");
            }
        }
    }
}
