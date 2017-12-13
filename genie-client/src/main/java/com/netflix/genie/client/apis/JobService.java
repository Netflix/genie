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
package com.netflix.genie.client.apis;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import okhttp3.MultipartBody;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.DELETE;
import retrofit2.http.GET;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;
import retrofit2.http.Path;
import retrofit2.http.Query;
import retrofit2.http.Streaming;

import java.util.List;
import java.util.Set;

/**
 * An interface that provides all methods needed for the Genie job client implementation.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface JobService {

    /**
     * Path to Jobs.
     */
    String JOBS_URL_SUFFIX = "/api/v3/jobs";

    /**
     * Method to submit a job to Genie.
     *
     * @param request The request object containing all the
     * @return A callable object.
     */
    @POST(JOBS_URL_SUFFIX)
    Call<Void> submitJob(@Body final JobRequest request);

    /**
     * Submit a job with attachments.
     *
     * @param request     A JobRequest object containing all the details needed to run the job.
     * @param attachments A list of all the attachment files to be sent to the server.
     * @return A callable object.
     */
    @Multipart
    @POST(JOBS_URL_SUFFIX)
    Call<Void> submitJobWithAttachments(
        @Part("request") JobRequest request,
        @Part List<MultipartBody.Part> attachments);

    /**
     * Method to get all jobs from Genie.
     *
     * @param id               id for job
     * @param name             name of job (can be a SQL-style pattern such as HIVE%)
     * @param user             user who submitted job
     * @param statuses         statuses of jobs to find
     * @param tags             tags for the job
     * @param clusterName      the name of the cluster
     * @param clusterId        the id of the cluster
     * @param commandName      the name of the command run by the job
     * @param commandId        the id of the command run by the job
     * @param minStarted       The time which the job had to start after in order to be return (inclusive)
     * @param maxStarted       The time which the job had to start before in order to be returned (exclusive)
     * @param minFinished      The time which the job had to finish after in order to be return (inclusive)
     * @param maxFinished      The time which the job had to finish before in order to be returned (exclusive)
     * @param grouping         The grouping the job should be a member of
     * @param groupingInstance The grouping instance the job should be a member of
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX)
    Call<JsonNode> getJobs(
        @Query("id") final String id,
        @Query("name") final String name,
        @Query("user") final String user,
        @Query("status") final Set<String> statuses,
        @Query("tag") final Set<String> tags,
        @Query("clusterName") final String clusterName,
        @Query("clusterId") final String clusterId,
        @Query("commandName") final String commandName,
        @Query("commandId") final String commandId,
        @Query("minStarted") final Long minStarted,
        @Query("maxStarted") final Long maxStarted,
        @Query("minFinished") final Long minFinished,
        @Query("maxFinished") final Long maxFinished,
        @Query("grouping") final String grouping,
        @Query("groupingInstance") final String groupingInstance
    );

    /**
     * Method to fetch a single job from Genie.
     *
     * @param jobId The id of the job to get.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}")
    Call<Job> getJob(@Path("id") final String jobId);

    /**
     * Method to fetch the stdout of a job from Genie.
     *
     * @param jobId The id of the job whose stdout is desired.
     * @return A callable object.
     */
    @Streaming
    @GET(JOBS_URL_SUFFIX + "/{id}/output/stdout")
    Call<ResponseBody> getJobStdout(@Path("id") final String jobId);

    /**
     * Method to fetch the stderr of a job from Genie.
     *
     * @param jobId The id of the job whose stderr is desired.
     * @return A callable object.
     */
    @Streaming
    @GET(JOBS_URL_SUFFIX + "/{id}/output/stderr")
    Call<ResponseBody> getJobStderr(@Path("id") final String jobId);

    /**
     * Method to get Job status.
     *
     * @param jobId The id of the job whose status is desired.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}/status")
    Call<JsonNode> getJobStatus(@Path("id") final String jobId);

    /**
     * Method to get the cluster information on which a job is run.
     *
     * @param jobId The id of the job.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}/cluster")
    Call<Cluster> getJobCluster(@Path("id") final String jobId);

    /**
     * Method to get the command information on which a job is run.
     *
     * @param jobId The id of the job.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}/command")
    Call<Command> getJobCommand(@Path("id") final String jobId);

    /**
     * Method to get the JobRequest for a job.
     *
     * @param jobId The id of the job.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}/request")
    Call<JobRequest> getJobRequest(@Path("id") final String jobId);

    /**
     * Method to get the execution information for a job.
     *
     * @param jobId The id of the job.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}/execution")
    Call<JobExecution> getJobExecution(@Path("id") final String jobId);

    /**
     * Method to get the Applications for a job.
     *
     * @param jobId The id of the job.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}/applications")
    Call<List<Application>> getJobApplications(@Path("id") final String jobId);

    /**
     * Method to send a job kill request to Genie.
     *
     * @param jobId The id of the job.
     * @return A callable object.
     */
    @DELETE(JOBS_URL_SUFFIX + "/{id}")
    Call<Void> killJob(@Path("id") final String jobId);
}
