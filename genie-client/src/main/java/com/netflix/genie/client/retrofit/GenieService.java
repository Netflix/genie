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
package com.netflix.genie.client.retrofit;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import okhttp3.MultipartBody;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Multipart;
import retrofit2.http.POST;
import retrofit2.http.Part;
import retrofit2.http.Path;

import java.util.List;

/**
 * An interface for Retrofit to use and generate all the methods needed for the Genie client implementation.
 *
 * @author amsharma
 * @since 3.0.0
 */
public interface GenieService {

    /**
     * Path to Jobs.
     */
    String JOBS_URL_SUFFIX = "/api/v3/jobs";

    /**
     * Method to get all jobs from Genie.
     *
     * @return A callable object.
     */
    // TODO need to add query parameters
    @GET(JOBS_URL_SUFFIX)
    Call<JsonNode> getJobs();

    /**
     * Method to fetch a single job from Genie.
     *
     * @param jobId The id of the job to get.
     * @return A callable object.
     */
    @GET(JOBS_URL_SUFFIX + "/{id}")
    Call<Job> getJob(@Path("id") final String jobId);

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
     * @param request A JobRequest object containing all the details needed to run the job.
     * @param attachments A list of all the attachment files to be sent to the server.
     *
     * @return A callable object.
     */
    @Multipart
    @POST(JOBS_URL_SUFFIX)
    Call<Void> submitJobWithAttachments(@Part("request") JobRequest request, @Part List<MultipartBody.Part> attachments);
}
