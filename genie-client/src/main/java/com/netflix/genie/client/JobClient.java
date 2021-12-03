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
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.io.ByteStreams;
import com.netflix.genie.client.apis.JobService;
import com.netflix.genie.client.apis.SortAttribute;
import com.netflix.genie.client.apis.SortDirection;
import com.netflix.genie.client.configs.GenieNetworkConfiguration;
import com.netflix.genie.client.exceptions.GenieClientException;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieTimeoutException;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import org.apache.commons.lang3.StringUtils;
import retrofit2.Retrofit;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Client library for the Job Service.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobClient {

    private static final String STATUS = "status";
    private static final String ATTACHMENT = "attachment";
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    private static final String EMPTY_STRING = "";
    private static final int ZERO = 0;
    private static final String STDOUT = "stdout";
    private static final String STDERR = "stderr";

    private final JobService jobService;
    private final int maxStatusRetries;

    /**
     * Constructor.
     *
     * @param retrofit         The configured {@link Retrofit} client to a Genie server
     * @param maxStatusRetries The maximum number of retries to check for job status
     */
    public JobClient(@NotNull final Retrofit retrofit, final int maxStatusRetries) {
        this.jobService = retrofit.create(JobService.class);
        this.maxStatusRetries = maxStatusRetries;
    }

    /**
     * Constructor.
     *
     * @param url                       The endpoint URL of the Genie API. Not null or empty
     * @param interceptors              Any interceptors to configure the client with, can include security ones
     * @param genieNetworkConfiguration The network configuration parameters. Could be null
     * @throws GenieClientException On error
     * @deprecated Use {@link #JobClient(Retrofit, int)}
     */
    @Deprecated
    public JobClient(
        @NotEmpty final String url,
        @Nullable final List<Interceptor> interceptors,
        @Nullable final GenieNetworkConfiguration genieNetworkConfiguration
    ) throws GenieClientException {
        this(
            GenieClientUtils.createRetrofitInstance(url, interceptors, genieNetworkConfiguration),
            genieNetworkConfiguration == null
                ? GenieNetworkConfiguration.DEFAULT_NUM_RETRIES
                : genieNetworkConfiguration.getMaxStatusRetries()
        );
    }

    /**
     * Submit a job to genie using the jobRequest provided.
     *
     * @param jobRequest A job request containing all the details for running a job.
     * @return jobId The id of the job submitted.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public String submitJob(
        final JobRequest jobRequest
    ) throws IOException, GenieClientException {
        if (jobRequest == null) {
            throw new IllegalArgumentException("Job Request cannot be null.");
        }
        final String locationHeader = this.jobService
            .submitJob(jobRequest)
            .execute()
            .headers()
            .get(GenieClientUtils.LOCATION_HEADER);

        if (StringUtils.isBlank(locationHeader)) {
            throw new GenieClientException("No location header. Unable to get ID");
        }
        return GenieClientUtils.getIdFromLocation(locationHeader);
    }

    /**
     * Submit a job to genie using the jobRequest and attachments provided.
     *
     * @param jobRequest  A job request containing all the details for running a job.
     * @param attachments A map of filenames/input-streams needed to be sent to the server as attachments.
     * @return jobId The id of the job submitted.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public String submitJobWithAttachments(
        final JobRequest jobRequest,
        final Map<String, InputStream> attachments
    ) throws IOException, GenieClientException {
        if (jobRequest == null) {
            throw new IllegalArgumentException("Job Request cannot be null.");
        }

        final MediaType attachmentMediaType = MediaType.parse(APPLICATION_OCTET_STREAM);
        final ArrayList<MultipartBody.Part> attachmentFiles = new ArrayList<>();

        for (Map.Entry<String, InputStream> entry : attachments.entrySet()) {

            // create a request body from the input stream provided
            final RequestBody requestBody = new RequestBody() {
                @Override
                public MediaType contentType() {
                    return attachmentMediaType;
                }

                @Override
                public void writeTo(final BufferedSink sink) throws IOException {
                    ByteStreams.copy(entry.getValue(), sink.outputStream());
                }
            };

            final MultipartBody.Part part = MultipartBody.Part.createFormData(
                ATTACHMENT,
                entry.getKey(),
                requestBody);

            attachmentFiles.add(part);
        }
        final String locationHeader = this.jobService
            .submitJobWithAttachments(jobRequest, attachmentFiles)
            .execute()
            .headers()
            .get(GenieClientUtils.LOCATION_HEADER);

        if (StringUtils.isBlank(locationHeader)) {
            throw new GenieClientException("No location header. Unable to get ID");
        }

        return GenieClientUtils.getIdFromLocation(locationHeader);
    }

    /**
     * Method to get a list of all the jobs.
     *
     * @return A list of jobs.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public List<JobSearchResult> getJobs() throws IOException, GenieClientException {
        return this.getJobs(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    /**
     * Method to get a list of all the jobs from Genie for the query parameters specified.
     * <p>
     * Deprecated: For new search fields
     *
     * @param id          id for job
     * @param name        name of job (can be a SQL-style pattern such as HIVE%)
     * @param user        user who submitted job
     * @param statuses    statuses of jobs to find
     * @param tags        tags for the job
     * @param clusterName the name of the cluster
     * @param clusterId   the id of the cluster
     * @param commandName the name of the command run by the job
     * @param commandId   the id of the command run by the job
     * @param minStarted  The time which the job had to start after in order to be return (inclusive)
     * @param maxStarted  The time which the job had to start before in order to be returned (exclusive)
     * @param minFinished The time which the job had to finish after in order to be return (inclusive)
     * @param maxFinished The time which the job had to finish before in order to be returned (exclusive)
     * @return A list of jobs.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     * @see #getJobs(
     *String,
     * String,
     * String,
     * Set,
     * Set,
     * String,
     * String,
     * String,
     * String,
     * Long,
     * Long,
     * Long,
     * Long,
     * String,
     * String
     *)
     */
    @Deprecated
    public List<JobSearchResult> getJobs(
        final String id,
        final String name,
        final String user,
        final Set<String> statuses,
        final Set<String> tags,
        final String clusterName,
        final String clusterId,
        final String commandName,
        final String commandId,
        final Long minStarted,
        final Long maxStarted,
        final Long minFinished,
        final Long maxFinished
    ) throws IOException, GenieClientException {
        return this.getJobs(
            id,
            name,
            user,
            statuses,
            tags,
            clusterName,
            clusterId,
            commandName,
            commandId,
            minStarted,
            maxStarted,
            minFinished,
            maxFinished,
            null,
            null
        );
    }

    /**
     * Method to get a list of all the jobs from Genie for the query parameters specified.
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
     * @return A list of jobs.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     * @deprecated Use {@link #getJobs(String, String, String, Set, Set, String, String, String, String, Long, Long,
     * Long, Long, String, String, Integer, SortAttribute, SortDirection, Integer)}
     */
    public List<JobSearchResult> getJobs(
        @Nullable final String id,
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<String> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String clusterName,
        @Nullable final String clusterId,
        @Nullable final String commandName,
        @Nullable final String commandId,
        @Nullable final Long minStarted,
        @Nullable final Long maxStarted,
        @Nullable final Long minFinished,
        @Nullable final Long maxFinished,
        @Nullable final String grouping,
        @Nullable final String groupingInstance
    ) throws IOException, GenieClientException {
        return this.getJobs(
            id,
            name,
            user,
            statuses,
            tags,
            clusterName,
            clusterId,
            commandName,
            commandId,
            minStarted,
            maxStarted,
            minFinished,
            maxFinished,
            grouping,
            groupingInstance,
            null,
            null,
            null,
            null
        );
    }


    /**
     * Method to get a list of all the jobs from Genie for the query parameters specified.
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
     * @param pageSize         The maximum number of results returned
     * @param sortAttribute    The entity attribute used to sort
     * @param sortDirection    The sort direction
     * @param pageIndex        The page index
     * @return A list of jobs.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public List<JobSearchResult> getJobs(
        @Nullable final String id,
        @Nullable final String name,
        @Nullable final String user,
        @Nullable final Set<String> statuses,
        @Nullable final Set<String> tags,
        @Nullable final String clusterName,
        @Nullable final String clusterId,
        @Nullable final String commandName,
        @Nullable final String commandId,
        @Nullable final Long minStarted,
        @Nullable final Long maxStarted,
        @Nullable final Long minFinished,
        @Nullable final Long maxFinished,
        @Nullable final String grouping,
        @Nullable final String groupingInstance,
        @Nullable @Min(1) final Integer pageSize,
        @Nullable final SortAttribute sortAttribute,
        @Nullable final SortDirection sortDirection,
        @Nullable @Min(0) final Integer pageIndex
    ) throws IOException, GenieClientException {
        return GenieClientUtils.parseSearchResultsResponse(
            this.jobService.getJobs(
                id,
                name,
                user,
                statuses,
                tags,
                clusterName,
                clusterId,
                commandName,
                commandId,
                minStarted,
                maxStarted,
                minFinished,
                maxFinished,
                grouping,
                groupingInstance,
                pageSize,
                GenieClientUtils.getSortParameter(sortAttribute, sortDirection),
                pageIndex
            ).execute(),
            "jobSearchResultList",
            JobSearchResult.class
        );
    }

    /**
     * Method to get a job from Genie.
     *
     * @param jobId The id of the job to get.
     * @return The job details.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public Job getJob(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return jobService.getJob(jobId).execute().body();
    }

    /**
     * Method to get the cluster on which the job executes.
     *
     * @param jobId The id of the job.
     * @return The cluster object.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public Cluster getJobCluster(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return jobService.getJobCluster(jobId).execute().body();
    }

    /**
     * Method to get the command on which the job executes.
     *
     * @param jobId The id of the job.
     * @return The command object.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public Command getJobCommand(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return jobService.getJobCommand(jobId).execute().body();
    }

    /**
     * Method to get the Job Request for the job.
     *
     * @param jobId The id of the job.
     * @return The job requests object.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public JobRequest getJobRequest(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return jobService.getJobRequest(jobId).execute().body();
    }

    /**
     * Method to get the Job Execution information for the job.
     *
     * @param jobId The id of the job.
     * @return The job execution object.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public JobExecution getJobExecution(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return jobService.getJobExecution(jobId).execute().body();
    }

    /**
     * Method to get the metadata information for the job.
     *
     * @param jobId The id of the job.
     * @return The metadata object.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public JobMetadata getJobMetadata(final String jobId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return this.jobService.getJobMetadata(jobId).execute().body();
    }

    /**
     * Method to get the Applications for the job.
     *
     * @param jobId The id of the job.
     * @return The list of Applications.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public List<Application> getJobApplications(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        return jobService.getJobApplications(jobId).execute().body();
    }

    /**
     * Method to fetch the stdout of a job from Genie.
     *
     * @param jobId The id of the job whose output is desired.
     * @return An input stream to the output contents.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public InputStream getJobStdout(final String jobId) throws IOException, GenieClientException {
        return this.getJobStdout(jobId, null, null);
    }

    /**
     * Method to fetch the stdout of a job from Genie.
     *
     * <p>
     * Range Logic:
     * <p>
     * {@literal rangeStart} but no {@literal rangeEnd} then go from the start byte to the end of available content
     * <p>
     * {@literal rangeStart} and {@literal rangeEnd} return that range of bytes from the file if they exist
     * <p>
     * If only {@literal rangeEnd} then return the last number of those bytes from the file if they exist
     *
     * @param jobId      The id of the job whose output is desired.
     * @param rangeStart The start byte of the file to retrieve. Optional. Greater than or equal to 0.
     * @param rangeEnd   The end byte of the file to retrieve. Optional. Greater than or equal to 0. Must be
     *                   greater than {@literal rangeStart}.
     * @return An input stream to the output contents.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public InputStream getJobStdout(
        final String jobId,
        @Nullable final Long rangeStart,
        @Nullable final Long rangeEnd
    ) throws IOException, GenieClientException {
        return this.getJobOutputFile(jobId, STDOUT, rangeStart, rangeEnd);
    }

    /**
     * Method to fetch the stderr of a job from Genie.
     *
     * @param jobId The id of the job whose stderr is desired.
     * @return An input stream to the stderr contents.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public InputStream getJobStderr(final String jobId) throws IOException, GenieClientException {
        return this.getJobStderr(jobId, null, null);
    }

    /**
     * Method to fetch the stderr of a job from Genie.
     *
     * <p>
     * Range Logic:
     * <p>
     * {@literal rangeStart} but no {@literal rangeEnd} then go from the start byte to the end of available content
     * <p>
     * {@literal rangeStart} and {@literal rangeEnd} return that range of bytes from the file if they exist
     * <p>
     * If only {@literal rangeEnd} then return the last number of those bytes from the file if they exist
     *
     * @param jobId      The id of the job whose stderr is desired.
     * @param rangeStart The start byte of the file to retrieve. Optional. Greater than or equal to 0.
     * @param rangeEnd   The end byte of the file to retrieve. Optional. Greater than or equal to 0. Must be
     *                   greater than {@literal rangeStart}.
     * @return An input stream to the stderr contents.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public InputStream getJobStderr(
        final String jobId,
        @Nullable final Long rangeStart,
        @Nullable final Long rangeEnd
    ) throws IOException, GenieClientException {
        return this.getJobOutputFile(jobId, STDERR, rangeStart, rangeEnd);
    }

    /**
     * Method to fetch an output file for a job from Genie.
     *
     * <p>
     * <b>NOTE</b>: If the specified outputFilePath is a directory, then the directory
     * manifest is returned.
     * </p>
     *
     * @param jobId          The id of the job whose output file is desired.
     * @param outputFilePath The path to the file within output directory.
     * @return An input stream to the output file contents.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public InputStream getJobOutputFile(
        final String jobId,
        final String outputFilePath
    ) throws IOException, GenieClientException {
        return this.getJobOutputFile(jobId, outputFilePath, null, null);
    }

    /**
     * Method to fetch an output file for a job from Genie and accepting an range of bytes to return.
     *
     * <p>
     * <b>NOTE</b>: If the specified outputFilePath is a directory, then the directory
     * manifest is returned.
     * </p>
     *
     * <p>
     * Range Logic:
     * <p>
     * {@literal rangeStart} but no {@literal rangeEnd} then go from the start byte to the end of available content
     * <p>
     * {@literal rangeStart} and {@literal rangeEnd} return that range of bytes from the file if they exist
     * <p>
     * If only {@literal rangeEnd} then return the last number of those bytes from the file if they exist
     *
     * @param jobId          The id of the job whose output file is desired.
     * @param outputFilePath The path to the file within output directory.
     * @param rangeStart     The start byte of the file to retrieve. Optional. Greater than or equal to 0.
     * @param rangeEnd       The end byte of the file to retrieve. Optional. Greater than or equal to 0. Must be
     *                       greater than {@literal rangeStart}.
     * @return An input stream to the output file contents.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     * @see <a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range">Range Header Documentation</a>
     */
    public InputStream getJobOutputFile(
        final String jobId,
        final String outputFilePath,
        @Nullable final Long rangeStart,
        @Nullable final Long rangeEnd
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        if (rangeStart != null && rangeStart < ZERO) {
            throw new IllegalArgumentException("Range start must be greater than or equal to 0");
        }
        if (rangeEnd != null && rangeEnd < ZERO) {
            throw new IllegalArgumentException("Range end must be greater than or equal to 0");
        }

        String rangeHeader = null;
        if (rangeStart != null || rangeEnd != null) {
            if (rangeStart != null && rangeEnd != null) {
                if (rangeEnd < rangeStart) {
                    throw new IllegalArgumentException("Range end must be greater than range start");
                }
                rangeHeader = "bytes=" + rangeStart + "-" + rangeEnd;
            } else if (rangeStart != null) {
                rangeHeader = "bytes=" + rangeStart + "-";
            } else {
                rangeHeader = "bytes=-" + rangeEnd;
            }
        }
        final String pathArg = StringUtils.isEmpty(outputFilePath) ? EMPTY_STRING : outputFilePath;
        final ResponseBody body = rangeHeader == null
            ? this.jobService.getJobOutputFile(jobId, pathArg).execute().body()
            : this.jobService.getJobOutputFile(jobId, pathArg, rangeHeader).execute().body();
        if (body == null) {
            throw new GenieClientException(String.format("No data for %s returned", outputFilePath));
        }
        return body.byteStream();
    }

    /**
     * Method to fetch the status of a job.
     *
     * @param jobId The id of the job.
     * @return The status of the Job.
     * @throws GenieClientException If the response recieved is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public JobStatus getJobStatus(
        final String jobId
    ) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        final JsonNode jsonNode = this.jobService.getJobStatus(jobId).execute().body();
        if (jsonNode == null || jsonNode.getNodeType() != JsonNodeType.OBJECT) {
            throw new GenieClientException("Unknown response from server: " + jsonNode);
        }
        try {
            final JsonNode statusNode = jsonNode.get(STATUS);
            if (statusNode == null || statusNode.getNodeType() != JsonNodeType.STRING) {
                throw new GenieClientException("Unknown response format for status: " + statusNode);
            }
            return JobStatus.parse(statusNode.asText());
        } catch (GeniePreconditionException ge) {
            throw new GenieClientException(ge.getMessage());
        }
    }

    /**
     * Method to send a kill job request to Genie.
     *
     * @param jobId The id of the job.
     * @throws GenieClientException If the response received is not 2xx.
     * @throws IOException          For Network and other IO issues.
     */
    public void killJob(final String jobId) throws IOException, GenieClientException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        jobService.killJob(jobId).execute();
    }

    /**
     * Wait for job to complete, until the given timeout.
     *
     * @param jobId        the Genie job ID to wait for completion
     * @param blockTimeout the time to block for (in ms), after which a
     *                     GenieClientException will be thrown
     * @param pollTime     the time to sleep between polling for job status
     * @return The job status for the job after completion
     * @throws InterruptedException  on thread errors.
     * @throws GenieClientException  If the response received is not 2xx.
     * @throws IOException           For Network and other IO issues.
     * @throws GenieTimeoutException If the job times out.
     */
    public JobStatus waitForCompletion(final String jobId, final long blockTimeout, final long pollTime)
        throws GenieClientException, InterruptedException, IOException, GenieTimeoutException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }

        final long startTime = System.currentTimeMillis();
        int errorCount = 0;

        // wait for job to finish
        while (true) {
            try {
                final JobStatus status = this.getJobStatus(jobId);

                if (status.isFinished()) {
                    return status;
                }

                // reset the error count
                errorCount = 0;
            } catch (final IOException ioe) {
                errorCount++;
                // Ignore for 5 times in a row
                if (errorCount >= this.maxStatusRetries) {
                    throw ioe;
                }
            }

            if (System.currentTimeMillis() - startTime < blockTimeout) {
                Thread.sleep(pollTime);
            } else {
                throw new GenieTimeoutException("Timed out waiting for job to finish: " + jobId);
            }
        }
    }

    /**
     * Wait for job to complete, until the given timeout.
     *
     * @param jobId        the Genie job ID to wait for completion.
     * @param blockTimeout the time to block for (in ms), after which a
     *                     GenieClientException will be thrown.
     * @return The job status for the job after completion.
     * @throws InterruptedException  on thread errors.
     * @throws GenieClientException  If the response received is not 2xx.
     * @throws IOException           For Network and other IO issues.
     * @throws GenieTimeoutException If the job times out.
     */
    public JobStatus waitForCompletion(final String jobId, final long blockTimeout)
        throws GenieClientException, InterruptedException, IOException, GenieTimeoutException {
        if (StringUtils.isEmpty(jobId)) {
            throw new IllegalArgumentException("Missing required parameter: jobId.");
        }
        final long pollTime = 10000L;
        return waitForCompletion(jobId, blockTimeout, pollTime);
    }
}
