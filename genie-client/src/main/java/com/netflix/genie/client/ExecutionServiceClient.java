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

package com.netflix.genie.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.JobRequest;
import com.netflix.genie.common.messages.JobResponse;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.common.model.Types;

import com.netflix.client.http.HttpRequest.Verb;

import com.google.common.collect.Multimap;

/**
 * Singleton class, which acts as the client library for the Genie Execution
 * Service.
 *
 * @author skrishnan
 *
 */
public final class ExecutionServiceClient extends BaseGenieClient {

    private static final Logger LOG = LoggerFactory
            .getLogger(ExecutionServiceClient.class);

    private static final String BASE_EXECUTION_REST_URI = BASE_REST_URI + "jobs";

    // reference to the instance object
    private static ExecutionServiceClient instance;

    /**
     * Private constructor for singleton class.
     *
     * @throws IOException if there is any error during initialization
     */
    private ExecutionServiceClient() throws IOException {
        super();
    }

    /**
     * Returns the singleton instance that can be used by clients.
     *
     * @return ExecutionServiceClient instance
     * @throws IOException if there is an error instantiating client
     */
    public static synchronized ExecutionServiceClient getInstance() throws IOException {
        if (instance == null) {
            instance = new ExecutionServiceClient();
        }

        return instance;
    }

    /**
     * Submits a job using given parameters.
     *
     * @param jobInfo
     *            for submitting job (can't be null)<br>
     *            jobInfo.userName not null - required<br>
     *            jobInfo.userAgent optional, e.g. UC4, search, etc (however
     *            client wishes to categorize themselves)<br>
     *            jobInfo.jobName optional - job name to be used<br>
     *            jobInfo.jobID optional - will be auto-generated if it is null<br>
     *            jobInfo.jobType required - HADOOP, HIVE or PIG<br>
     *            jobInfo.configuration required for HIVE, PIG - PROD or TEST<br>
     *            jobInfo.schedule required - ADHOC or SCHEDULED<br>
     *            jobInfo.cmdArgs required - command-line arguments<br>
     *            jobInfo.fileDependencies optional - file dependencies in S3,
     *            represented as a CSV<br>
     *
     *            More details can be found on the Genie User Guide on GitHub.
     *
     * @return updated jobInfo for submitted job, if there is no error
     * @throws CloudServiceException
     */
    public JobElement submitJob(JobElement jobInfo)
            throws CloudServiceException {
        if (jobInfo == null) {
            String msg = "Required parameter jobInfo can't be NULL";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (jobInfo.getUserName() == null) {
            String msg = "User name is missing";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (jobInfo.getJobType() == null) {
            String msg = "JobType is missing";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
        if (!jobInfo.getJobType().equalsIgnoreCase(Types.JobType.YARN.name())) {
            String msg = "Configuration is missing";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
//        if (jobInfo.getSchedule() == null) {
//            String msg = "Schedule is missing";
//            LOG.error(msg);
//            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
//                    msg);
//        }
        if (jobInfo.getCmdArgs() == null) {
            String msg = "Command-line arguments are missing";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }
//        if (jobInfo.getUserAgent() == null) {
//            jobInfo.setUserAgent("java-client-library");
//        }
        JobRequest request = new JobRequest();
        request.setJobInfo(jobInfo);

        JobResponse ji = executeRequest(Verb.POST, BASE_EXECUTION_REST_URI,
                null, null, request, JobResponse.class);

        if ((ji.getJobs() == null) || (ji.getJobs().length == 0)) {
            String msg = "Unable to parse job info from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) jobInfo
        return ji.getJobs()[0];
    }

    /**
     * Gets job information for a given jobID.
     *
     * @param jobID
     *            the Genie jobID (can't be null)
     * @return the jobInfo for this jobID
     * @throws CloudServiceException
     */
    public JobElement getJob(String jobID) throws CloudServiceException {
        if (jobID == null) {
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    "Missing required parameter: jobID");
        }

        JobResponse ji = executeRequest(Verb.GET, BASE_EXECUTION_REST_URI,
                jobID, null, null, JobResponse.class);

        if ((ji.getJobs() == null) || (ji.getJobs().length == 0)) {
            String msg = "Unable to parse job info from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // return the first (only) jobInfo
        return ji.getJobs()[0];
    }

    /**
     * Gets a set of jobs for the given parameters.
     *
     * @param params
     *            key/value pairs in a map object.<br>
     *
     *            More details on the parameters can be found
     *            on the Genie User Guide on GitHub.
     * @return array of jobInfos that match the filter
     * @throws CloudServiceException
     */
    public JobElement[] getJobs(Multimap<String, String> params)
            throws CloudServiceException {
        JobResponse ji = executeRequest(Verb.GET, BASE_EXECUTION_REST_URI, null,
                params, null, JobResponse.class);

        // this will only happen if 200 is returned, and parsing fails for some
        // reason
        if ((ji.getJobs() == null) || (ji.getJobs().length == 0)) {
            String msg = "Unable to parse job info from response";
            LOG.error(msg);
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, msg);
        }

        // if we get here, there are non-zero jobInfos - return all
        return ji.getJobs();
    }

    /**
     * Wait for job to complete, until the given timeout.
     *
     * @param jobID
     *            the Genie job ID to wait for completion
     * @param blockTimeout
     *            the time to block for (in ms), after which a
     *            CloudServiceException will be thrown
     * @return the jobInfo for the job after completion
     * @throws CloudServiceException
     *             on service errors
     * @throws InterruptedException
     *             on timeout/thread errors
     */
    public JobElement waitForCompletion(String jobID, long blockTimeout)
            throws CloudServiceException, InterruptedException {
        long pollTime = 10000;
        return waitForCompletion(jobID, blockTimeout, pollTime);
    }

    /**
     * Wait for job to complete, until the given timeout.
     *
     * @param jobID
     *            the Genie job ID to wait for completion
     * @param blockTimeout
     *            the time to block for (in ms), after which a
     *            CloudServiceException will be thrown
     * @param pollTime
     *            the time to sleep between polling for job status
     * @return the jobInfo for the job after completion
     * @throws CloudServiceException
     *             on service errors
     * @throws InterruptedException
     *             on timeout/thread errors
     */
    public JobElement waitForCompletion(String jobID, long blockTimeout,
            long pollTime) throws CloudServiceException, InterruptedException {
        if (jobID == null) {
            String msg = "Missing required parameter: jobID";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        long startTime = System.currentTimeMillis();

        while (true) {
            JobElement jobInfo = getJob(jobID);

            // wait for job to finish - and finish time to be updated
            if (jobInfo.getFinishTime() > 0) {
                return jobInfo;
            }

            // block until timeout
            long currTime = System.currentTimeMillis();
            if (currTime - startTime < blockTimeout) {
                Thread.sleep(pollTime);
            } else {
                String msg = "Timed out waiting for job to finish";
                LOG.error(msg);
                throw new InterruptedException(msg);
            }
        }
    }

    /**
     * Kill a job using its jobID.
     *
     * @param jobID
     *            the Genie jobID for the job to kill
     * @return the final job status for this job
     * @throws CloudServiceException
     */
    public JobStatusResponse killJob(String jobID) throws CloudServiceException {
        if (jobID == null) {
            String msg = "Missing required parameter: jobID";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST,
                    msg);
        }

        // this assumes that the service will forward the delete to the right
        // instance
        JobStatusResponse js = executeRequest(Verb.DELETE, BASE_EXECUTION_REST_URI,
                jobID, null, null, JobStatusResponse.class);
        
        // return the response
        return js;
    }
}
