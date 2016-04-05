package com.netflix.genie.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import retrofit2.Call;
import retrofit2.Response;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Client library for the Job Service.
 *
 * @author amsharma
 */
public class JobClient extends BaseGenieClient {

    /**
     * Constructor.
     *
     * @param genieServiceURI The base URL for the Genie service of the type: http://localhost:8080
     */
    public JobClient(
        final String genieServiceURI
    ) {
        super(genieServiceURI);
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
//        final ResponseEntity<String> response =
//            restTemplate.postForEntity(serviceBaseURL, jobRequest, String.class);

//        final URI jobLocation = restTemplate.postForLocation(serviceBaseURL, jobRequest);
//
//        if (jobLocation != null) {
//            return getIdFromLocation(jobLocation.toString());
//        } else {
//            throw new GenieServerException("Failed to submit job successfully.");
//        }
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
                        //.withFinished(dateFormatter.parse(job.get("finished")))
                        .build());
            }

            return jobList;

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
