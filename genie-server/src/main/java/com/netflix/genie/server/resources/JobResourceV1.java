/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.server.resources;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.model.Job;
import com.netflix.genie.common.model.Types.JobStatus;
import com.netflix.genie.server.services.ExecutionService;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;
import com.wordnik.swagger.annotations.ApiResponse;
import com.wordnik.swagger.annotations.ApiResponses;
import java.net.HttpURLConnection;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resource class for executing and monitoring jobs via Genie.
 *
 * @author amsharma
 * @author tgianos
 */
@Path("/v1/jobs")
@Produces({
    MediaType.APPLICATION_XML,
    MediaType.APPLICATION_JSON
})
@Named
public class JobResourceV1 {

    private static final Logger LOG = LoggerFactory.getLogger(JobResourceV1.class);

    /**
     * The execution service.
     */
    private final ExecutionService xs;

    /**
     * Uri info for gathering information on the request.
     */
    @Context
    private UriInfo uriInfo;
    
    /**
     * Constructor.
     * 
     * @param xs The execution service to use.
     */
    @Inject
    public JobResourceV1(final ExecutionService xs) {
        this.xs = xs;
    }

    /**
     * Submit a new job.
     *
     * @param job request object containing job info element for new job
     * @param hsr servlet context
     * @return The submitted job
     * @throws CloudServiceException
     */
    @POST
    @Consumes({
        MediaType.APPLICATION_XML,
        MediaType.APPLICATION_JSON
    })
    @ApiOperation(
            value = "Submit a job",
            notes = "Submit a new job to run to genie",
            response = Job.class)
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Job.class)
    })
    public Response submitJob(
            @ApiParam(value = "Job object to run.", required = true)
            final Job job,
            @ApiParam(value = "Http Servlet request object", required = true)
            @Context final HttpServletRequest hsr) throws CloudServiceException {
        // get client's host from the context
        String clientHost = hsr.getHeader("X-Forwarded-For");
        if (clientHost != null) {
            clientHost = clientHost.split(",")[0];
        } else {
            clientHost = hsr.getRemoteAddr();
        }
        LOG.debug("called from: " + clientHost);
        if (job == null) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No job entered. Unable to submit.");
        }

        // set the clientHost, if it is not overridden already
        if (StringUtils.isEmpty(clientHost)) {
            job.setClientHost(clientHost);
        }

        final Job createdJob = this.xs.submitJob(job);
        return Response.created(
                this.uriInfo.getAbsolutePathBuilder().path(createdJob.getId()).build()).
                entity(createdJob).
                build();
    }

    /**
     * Get job information for given job id.
     *
     * @param id id for job to look up
     * @return the Job
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}")
    @ApiOperation(
            value = "Find a job by id",
            notes = "Get the job by id if it exists",
            response = Job.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Job.class),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Job not found")
    })
    public Job getJob(
            @ApiParam(value = "Id of the job to get.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("called for jobID: " + id);
        return this.xs.getJobInfo(id);
    }

    /**
     * Get job status for give job id.
     *
     * @param id id for job to look up
     * @return The status of the job
     * @throws CloudServiceException
     */
    @GET
    @Path("/{id}/status")
    @ApiOperation(
            value = "Get the status of the job ",
            notes = "Get the status of job whose id is sent",
            response = Job.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Job.class),
        @ApiResponse(code = 400, message = "Invalid id supplied"),
        @ApiResponse(code = 404, message = "Job not found")
    })
    @Produces(MediaType.APPLICATION_JSON)
    public JobStatus getJobStatus(
            @ApiParam(value = "Id of the job.", required = true)
            @PathParam("id")
            final String id) throws CloudServiceException {
        LOG.debug("called for jobID" + id);
        return this.xs.getJobStatus(id);
    }

    /**
     * Get jobs for given filter criteria.
     *
     * @param id id for job
     * @param jobName name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName user who submitted job
     * @param status status of job - possible types Type.JobStatus
     * @param clusterName the name of the cluster
     * @param clusterId the id of the cluster
     * @param page page number for job
     * @param limit max number of jobs to return
     * @return successful response, or one with HTTP error code
     * @throws CloudServiceException
     */
    @GET
    @ApiOperation(
            value = "Find jobs",
            notes = "Find jobs by the submitted criteria.",
            response = Job.class,
            responseContainer = "List")
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Job.class)
    })
    public List<Job> getJobs(
            @ApiParam(value = "Id of the job.", required = false)
            @QueryParam("id")
            final String id,
            @ApiParam(value = "Name of the job.", required = false)
            @QueryParam("jobName")
            final String jobName,
            @ApiParam(value = "Name of the user who submitted the job.", required = false)
            @QueryParam("userName")
            final String userName,
            @ApiParam(value = "Status of the jobs to fetch.", required = false)
            @QueryParam("status")
            final String status,
            @ApiParam(value = "Name of the cluster on which the job ran.", required = false)
            @QueryParam("clusterName")
            final String clusterName,
            @ApiParam(value = "Id of the cluster on which the job ran.", required = false)
            @QueryParam("clusterId")
            final String clusterId,
            @ApiParam(value = "The page to start on.", required = false)
            @QueryParam("page") @DefaultValue("0") int page,
            @ApiParam(value = "Max number of results per page.", required = false)
            @QueryParam("limit") @DefaultValue("1024") int limit)
            throws CloudServiceException {
        LOG.debug("Called");
        return this.xs.getJobs(
                id,
                jobName,
                userName,
                JobStatus.parse(status),
                clusterName,
                clusterId,
                limit,
                page);
    }

    /**
     * Kill job based on given job ID.
     *
     * @param id id for job to kill
     * @return The job that was killed
     * @throws CloudServiceException
     */
    @DELETE
    @Path("/{id}")
    @ApiOperation(
            value = "Delete a job",
            notes = "Delete the jobs by the id specified.",
            response = Job.class)
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "OK", response = Job.class)
    })
    public Job killJob(
            @PathParam("id")
            @ApiParam(value = "Id of the job.", required = true)
            final String id) throws CloudServiceException {
        LOG.debug("called for jobID: " + id);
        return this.xs.killJob(id);
    }
}
