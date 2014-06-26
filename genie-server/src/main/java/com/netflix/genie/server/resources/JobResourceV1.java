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
import com.netflix.genie.server.services.ExecutionServiceFactory;
import java.net.HttpURLConnection;
import java.util.List;
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
@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
public class JobResourceV1 {

    private final ExecutionService xs;
    private static final Logger LOG = LoggerFactory.getLogger(JobResourceV1.class);

    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     */
    public JobResourceV1() throws CloudServiceException {
        xs = ExecutionServiceFactory.getExecutionServiceImpl();
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
    @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Job submitJob(
            final Job job,
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

        return this.xs.submitJob(job);
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
    public Job getJobInfo(
            @PathParam("id") final String id) throws CloudServiceException {
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
    @Produces(MediaType.APPLICATION_JSON)
    public JobStatus getJobStatus(
            @PathParam("id") final String id) throws CloudServiceException {
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
    public List<Job> getJobs(
            @QueryParam("id") final String id,
            @QueryParam("jobName") final String jobName,
            @QueryParam("userName") final String userName,
            @QueryParam("status") final String status,
            @QueryParam("clusterName") final String clusterName,
            @QueryParam("clusterId") final String clusterId,
            @QueryParam("page") @DefaultValue("0") int page,
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
    public Job killJob(@PathParam("id") final String id) throws CloudServiceException {
        LOG.debug("called for jobID: " + id);
        return this.xs.killJob(id);
    }
}
