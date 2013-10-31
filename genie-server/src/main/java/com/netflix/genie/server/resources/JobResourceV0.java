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

package com.netflix.genie.server.resources;

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
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.netflix.genie.common.messages.JobInfoRequest;
import com.netflix.genie.common.messages.JobInfoResponse;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.server.services.ExecutionService;
import com.netflix.genie.server.services.ExecutionServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * Job REST Resource. This resource is responsible for launching jobs and
 * providing information about their state.
 * <p>
 * <b>NOTE</b>: Version 0
 *
 * @author skrishnan
 * @author bmundlapudi
 */
@Path("/v0/jobs")
@Produces({ "application/xml", "application/json" })
public class JobResourceV0 {

    private ExecutionService xs;
    private static Logger logger = LoggerFactory.getLogger(JobResourceV0.class);

    /**
     * Custom JAXB context resolver based for the job request/responses.
     *
     * @author skrishnan
     */
    @Provider
    public static class JobJAXBContextResolver extends JAXBContextResolver {
        /**
         * Constructor - initialize the resolver for the types that
         * this resource cares about.
         *
         * @throws Exception if there is any error in initialization
         */
        public JobJAXBContextResolver() throws Exception {
            super(new Class[]{JobInfoRequest.class,
                    JobStatusResponse.class,
                    JobInfoElement.class,
                    JobInfoResponse.class});
        }
    }

    /**
     * Default constructor.
     *
     * @throws CloudServiceException
     */
    public JobResourceV0() throws CloudServiceException {
        xs = ExecutionServiceFactory.getExecutionServiceImpl();
    }

    /**
     * Submit a new job.
     *
     * @param request
     *            request object containing job info element for new job
     * @param hsr
     *            servlet context
     * @return successful response, or one with HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({ "application/xml", "application/json" })
    public Response submitJob(JobInfoRequest request,
            @Context HttpServletRequest hsr) {
        // get client's host from the context
        String clientHost = hsr.getHeader("X-Forwarded-For");
        if (clientHost != null) {
            clientHost = clientHost.split(",")[0];
        } else {
            clientHost = hsr.getRemoteAddr();
        }
        logger.info("called from: " + clientHost);

        // set the clientHost, if it is not overridden already
        JobInfoElement jobInfo = request.getJobInfo();
        if ((jobInfo != null) &&
                ((jobInfo.getClientHost() == null) ||
                        jobInfo.getClientHost().isEmpty())) {
            jobInfo.setClientHost(clientHost);
        }

        JobInfoResponse response = xs.submitJob(request);
        return ResponseUtil.createResponse(response);
    }

    /**
     * Get job information for given job id.
     *
     * @param jobID
     *            id for job to look up
     * @return successful response, or one with HTTP error code
     */
    @GET
    @Path("/{jobID}")
    public Response getJobInfo(@PathParam("jobID") String jobID) {
        logger.info("called for jobID: " + jobID);
        JobInfoResponse response = xs.getJobInfo(jobID);
        return ResponseUtil.createResponse(response);
    }

    /**
     * Get job status for give job id.
     *
     * @param jobID
     *            id for job to look up
     * @return successful response, or one with HTTP error code
     */
    @GET
    @Path("/{jobID}/status")
    public Response getJobStatus(@PathParam("jobID") String jobID) {
        logger.info("called for jobID" + jobID);
        JobStatusResponse response = xs.getJobStatus(jobID);
        return ResponseUtil.createResponse(response);
    }

    /**
     * Get job info for given filter criteria.
     *
     * @param jobID
     *            id for job
     * @param jobName
     *            name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName
     *            user who submitted job
     * @param jobType
     *            type of job - possible types Type.JobType
     * @param status
     *            status of job - possible types Type.JobStatus
     * @param limit
     *            max number of jobs to return
     * @param page
     *            page number for job
     * @return successful response, or one with HTTP error code
     */
    @GET
    @Path("/")
    public Response getJobs(@QueryParam("jobID") String jobID,
            @QueryParam("jobName") String jobName,
            @QueryParam("userName") String userName,
            @QueryParam("jobType") String jobType,
            @QueryParam("status") String status,
            @QueryParam("clusterName") String clusterName,
            @QueryParam("clusterId") String clusterId,
            @QueryParam("limit") @DefaultValue("1024") int limit,
            @QueryParam("page") @DefaultValue("0") int page) {
        logger.info("called");
        JobInfoResponse response = null;
        response = xs.getJobs(jobID, jobName, userName, jobType, status, clusterName, clusterId,
                limit, page);
        return ResponseUtil.createResponse(response);
    }

    /**
     * Kill job based on given job ID.
     *
     * @param jobID
     *            id for job to kill
     * @return successful response, or one with HTTP error code
     */
    @DELETE
    @Path("/{jobID}")
    public Response killJob(@PathParam("jobID") String jobID) {
        logger.info("called for jobID: " + jobID);
        JobStatusResponse response = xs.killJob(jobID);
        return ResponseUtil.createResponse(response);
    }
}
