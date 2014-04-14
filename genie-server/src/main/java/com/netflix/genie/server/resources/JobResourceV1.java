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
import com.netflix.genie.common.messages.JobRequest;
import com.netflix.genie.common.messages.JobResponse;
import com.netflix.genie.common.messages.JobStatusResponse;
import com.netflix.genie.common.model.ClusterCriteria;
import com.netflix.genie.common.model.JobElement;
import com.netflix.genie.common.model.JobInfoElement;
import com.netflix.genie.server.persistence.PersistenceManager;
import com.netflix.genie.server.persistence.QueryBuilder;
import com.netflix.genie.server.services.ExecutionService;
import com.netflix.genie.server.services.ExecutionServiceFactory;
import com.netflix.genie.server.util.JAXBContextResolver;
import com.netflix.genie.server.util.ResponseUtil;

/**
 * @author amsharma
 */
@Path("/v1/jobs")
@Produces({ "application/xml", "application/json" })
public class JobResourceV1 {

    private ExecutionService xs;
    private static Logger logger = LoggerFactory.getLogger(JobResourceV1.class);

    /**
     * Custom JAXB context resolver based for the job request/responses.
     *
     * @author amsharma
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
            super(new Class[]{JobRequest.class,
                    JobStatusResponse.class,
                    JobElement.class,
                    JobResponse.class,
                    ClusterCriteria.class});
        }
    }
    
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
     * @param request
     *            request object containing job info element for new job
     * @param hsr
     *            servlet context
     * @return successful response, or one with HTTP error code
     */
    @POST
    @Path("/")
    @Consumes({ "application/xml", "application/json" })
    public Response submitJob(JobRequest request,
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
        JobElement jobInfo = request.getJobInfo();
        if ((jobInfo != null)
                && ((jobInfo.getClientHost() == null)
                        || jobInfo.getClientHost().isEmpty())) {
            jobInfo.setClientHost(clientHost);
            // TODO: Has to be a better way of doing it
          //  jobInfo.setClusterCriteriaString(jobInfo.getClusterCriteriaList());
        }
        
        if (jobInfo != null) {
            logger.debug("Genie V1 job request submitted to execution service. Trying to persist it.Object Details are");
            logger.debug("Job Id: " + jobInfo.getJobID() + " Job Name: " + jobInfo.getJobName());
            logger.debug("Cluster Criteria List: " + jobInfo.getClusterCriteriaList());
            PersistenceManager<JobElement> pm = new PersistenceManager<JobElement>();
            pm.createEntity(jobInfo);
          
            logger.debug("Persistence Successful. Return Response");
            //JobResponse response = xs.submitJob(request);
        } else {
            logger.debug ("Job Info is Null. Inspecting request object");
            logger.debug(request.toString());
        }
        
        JobResponse response  = new JobResponse();
        response.setJob(jobInfo);
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
        JobResponse response = xs.getJobInfo(jobID);
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
        
        JobResponse response = null;
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