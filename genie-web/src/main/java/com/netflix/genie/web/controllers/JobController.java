/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.controllers;

import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.ExecutionService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.web.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.hateoas.resources.JobResource;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.Set;

/**
 * REST end-point for supporting jobs.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3/jobs")
@Api(value = "jobs", tags = "jobs", description = "Manage Genie Jobs.")
public final class JobController {

    private static final Logger LOG = LoggerFactory.getLogger(JobController.class);
//    private static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";

    private final ExecutionService executionService;
    private final JobSearchService jobSearchService;
    private final JobResourceAssembler jobResourceAssembler;

    /**
     * Constructor.
     *
     * @param executionService     The execution service to use.
     * @param jobSearchService     The job search service to use.
     * @param jobResourceAssembler Assemble job resources out of jobs
     */
    @Autowired
    public JobController(
            final ExecutionService executionService,
            final JobSearchService jobSearchService,
            final JobResourceAssembler jobResourceAssembler
    ) {
        this.executionService = executionService;
        this.jobSearchService = jobSearchService;
        this.jobResourceAssembler = jobResourceAssembler;
    }

    /**
     * Submit a new job.
     *
     * @param jobRequest The job request information
     * @return The submitted job
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    @ApiOperation(
            value = "Submit a job",
            notes = "Submit a new job to run to genie"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_ACCEPTED,
                    message = "Accepted for processing"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_BAD_REQUEST,
                    message = "Bad Request"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_CONFLICT,
                    message = "Job with ID already exists."
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Precondition Failed"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public ResponseEntity<Void> submitJob(
            @ApiParam(
                    value = "Job object to run.",
                    required = true
            )
            @RequestBody
            final JobRequest jobRequest    //,
//            @RequestHeader(FORWARDED_FOR_HEADER)
//            final String clientHost,
//            final HttpServletRequest httpServletRequest
    ) throws GenieException {
        if (jobRequest == null) {
            throw new GenieException(HttpURLConnection.HTTP_PRECON_FAILED, "No job entered. Unable to submit.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to submit job: " + jobRequest);
        }

        //TODO: Re-implement with new API type call that passes info along
//        // get client's host from the context
//        String localClientHost;
//        if (StringUtils.isNotBlank(clientHost)) {
//            localClientHost = clientHost.split(",")[0];
//        } else {
//            localClientHost = httpServletRequest.getRemoteAddr();
//        }
//
//        // set the clientHost, if it is not overridden already
//        if (StringUtils.isNotBlank(localClientHost)) {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("called from: " + localClientHost);
//            }
//            job.setClientHost(localClientHost);
//        }

        final String id = this.executionService.submitJob(jobRequest);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
                ServletUriComponentsBuilder
                        .fromCurrentRequest()
                        .path("/{id}")
                        .buildAndExpand(id)
                        .toUri()
        );
        return new ResponseEntity<>(httpHeaders, HttpStatus.ACCEPTED);
    }

    /**
     * Get job information for given job id.
     *
     * @param id id for job to look up
     * @return the Job
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ApiOperation(
            value = "Find a job by id",
            notes = "Get the job by id if it exists",
            response = Job.class
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_BAD_REQUEST,
                    message = "Bad Request"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Job not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid id supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public JobResource getJob(
            @ApiParam(
                    value = "Id of the job to get.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called for job with id: " + id);
        }
        return this.jobResourceAssembler.toResource(this.jobSearchService.getJob(id));
    }

//    /**
//     * Get job status for give job id.
//     *
//     * @param id id for job to look up
//     * @return The status of the job
//     * @throws GenieException For any error
//     */
//    @RequestMapping(value = "/{id}/status", method = RequestMethod.GET)
//    @ApiOperation(
//            value = "Get the status of the job ",
//            notes = "Get the status of job whose id is sent",
//            response = String.class
//    )
//    @ApiResponses(value = {
//            @ApiResponse(
//                    code = HttpURLConnection.HTTP_BAD_REQUEST,
//                    message = "Bad Request"
//            ),
//            @ApiResponse(
//                    code = HttpURLConnection.HTTP_NOT_FOUND,
//                    message = "Job not found"
//            ),
//            @ApiResponse(
//                    code = HttpURLConnection.HTTP_PRECON_FAILED,
//                    message = "Invalid id supplied"
//            ),
//            @ApiResponse(
//                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
//                    message = "Genie Server Error due to Unknown Exception"
//            )
//    })
//    public ObjectNode getJobStatus(
//            @ApiParam(
//                    value = "Id of the job.",
//                    required = true
//            )
//            @PathVariable("id")
//            final String id
//    ) throws GenieException {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Called for job id:" + id);
//        }
//        final ObjectMapper mapper = new ObjectMapper();
//        final ObjectNode node = mapper.createObjectNode();
//        node.put("status", this.jobService.getJobStatus(id).toString());
//        return node;
//    }

    /**
     * Get jobs for given filter criteria.
     *
     * @param id          id for job
     * @param name        name of job (can be a SQL-style pattern such as HIVE%)
     * @param userName    user who submitted job
     * @param statuses    statuses of jobs to find
     * @param tags        tags for the job
     * @param clusterName the name of the cluster
     * @param clusterId   the id of the cluster
     * @param commandName the name of the command run by the job
     * @param commandId   the id of the command run by the job
     * @param page        page information for job
     * @param assembler   The paged resources assembler to use
     * @return successful response, or one with HTTP error code
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.GET)
    @ApiOperation(
            value = "Find jobs",
            notes = "Find jobs by the submitted criteria.",
            response = Job.class,
            responseContainer = "List"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_BAD_REQUEST,
                    message = "Bad Request"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Job not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid id supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public PagedResources<JobResource> getJobs(
            @ApiParam(
                    value = "Id of the job."
            )
            @RequestParam(value = "id", required = false)
            final String id,
            @ApiParam(
                    value = "Name of the job."
            )
            @RequestParam(value = "name", required = false)
            final String name,
            @ApiParam(
                    value = "Name of the user who submitted the job."
            )
            @RequestParam(value = "userName", required = false)
            final String userName,
            @ApiParam(
                    value = "Statuses of the jobs to fetch.",
                    allowableValues = "INIT, RUNNING, SUCCEEDED, KILLED, FAILED"
            )
            @RequestParam(value = "status", required = false)
            final Set<String> statuses,
            @ApiParam(
                    value = "Tags for the job."
            )
            @RequestParam(value = "tag", required = false)
            final Set<String> tags,
            @ApiParam(
                    value = "Name of the cluster on which the job ran."
            )
            @RequestParam(value = "executionClusterName", required = false)
            final String clusterName,
            @ApiParam(
                    value = "Id of the cluster on which the job ran."
            )
            @RequestParam(value = "executionClusterId", required = false)
            final String clusterId,
            @ApiParam(
                    value = "The page to start on."
            )
            @RequestParam(value = "commandName", required = false)
            final String commandName,
            @ApiParam(
                    value = "Id of the cluster on which the job ran."
            )
            @RequestParam(value = "commandId", required = false)
            final String commandId,
            @PageableDefault(page = 0, size = 64, sort = {"updated"}, direction = Sort.Direction.DESC)
            final Pageable page,
            final PagedResourcesAssembler<Job> assembler
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Called with "
                            + "[id | jobName | userName | statuses | executionClusterName | executionClusterId | page]"
            );
            LOG.debug(
                    id
                            + " | "
                            + name
                            + " | "
                            + userName
                            + " | "
                            + statuses
                            + " | "
                            + tags
                            + " | "
                            + clusterName
                            + " | "
                            + clusterId
                            + " | "
                            + commandName
                            + " | "
                            + commandId
                            + " | "
                            + page
            );
        }
        Set<JobStatus> enumStatuses = null;
        if (statuses != null && !statuses.isEmpty()) {
            enumStatuses = EnumSet.noneOf(JobStatus.class);
            for (final String status : statuses) {
                if (StringUtils.isNotBlank(status)) {
                    enumStatuses.add(JobStatus.parse(status));
                }
            }
        }

        return assembler.toResource(
                this.jobSearchService.getJobs(
                        id,
                        name,
                        userName,
                        enumStatuses,
                        tags,
                        clusterName,
                        clusterId,
                        commandName,
                        commandId,
                        page
                ),
                this.jobResourceAssembler
        );
    }

    /**
     * Kill job based on given job ID.
     *
     * @param id id for job to kill
     * @throws GenieException For any error
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(
            value = "Kill a job",
            notes = "Kill the job with the id specified."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = HttpURLConnection.HTTP_NOT_FOUND,
                    message = "Job not found"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_PRECON_FAILED,
                    message = "Invalid id supplied"
            ),
            @ApiResponse(
                    code = HttpURLConnection.HTTP_INTERNAL_ERROR,
                    message = "Genie Server Error due to Unknown Exception"
            )
    })
    public void killJob(
            @ApiParam(
                    value = "Id of the job.",
                    required = true
            )
            @PathVariable("id")
            final String id
    ) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called for job id: " + id);
        }
        this.executionService.killJob(id);
    }
}
