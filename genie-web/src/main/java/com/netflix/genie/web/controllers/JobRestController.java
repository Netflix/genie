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
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.JobService;
import com.netflix.genie.web.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.hateoas.resources.JobResource;
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
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;

/**
 * REST end-point for supporting jobs.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
@RestController
@RequestMapping(value = "/api/v3/jobs")
public class JobRestController {

    private static final Logger LOG = LoggerFactory.getLogger(JobRestController.class);
//    private static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";

    private final JobService jobService;
    private final AttachmentService attachmentService;
    private final JobResourceAssembler jobResourceAssembler;

    /**
     * Constructor.
     *
     * @param jobService     The job search service to use.
     * @param attachmentService    The attachment service to use to save attachments.
     * @param jobResourceAssembler Assemble job resources out of jobs

     */
    @Autowired
    public JobRestController(
            final JobService jobService,
            final AttachmentService attachmentService,
            final JobResourceAssembler jobResourceAssembler
    ) {
        this.jobService = jobService;
        this.attachmentService = attachmentService;
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
    public ResponseEntity<Void> submitJob(
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

        //final String id = this.executionService.submitJob(jobRequest);
        final String id = "blah";
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
     * Submit a new job with attachments.
     *
     * @param jobRequest  The job request information
     * @param attachments The attachments for the job
     * @return The submitted job
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
            @RequestPart("request") final JobRequest jobRequest,
            @RequestPart("attachment") final MultipartFile[] attachments
    ) throws GenieException {
        if (jobRequest == null) {
            throw new GenieException(HttpURLConnection.HTTP_PRECON_FAILED, "No job entered. Unable to submit.");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called to submit job: " + jobRequest);
        }

        final String jobId = UUID.randomUUID().toString();
        if (attachments != null) {
            for (final MultipartFile attachment : attachments) {
                LOG.info("Attachment name: " + attachment.getOriginalFilename()
                        + " Size " + attachment.getSize());
                try {
                    this.attachmentService.save(jobId, attachment.getOriginalFilename(), attachment.getInputStream());
                } catch (final IOException ioe) {
                    throw new GenieServerException(ioe);
                }
            }
        }

//        final String id = this.executionService.submitJob(jobRequest);
        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
                ServletUriComponentsBuilder
                        .fromCurrentRequest()
                        .path("/{id}")
                        .buildAndExpand(jobRequest.getId())
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
    public JobResource getJob(@PathVariable("id") final String id) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("called for job with id: " + id);
        }
        return this.jobResourceAssembler.toResource(this.jobService.getJob(id));
    }

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
    @RequestMapping(method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public PagedResources<JobResource> getJobs(
            @RequestParam(value = "id", required = false) final String id,
            @RequestParam(value = "name", required = false) final String name,
            @RequestParam(value = "userName", required = false) final String userName,
            @RequestParam(value = "status", required = false) final Set<String> statuses,
            @RequestParam(value = "tag", required = false) final Set<String> tags,
            @RequestParam(value = "executionClusterName", required = false) final String clusterName,
            @RequestParam(value = "executionClusterId", required = false) final String clusterId,
            @RequestParam(value = "commandName", required = false) final String commandName,
            @RequestParam(value = "commandId", required = false) final String commandId,
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
                this.jobService.getJobs(
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
    public void killJob(@PathVariable("id") final String id) throws GenieException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Called for job id: " + id);
        }
        //this.executionService.killJob(id);
    }

    /**
     * Get the original job request.
     *
     * @param id The id of the job
     * @return The job request
     * @throws GenieException On any internal error
     */
    @RequestMapping(value = "/{id}/request", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public JobRequest getJobRequest(@PathVariable("id") final String id) throws GenieException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Get the execution information about a job.
     *
     * @param id The id of the job
     * @return The job execution
     * @throws GenieException On any internal error
     */
    @RequestMapping(value = "/{id}/execution", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public JobExecution getJobExecution(@PathVariable("id") final String id) throws GenieException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
