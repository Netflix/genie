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

import com.google.common.io.ByteStreams;
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
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.util.AntPathMatcher;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.EnumSet;
import java.util.Enumeration;
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
@Slf4j
public class JobRestController {

    private final JobService jobService;
    private final AttachmentService attachmentService;
    private final JobResourceAssembler jobResourceAssembler;
    private final String hostname;
    private final HttpClient httpClient;
    private final GenieResourceHttpRequestHandler resourceHttpRequestHandler;
    private final boolean directoryForwardingEnabled;

    /**
     * Constructor.
     *
     * @param jobService                 The job search service to use.
     * @param attachmentService          The attachment service to use to save attachments.
     * @param jobResourceAssembler       Assemble job resources out of jobs
     * @param hostname                   The hostname this Genie instance is running on
     * @param httpClient                 The http client to use for forwarding requests
     * @param resourceHttpRequestHandler The handler to return requests for static resources on the Genie File System.
     * @param directoryForwardingEnabled Whether job directory forwarding is enabled or not
     */
    @Autowired
    public JobRestController(
        final JobService jobService,
        final AttachmentService attachmentService,
        final JobResourceAssembler jobResourceAssembler,
        final String hostname,
        final HttpClient httpClient,
        final GenieResourceHttpRequestHandler resourceHttpRequestHandler,
        @Value("${genie.jobs.dir.forwarding.enabled}") final boolean directoryForwardingEnabled
    ) {
        this.jobService = jobService;
        this.attachmentService = attachmentService;
        this.jobResourceAssembler = jobResourceAssembler;
        this.hostname = hostname;
        this.httpClient = httpClient;
        this.resourceHttpRequestHandler = resourceHttpRequestHandler;
        this.directoryForwardingEnabled = directoryForwardingEnabled;
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
        log.debug("Called to submit job: {}", jobRequest);

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
//            log.debug("called from: {}", localClientHost);
//            job.setClientHost(localClientHost);
//        }

        final String id = this.jobService.runJob(jobRequest);
        //final String id = "blah";
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
        log.debug("Called to submit job: {}", jobRequest);

        final String jobId = UUID.randomUUID().toString();
        if (attachments != null) {
            for (final MultipartFile attachment : attachments) {
                log.debug("Attachment name: {} Size: {}", attachment.getOriginalFilename(), attachment.getSize());
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
        log.debug("called for job with id: {}", id);
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
        @RequestParam(value = "clusterName", required = false) final String clusterName,
        @RequestParam(value = "clusterId", required = false) final String clusterId,
        @RequestParam(value = "commandName", required = false) final String commandName,
        @RequestParam(value = "commandId", required = false) final String commandId,
        @PageableDefault(page = 0, size = 64, sort = {"updated"}, direction = Sort.Direction.DESC)
        final Pageable page,
        final PagedResourcesAssembler<Job> assembler
    ) throws GenieException {
        log.debug(
            "Called with "
                + "[id | jobName | userName | statuses | clusterName | clusterId | page]"
        );
        log.debug(
            "{} | {} | {} | {} | {} | {} | {}",
            id,
            name,
            userName,
            statuses,
            tags,
            clusterName,
            clusterId,
            commandName,
            commandId,
            page
        );
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
        log.debug("Called for job id: {}", id);
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

    /**
     * Get the job output directory.
     *
     * @param id            The id of the job to get output for
     * @param forwardedFrom The host this request was forwarded from if present
     * @param request       the servlet request
     * @param response      the servlet response to send the redirect with
     * @throws IOException      on redirect error
     * @throws ServletException when trying to handle the request
     * @throws GenieException   on any Genie internal error
     */
    @RequestMapping(
        value = {
            "/{id}/output",
            "/{id}/output/",
            "/{id}/output/**"
        },
        method = RequestMethod.GET,
        produces = MediaType.ALL_VALUE
    )
    public void getJobOutput(
        @PathVariable("id") final String id,
        @RequestHeader(
            name = GenieResourceHttpRequestHandler.GENIE_FORWARDED_FROM_HEADER,
            required = false
        ) final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws IOException, ServletException, GenieException {
        // if forwarded from isn't null it's already been forwarded to this node. Assume data is on this node.
        if (this.directoryForwardingEnabled && forwardedFrom == null) {
            final String jobHostName = this.jobService.getJobHost(id);
            if (!this.hostname.equals(jobHostName)) {
                // Use Apache HttpClient for easier access to result bytes as stream than RestTemplate
                // RestTemplate read entire byte[] payload into memory before the result object even given back to
                // application control. Concerned about people getting stdout which could be huge file.
                final HttpGet getRequest = new HttpGet(
                    request.getScheme() + "://" + jobHostName + ":" + request.getServerPort() + request.getRequestURI()
                );

                // Copy all the headers (necessary for ACCEPT and security headers especially)
                final Enumeration<String> headerNames = request.getHeaderNames();
                if (headerNames != null) {
                    while (headerNames.hasMoreElements()) {
                        final String headerName = headerNames.nextElement();
                        final String headerValue = request.getHeader(headerName);
                        log.debug("Request Header: name = {} value = {}", headerName, headerValue);
                        getRequest.addHeader(headerName, headerValue);
                    }
                }
                getRequest.addHeader(
                    GenieResourceHttpRequestHandler.GENIE_FORWARDED_FROM_HEADER,
                    request.getRequestURL().toString()
                );

                final HttpResponse getResponse = this.httpClient.execute(getRequest);
                if (getResponse.getStatusLine().getStatusCode() != HttpStatus.OK.value()) {
                    response.sendError(getResponse.getStatusLine().getStatusCode());
                    return;
                }

                response.setStatus(HttpStatus.OK.value());
                for (final Header header : getResponse.getAllHeaders()) {
                    response.setHeader(header.getName(), header.getValue());
                }

                // Documentation I could find pointed to the HttpEntity reading the bytes off the stream so this should
                // resolve memory problems if the file returned is large
                try (final InputStream inputStream = getResponse.getEntity().getContent()) {
                    ByteStreams.copy(inputStream, response.getOutputStream());
                }

                //No need to search on this node
                return;
            }
        }

        String path = (String) request.getAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        if (path != null) {
            final String bestMatchPattern
                = (String) request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            log.debug("bestMatchPattern = {}", bestMatchPattern);
            path = new AntPathMatcher().extractPathWithinPattern(bestMatchPattern, path);
            if (StringUtils.isNotBlank(path)) {
                request.setAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_IS_ROOT_DIRECTORY, false);
            } else {
                request.setAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_IS_ROOT_DIRECTORY, true);
            }
        }
        log.debug("PATH = {}", path);
        request.setAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE, id + "/" + path);

        this.resourceHttpRequestHandler.handleRequest(request, response);
    }
}
