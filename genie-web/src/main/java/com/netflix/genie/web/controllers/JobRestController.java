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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.io.ByteStreams;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.services.AttachmentService;
import com.netflix.genie.core.services.JobCoordinatorService;
import com.netflix.genie.core.services.JobSearchService;
import com.netflix.genie.web.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobExecutionResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobRequestResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobSearchResultResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import com.netflix.genie.web.hateoas.resources.JobExecutionResource;
import com.netflix.genie.web.hateoas.resources.JobRequestResource;
import com.netflix.genie.web.hateoas.resources.JobResource;
import com.netflix.genie.web.hateoas.resources.JobSearchResultResource;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
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
import java.util.Date;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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

    private static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";

    private final JobCoordinatorService jobCoordinatorService;
    private final JobSearchService jobSearchService;
    private final AttachmentService attachmentService;
    private final ApplicationResourceAssembler applicationResourceAssembler;
    private final ClusterResourceAssembler clusterResourceAssembler;
    private final CommandResourceAssembler commandResourceAssembler;
    private final JobResourceAssembler jobResourceAssembler;
    private final JobRequestResourceAssembler jobRequestResourceAssembler;
    private final JobExecutionResourceAssembler jobExecutionResourceAssembler;
    private final JobSearchResultResourceAssembler jobSearchResultResourceAssembler;
    private final String hostName;
    private final HttpClient httpClient;
    private final GenieResourceHttpRequestHandler resourceHttpRequestHandler;
    private final boolean forwardingEnabled;

    /**
     * Constructor.
     *
     * @param jobCoordinatorService            The job coordinator service to use.
     * @param jobSearchService                 The search service to use
     * @param attachmentService                The attachment service to use to save attachments.
     * @param applicationResourceAssembler     Assemble application resources out of applications
     * @param clusterResourceAssembler         Assemble cluster resources out of applications
     * @param commandResourceAssembler         Assemble cluster resources out of applications
     * @param jobResourceAssembler             Assemble job resources out of jobs
     * @param jobRequestResourceAssembler      Assemble job request resources out of job requests
     * @param jobExecutionResourceAssembler    Assemble job execution resources out of job executions
     * @param jobSearchResultResourceAssembler Assemble job search resources out of jobs
     * @param hostName                         The hostname this Genie instance is running on
     * @param httpClient                       The http client to use for forwarding requests
     * @param resourceHttpRequestHandler       The handler to return requests for static resources on the
     *                                         Genie File System.
     * @param forwardingEnabled                Whether directory and kill request forwarding is enabled or not
     */
    @Autowired
    public JobRestController(
        final JobCoordinatorService jobCoordinatorService,
        final JobSearchService jobSearchService,
        final AttachmentService attachmentService,
        final ApplicationResourceAssembler applicationResourceAssembler,
        final ClusterResourceAssembler clusterResourceAssembler,
        final CommandResourceAssembler commandResourceAssembler,
        final JobResourceAssembler jobResourceAssembler,
        final JobRequestResourceAssembler jobRequestResourceAssembler,
        final JobExecutionResourceAssembler jobExecutionResourceAssembler,
        final JobSearchResultResourceAssembler jobSearchResultResourceAssembler,
        final String hostName,
        final HttpClient httpClient,
        final GenieResourceHttpRequestHandler resourceHttpRequestHandler,
        @Value("${genie.jobs.forwarding.enabled}") final boolean forwardingEnabled
    ) {
        this.jobCoordinatorService = jobCoordinatorService;
        this.jobSearchService = jobSearchService;
        this.attachmentService = attachmentService;
        this.applicationResourceAssembler = applicationResourceAssembler;
        this.clusterResourceAssembler = clusterResourceAssembler;
        this.commandResourceAssembler = commandResourceAssembler;
        this.jobResourceAssembler = jobResourceAssembler;
        this.jobRequestResourceAssembler = jobRequestResourceAssembler;
        this.jobExecutionResourceAssembler = jobExecutionResourceAssembler;
        this.jobSearchResultResourceAssembler = jobSearchResultResourceAssembler;
        this.hostName = hostName;
        this.httpClient = httpClient;
        this.resourceHttpRequestHandler = resourceHttpRequestHandler;
        this.forwardingEnabled = forwardingEnabled;
    }

    /**
     * Submit a new job.
     *
     * @param jobRequest         The job request information
     * @param clientHost         client host sending the request
     * @param httpServletRequest The http servlet request
     * @return The submitted job
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
        @RequestBody
        final JobRequest jobRequest,
        @RequestHeader(value = FORWARDED_FOR_HEADER, required = false)
        final String clientHost,
        final HttpServletRequest httpServletRequest
    ) throws GenieException {
        if (jobRequest == null) {
            throw new GenieException(HttpURLConnection.HTTP_PRECON_FAILED, "No job entered. Unable to submit.");
        }
        log.debug("Called to submit job: {}", jobRequest);

        return this.submitJob(
            jobRequest,
            null,
            clientHost,
            httpServletRequest
        );
    }

    /**
     * Submit a new job with attachments.
     *
     * @param jobRequest         The job request information
     * @param attachments        The attachments for the job
     * @param clientHost         client host sending the request
     * @param httpServletRequest The http servlet request
     * @return The submitted job
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
        @RequestPart("request")
        final JobRequest jobRequest,
        @RequestPart("attachment")
        final MultipartFile[] attachments,
        @RequestHeader(value = FORWARDED_FOR_HEADER, required = false)
        final String clientHost,
        final HttpServletRequest httpServletRequest
    ) throws GenieException {
        if (jobRequest == null) {
            throw new GenieException(HttpURLConnection.HTTP_PRECON_FAILED, "No job entered. Unable to submit.");
        }
        log.debug("Called to submit job: {}", jobRequest);

        // get client's host from the context
        final String localClientHost;
        if (StringUtils.isNotBlank(clientHost)) {
            localClientHost = clientHost.split(",")[0];
        } else {
            localClientHost = httpServletRequest.getRemoteAddr();
        }

        final JobRequest jobRequestWithId;
        // If the job request does not contain an id create one else use the one provided.
        final String jobId;
        if (StringUtils.isNotBlank(jobRequest.getId())) {
            jobId = jobRequest.getId();
            jobRequestWithId = jobRequest;
        } else {
            jobId = UUID.randomUUID().toString();
            jobRequestWithId = new JobRequest.Builder(
                jobRequest.getName(),
                jobRequest.getUser(),
                jobRequest.getVersion(),
                jobRequest.getCommandArgs(),
                jobRequest.getClusterCriterias(),
                jobRequest.getCommandCriteria()
            ).withId(jobId)
                .withCpu(jobRequest.getCpu())
                .withMemory(jobRequest.getMemory())
                .withDisableLogArchival(jobRequest.isDisableLogArchival())
                .withGroup(jobRequest.getGroup())
                .withSetupFile(jobRequest.getSetupFile())
                .withDescription(jobRequest.getDescription())
                .withTags(jobRequest.getTags())
                .withEmail(jobRequest.getEmail())
                .withDependencies(jobRequest.getDependencies())
                .withTimeout(jobRequest.getTimeout())
                .build();
        }

        // Download attachments
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

        this.jobCoordinatorService.coordinateJob(jobRequestWithId, localClientHost);

        final HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
            ServletUriComponentsBuilder
                .fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(jobId)
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
        return this.jobResourceAssembler.toResource(this.jobSearchService.getJob(id));
    }

    /**
     * Get the status of the given job if it exists.
     *
     * @param id The id of the job to get status for
     * @return The status of the job as one of: {@link JobStatus}
     * @throws GenieException on error
     */
    @RequestMapping(value = "/{id}/status", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public JsonNode getJobStatus(@PathVariable("id") final String id) throws GenieException {
        final JsonNodeFactory factory = JsonNodeFactory.instance;
        return factory
            .objectNode()
            .set("status", factory.textNode(this.jobSearchService.getJobStatus(id).toString()));
    }

    /**
     * Get jobs for given filter criteria.
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
     * @param page        page information for job
     * @param assembler   The paged resources assembler to use
     * @return successful response, or one with HTTP error code
     * @throws GenieException For any error
     */
    @RequestMapping(method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public PagedResources<JobSearchResultResource> getJobs(
        @RequestParam(value = "id", required = false) final String id,
        @RequestParam(value = "name", required = false) final String name,
        @RequestParam(value = "user", required = false) final String user,
        @RequestParam(value = "status", required = false) final Set<String> statuses,
        @RequestParam(value = "tag", required = false) final Set<String> tags,
        @RequestParam(value = "clusterName", required = false) final String clusterName,
        @RequestParam(value = "clusterId", required = false) final String clusterId,
        @RequestParam(value = "commandName", required = false) final String commandName,
        @RequestParam(value = "commandId", required = false) final String commandId,
        @RequestParam(value = "minStarted", required = false) final Long minStarted,
        @RequestParam(value = "maxStarted", required = false) final Long maxStarted,
        @RequestParam(value = "minFinished", required = false) final Long minFinished,
        @RequestParam(value = "maxFinished", required = false) final Long maxFinished,
        @PageableDefault(sort = {"created"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<JobSearchResult> assembler
    ) throws GenieException {
        log.debug(
            "Called with "
                + "[id | jobName | user | statuses | clusterName "
                + "| clusterId | minStarted | maxStarted | minFinished | maxFinished | page]"
        );
        log.debug(
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
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

        // Build the self link which will be used for the next, previous, etc links
        final Link self = ControllerLinkBuilder
            .linkTo(
                ControllerLinkBuilder
                    .methodOn(JobRestController.class)
                    .getJobs(
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
                        page,
                        assembler
                    )
            ).withSelfRel();

        return assembler.toResource(
            this.jobSearchService.findJobs(
                id,
                name,
                user,
                enumStatuses,
                tags,
                clusterName,
                clusterId,
                commandName,
                commandId,
                minStarted == null ? null : new Date(minStarted),
                maxStarted == null ? null : new Date(maxStarted),
                minFinished == null ? null : new Date(minFinished),
                maxFinished == null ? null : new Date(maxFinished),
                page
            ),
            this.jobSearchResultResourceAssembler,
            self
        );
    }

    /**
     * Kill job based on given job ID.
     *
     * @param id            id for job to kill
     * @param forwardedFrom The host this request was forwarded from if present
     * @param request       the servlet request
     * @param response      the servlet response
     * @throws GenieException   For any error
     * @throws IOException      on redirect error
     * @throws ServletException when trying to handle the request
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void killJob(
        @PathVariable("id") final String id,
        @RequestHeader(name = JobConstants.GENIE_FORWARDED_FROM_HEADER, required = false) final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException, IOException, ServletException {
        log.debug("Called for job id: {}. Forwarded from: {}", id, forwardedFrom);

        // If forwarded from is null this request hasn't been forwarded at all. Check we're on the right node
        if (this.forwardingEnabled && forwardedFrom == null) {
            final String jobHostname = this.jobSearchService.getJobHost(id);
            if (!this.hostName.equals(jobHostname)) {
                //Need to forward job
                final HttpDelete deleteRequest = new HttpDelete(this.buildForwardURL(request, jobHostname));
                this.copyRequestHeaders(request, deleteRequest);
                final HttpResponse deleteResponse = this.httpClient.execute(deleteRequest);

                if (this.forwardResponseHasError(response, deleteResponse)) {
                    // Method already sent error through servlet response
                    return;
                }

                response.setStatus(HttpStatus.ACCEPTED.value());
                this.copyResponseHeaders(response, deleteResponse);

                // No need to do anything on this node
                return;
            }
        }

        // Job is on this node so try to kill it
        this.jobCoordinatorService.killJob(id);
        response.setStatus(HttpStatus.ACCEPTED.value());
    }

    /**
     * Get the original job request.
     *
     * @param id The id of the job
     * @return The job request
     * @throws GenieException On any internal error
     */
    @RequestMapping(value = "/{id}/request", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public JobRequestResource getJobRequest(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called for job request with id {}", id);
        return this.jobRequestResourceAssembler.toResource(this.jobSearchService.getJobRequest(id));
    }

    /**
     * Get the execution information about a job.
     *
     * @param id The id of the job
     * @return The job execution
     * @throws GenieException On any internal error
     */
    @RequestMapping(value = "/{id}/execution", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public JobExecutionResource getJobExecution(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called for job execution with id {}", id);
        return this.jobExecutionResourceAssembler.toResource(this.jobSearchService.getJobExecution(id));
    }

    /**
     * Get the cluster the job was run on or is currently running on.
     *
     * @param id The id of the job to get the cluster for
     * @return The cluster
     * @throws GenieException Usually GenieNotFound exception when either the job or the cluster aren't found
     */
    @RequestMapping(value = "/{id}/cluster", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ClusterResource getJobCluster(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        return this.clusterResourceAssembler.toResource(this.jobSearchService.getJobCluster(id));
    }

    /**
     * Get the command the job was run with or is currently running with.
     *
     * @param id The id of the job to get the command for
     * @return The command
     * @throws GenieException Usually GenieNotFound exception when either the job or the command aren't found
     */
    @RequestMapping(value = "/{id}/command", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public CommandResource getJobCommand(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called for job with id {}", id);
        return this.commandResourceAssembler.toResource(this.jobSearchService.getJobCommand(id));
    }

    /**
     * Get the applications used ot run the job.
     *
     * @param id The id of the job to get the applications for
     * @return The applications
     * @throws GenieException Usually GenieNotFound exception when either the job or the applications aren't found
     */
    @RequestMapping(value = "/{id}/applications", method = RequestMethod.GET, produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<ApplicationResource> getJobApplications(@PathVariable("id") final String id) throws GenieException {
        log.debug("Called for job with id {}", id);

        return this.jobSearchService
            .getJobApplications(id)
            .stream()
            .map(this.applicationResourceAssembler::toResource)
            .collect(Collectors.toList());
    }

    /**
     * Get the job output directory.
     *
     * @param id            The id of the job to get output for
     * @param forwardedFrom The host this request was forwarded from if present
     * @param request       the servlet request
     * @param response      the servlet response
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
        @RequestHeader(name = JobConstants.GENIE_FORWARDED_FROM_HEADER, required = false) final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws IOException, ServletException, GenieException {
        // if forwarded from isn't null it's already been forwarded to this node. Assume data is on this node.
        if (this.forwardingEnabled && forwardedFrom == null) {
            // TODO: It's possible that could use the JobMonitorCoordinator to check this in memory
            //       However that could get into problems where the job finished or died
            //       and it would return false on check if the job with given id is running on that node
            final String jobHostname = this.jobSearchService.getJobHost(id);
            if (!this.hostName.equals(jobHostname)) {
                // Use Apache HttpClient for easier access to result bytes as stream than RestTemplate
                // RestTemplate read entire byte[] payload into memory before the result object even given back to
                // application control. Concerned about people getting stdout which could be huge file.
                final HttpGet getRequest = new HttpGet(this.buildForwardURL(request, jobHostname));
                this.copyRequestHeaders(request, getRequest);
                final HttpResponse getResponse = this.httpClient.execute(getRequest);

                if (this.forwardResponseHasError(response, getResponse)) {
                    // Method already sent error through servlet response
                    return;
                }

                response.setStatus(HttpStatus.OK.value());
                this.copyResponseHeaders(response, getResponse);

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

    private String buildForwardURL(final HttpServletRequest request, final String jobHostname) {
        return request.getScheme() + "://" + jobHostname + ":" + request.getServerPort() + request.getRequestURI();
    }

    private void copyRequestHeaders(final HttpServletRequest request, final HttpRequestBase forwardRequest) {
        // Copy all the headers (necessary for ACCEPT and security headers especially)
        final Enumeration<String> headerNames = request.getHeaderNames();
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                final String headerName = headerNames.nextElement();
                final String headerValue = request.getHeader(headerName);
                log.debug("Request Header: name = {} value = {}", headerName, headerValue);
                forwardRequest.addHeader(headerName, headerValue);
            }
        }

        // This method only called when need to forward so add the forwarded from header
        forwardRequest.addHeader(JobConstants.GENIE_FORWARDED_FROM_HEADER, request.getRequestURL().toString());
    }

    private boolean forwardResponseHasError(
        final HttpServletResponse response,
        final HttpResponse forwardResponse
    ) throws IOException {
        final int statusCode = forwardResponse.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.OK.value() && statusCode != HttpStatus.ACCEPTED.value()) {
            response.sendError(statusCode, forwardResponse.getStatusLine().getReasonPhrase());
            return true;
        } else {
            return false;
        }
    }

    private void copyResponseHeaders(final HttpServletResponse response, final HttpResponse forwardResponse) {
        for (final Header header : forwardResponse.getAllHeaders()) {
            response.setHeader(header.getName(), header.getValue());
        }
    }
}
