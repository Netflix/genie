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
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.hateoas.assemblers.ApplicationResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.ClusterResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.CommandResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobExecutionResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobMetadataResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobRequestResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobResourceAssembler;
import com.netflix.genie.web.hateoas.assemblers.JobSearchResultResourceAssembler;
import com.netflix.genie.web.hateoas.resources.ApplicationResource;
import com.netflix.genie.web.hateoas.resources.ClusterResource;
import com.netflix.genie.web.hateoas.resources.CommandResource;
import com.netflix.genie.web.hateoas.resources.JobExecutionResource;
import com.netflix.genie.web.hateoas.resources.JobMetadataResource;
import com.netflix.genie.web.hateoas.resources.JobRequestResource;
import com.netflix.genie.web.hateoas.resources.JobResource;
import com.netflix.genie.web.hateoas.resources.JobSearchResultResource;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.resources.handlers.GenieResourceHttpRequestHandler;
import com.netflix.genie.web.services.AgentRoutingService;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobPersistenceService;
import com.netflix.genie.web.services.JobSearchService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedResources;
import org.springframework.hateoas.mvc.ControllerLinkBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private static final String TRANSFER_ENCODING_HEADER = "Transfer-Encoding";
    private static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";
    private static final String NAME_HEADER_COOKIE = "cookie";
    private static final String JOB_API_TEMPLATE = "/api/v3/jobs/{id}";
    private static final String EMPTY_STRING = "";
    private static final String COMMA = ",";

    private final JobCoordinatorService jobCoordinatorService;
    private final JobSearchService jobSearchService;
    private final AttachmentService attachmentService;
    private final ApplicationResourceAssembler applicationResourceAssembler;
    private final ClusterResourceAssembler clusterResourceAssembler;
    private final CommandResourceAssembler commandResourceAssembler;
    private final JobResourceAssembler jobResourceAssembler;
    private final JobRequestResourceAssembler jobRequestResourceAssembler;
    private final JobExecutionResourceAssembler jobExecutionResourceAssembler;
    private final JobMetadataResourceAssembler jobMetadataResourceAssembler;
    private final JobSearchResultResourceAssembler jobSearchResultResourceAssembler;
    private final String hostname;
    private final RestTemplate restTemplate;
    private final GenieResourceHttpRequestHandler resourceHttpRequestHandler;
    private final JobsProperties jobsProperties;
    private final AgentRoutingService agentRoutingService;
    private final JobPersistenceService jobPersistenceService;

    // Metrics
    private final Counter submitJobWithoutAttachmentsRate;
    private final Counter submitJobWithAttachmentsRate;

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
     * @param jobMetadataResourceAssembler     Assemble job metadata resources out of job metadata DTO
     * @param jobSearchResultResourceAssembler Assemble job search resources out of jobs
     * @param genieHostInfo                    Information about the host that the Genie process is running on
     * @param restTemplate                     The rest template for http requests
     * @param resourceHttpRequestHandler       The handler to return requests for static resources on the
     *                                         Genie File System.
     * @param jobsProperties                   All the properties associated with jobs
     * @param registry                         The metrics registry to use
     * @param jobPersistenceService            Job persistence service
     * @param agentRoutingService              Agent routing service
     */
    @Autowired
    @SuppressWarnings("checkstyle:parameternumber")
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
        final JobMetadataResourceAssembler jobMetadataResourceAssembler,
        final JobSearchResultResourceAssembler jobSearchResultResourceAssembler,
        final GenieHostInfo genieHostInfo,
        @Qualifier("genieRestTemplate") final RestTemplate restTemplate,
        final GenieResourceHttpRequestHandler resourceHttpRequestHandler,
        final JobsProperties jobsProperties,
        final MeterRegistry registry,
        final JobPersistenceService jobPersistenceService,
        final AgentRoutingService agentRoutingService
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
        this.jobMetadataResourceAssembler = jobMetadataResourceAssembler;
        this.jobSearchResultResourceAssembler = jobSearchResultResourceAssembler;
        this.hostname = genieHostInfo.getHostname();
        this.restTemplate = restTemplate;
        this.resourceHttpRequestHandler = resourceHttpRequestHandler;
        this.jobsProperties = jobsProperties;
        this.agentRoutingService = agentRoutingService;
        this.jobPersistenceService = jobPersistenceService;

        // Set up the metrics
        this.submitJobWithoutAttachmentsRate = registry.counter("genie.api.v3.jobs.submitJobWithoutAttachments.rate");
        this.submitJobWithAttachmentsRate = registry.counter("genie.api.v3.jobs.submitJobWithAttachments.rate");
    }

    /**
     * Submit a new job.
     *
     * @param jobRequest         The job request information
     * @param clientHost         client host sending the request
     * @param userAgent          The user agent string
     * @param httpServletRequest The http servlet request
     * @return The submitted job
     * @throws GenieException For any error
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
        @Valid @RequestBody final JobRequest jobRequest,
        @RequestHeader(value = FORWARDED_FOR_HEADER, required = false) final String clientHost,
        @RequestHeader(value = HttpHeaders.USER_AGENT, required = false) final String userAgent,
        final HttpServletRequest httpServletRequest
    ) throws GenieException {
        log.info("[submitJob] Called json method type to submit job: {}", jobRequest);
        this.submitJobWithoutAttachmentsRate.increment();
        return this.handleSubmitJob(jobRequest, null, clientHost, userAgent, httpServletRequest);
    }

    /**
     * Submit a new job with attachments.
     *
     * @param jobRequest         The job request information
     * @param attachments        The attachments for the job
     * @param clientHost         client host sending the request
     * @param userAgent          The user agent string
     * @param httpServletRequest The http servlet request
     * @return The submitted job
     * @throws GenieException For any error
     */
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
        @Valid @RequestPart("request") final JobRequest jobRequest,
        @RequestPart("attachment") final MultipartFile[] attachments,
        @RequestHeader(value = FORWARDED_FOR_HEADER, required = false) final String clientHost,
        @RequestHeader(value = HttpHeaders.USER_AGENT, required = false) final String userAgent,
        final HttpServletRequest httpServletRequest
    ) throws GenieException {
        log.info("[submitJob] Called multipart method to submit job: {}", jobRequest);
        this.submitJobWithAttachmentsRate.increment();
        return this.handleSubmitJob(jobRequest, attachments, clientHost, userAgent, httpServletRequest);
    }

    private ResponseEntity<Void> handleSubmitJob(
        final JobRequest jobRequest,
        final MultipartFile[] attachments,
        final String clientHost,
        final String userAgent,
        final HttpServletRequest httpServletRequest
    ) throws GenieException {
        if (jobRequest == null) {
            throw new GeniePreconditionException("No job request entered. Unable to submit.");
        }

        // get client's host from the context
        final String localClientHost;
        if (StringUtils.isNotBlank(clientHost)) {
            localClientHost = clientHost.split(COMMA)[0];
        } else {
            localClientHost = httpServletRequest.getRemoteAddr();
        }

        final JobRequest jobRequestWithId;
        // If the job request does not contain an id create one else use the one provided.
        final String jobId;
        final Optional<String> jobIdOptional = jobRequest.getId();
        if (jobIdOptional.isPresent() && StringUtils.isNotBlank(jobIdOptional.get())) {
            jobId = jobIdOptional.get();
            jobRequestWithId = jobRequest;
        } else {
            jobId = UUID.randomUUID().toString();
            final JobRequest.Builder builder = new JobRequest.Builder(
                jobRequest.getName(),
                jobRequest.getUser(),
                jobRequest.getVersion(),
                jobRequest.getClusterCriterias(),
                jobRequest.getCommandCriteria()
            )
                .withId(jobId)
                .withDisableLogArchival(jobRequest.isDisableLogArchival())
                .withTags(jobRequest.getTags())
                .withConfigs(jobRequest.getConfigs())
                .withDependencies(jobRequest.getDependencies())
                .withApplications(jobRequest.getApplications());

            jobRequest.getCommandArgs().ifPresent(
                commandArgs ->
                    builder
                        .withCommandArgs(
                            Lists.newArrayList(StringUtils.splitByWholeSeparator(commandArgs, StringUtils.SPACE))
                        )
            );
            jobRequest.getCpu().ifPresent(builder::withCpu);
            jobRequest.getMemory().ifPresent(builder::withMemory);
            jobRequest.getGroup().ifPresent(builder::withGroup);
            jobRequest.getSetupFile().ifPresent(builder::withSetupFile);
            jobRequest.getDescription().ifPresent(builder::withDescription);
            jobRequest.getEmail().ifPresent(builder::withEmail);
            jobRequest.getTimeout().ifPresent(builder::withTimeout);
            jobRequest.getMetadata().ifPresent(builder::withMetadata);

            jobRequestWithId = builder.build();
        }

        // Download attachments
        int numAttachments = 0;
        long totalSizeOfAttachments = 0L;
        if (attachments != null) {
            log.info("Saving attachments for job {}", jobId);
            numAttachments = attachments.length;
            for (final MultipartFile attachment : attachments) {
                totalSizeOfAttachments += attachment.getSize();
                log.debug("Attachment name: {} Size: {}", attachment.getOriginalFilename(), attachment.getSize());
                try {
                    String originalFilename = attachment.getOriginalFilename();
                    if (originalFilename == null) {
                        originalFilename = UUID.randomUUID().toString();
                    }
                    this.attachmentService.save(jobId, originalFilename, attachment.getInputStream());
                } catch (final IOException ioe) {
                    throw new GenieServerException("Failed to save job attachment", ioe);
                }
            }
        }

        final JobMetadata metadata = new JobMetadata
            .Builder()
            .withClientHost(localClientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments)
            .build();

        this.jobCoordinatorService.coordinateJob(jobRequestWithId, metadata);

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
    @GetMapping(value = "/{id}", produces = MediaTypes.HAL_JSON_VALUE)
    public JobResource getJob(
        @PathVariable("id") final String id) throws GenieException {
        log.info("[getJob] Called for job with id: {}", id);
        return this.jobResourceAssembler.toResource(this.jobSearchService.getJob(id));
    }

    /**
     * Get the status of the given job if it exists.
     *
     * @param id The id of the job to get status for
     * @return The status of the job as one of: {@link JobStatus}
     * @throws GenieException on error
     */
    @GetMapping(value = "/{id}/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public JsonNode getJobStatus(
        @PathVariable("id") final String id) throws GenieException {
        log.debug("[getJobStatus] Called for job with id: {}", id);
        final JsonNodeFactory factory = JsonNodeFactory.instance;
        return factory
            .objectNode()
            .set("status", factory.textNode(this.jobSearchService.getJobStatus(id).toString()));
    }

    /**
     * Get jobs for given filter criteria.
     *
     * @param id               id for job
     * @param name             name of job (can be a SQL-style pattern such as HIVE%)
     * @param user             user who submitted job
     * @param statuses         statuses of jobs to find
     * @param tags             tags for the job
     * @param clusterName      the name of the cluster
     * @param clusterId        the id of the cluster
     * @param commandName      the name of the command run by the job
     * @param commandId        the id of the command run by the job
     * @param minStarted       The time which the job had to start after in order to be return (inclusive)
     * @param maxStarted       The time which the job had to start before in order to be returned (exclusive)
     * @param minFinished      The time which the job had to finish after in order to be return (inclusive)
     * @param maxFinished      The time which the job had to finish before in order to be returned (exclusive)
     * @param grouping         The grouping the job should be a member of
     * @param groupingInstance The grouping instance the job should be a member of
     * @param page             page information for job
     * @param assembler        The paged resources assembler to use
     * @return successful response, or one with HTTP error code
     * @throws GenieException For any error
     */
    @GetMapping(produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    @SuppressWarnings("checkstyle:parameternumber")
    public PagedResources<JobSearchResultResource> findJobs(
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
        @RequestParam(value = "grouping", required = false) final String grouping,
        @RequestParam(value = "groupingInstance", required = false) final String groupingInstance,
        @PageableDefault(sort = {"created"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<JobSearchResult> assembler
    ) throws GenieException {
        log.info(
            "[getJobs] Called with "
                + "[id | jobName | user | statuses | clusterName "
                + "| clusterId | minStarted | maxStarted | minFinished | maxFinished | grouping | groupingInstance "
                + "| page]"
        );
        log.info(
            "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
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
            grouping,
            groupingInstance,
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
                    .findJobs(
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
                        grouping,
                        groupingInstance,
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
                minStarted == null ? null : Instant.ofEpochMilli(minStarted),
                maxStarted == null ? null : Instant.ofEpochMilli(maxStarted),
                minFinished == null ? null : Instant.ofEpochMilli(minFinished),
                maxFinished == null ? null : Instant.ofEpochMilli(maxFinished),
                grouping,
                groupingInstance,
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
     * @throws GenieException For any error
     * @throws IOException    on redirect error
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void killJob(
        @PathVariable("id") final String id,
        @RequestHeader(name = JobConstants.GENIE_FORWARDED_FROM_HEADER, required = false) final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException, IOException {
        log.info("[killJob] Called for job id: {}. Forwarded from: {}", id, forwardedFrom);

        // If forwarded from is null this request hasn't been forwarded at all. Check we're on the right node
        if (this.jobsProperties.getForwarding().isEnabled() && forwardedFrom == null) {
            String jobHostname = null;
            try {
                jobHostname = this.getJobOwnerHostname(id);
            } catch (GenieJobNotFoundException e) {
                throw new GenieNotFoundException("Job " + id + " not found", e);
            }

            if (!this.hostname.equals(jobHostname)) {
                log.info("Job {} is not on this node. Forwarding kill request to {}", id, jobHostname);
                final String forwardHost = this.buildForwardHost(jobHostname);
                try {
                    //Need to forward job
                    this.restTemplate.execute(
                        forwardHost + JOB_API_TEMPLATE,
                        HttpMethod.DELETE,
                        forwardRequest -> copyRequestHeaders(request, forwardRequest),
                        (final ClientHttpResponse forwardResponse) -> {
                            response.setStatus(HttpStatus.ACCEPTED.value());
                            copyResponseHeaders(response, forwardResponse);
                            return null;
                        },
                        id
                    );
                } catch (HttpStatusCodeException e) {
                    log.error("Failed killing job on {}. Error: {}", forwardHost, e.getMessage());
                    response.sendError(e.getStatusCode().value(), e.getStatusText());
                } catch (Exception e) {
                    log.error("Failed killing job on {}. Error: {}", forwardHost, e.getMessage());
                    response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
                }

                // No need to do anything on this node
                return;
            }
        }

        log.info("Job {} is on this node. Attempting to kill.", id);
        // Job is on this node so try to kill it
        this.jobCoordinatorService.killJob(id, JobStatusMessages.JOB_KILLED_BY_USER);
        response.setStatus(HttpStatus.ACCEPTED.value());
    }

    /**
     * Get the original job request.
     *
     * @param id The id of the job
     * @return The job request
     * @throws GenieException On any internal error
     */
    @GetMapping(value = "/{id}/request", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public JobRequestResource getJobRequest(
        @PathVariable("id") final String id) throws GenieException {
        log.info("[getJobRequest] Called for job request with id {}", id);
        return this.jobRequestResourceAssembler.toResource(this.jobSearchService.getJobRequest(id));
    }

    /**
     * Get the execution information about a job.
     *
     * @param id The id of the job
     * @return The job execution
     * @throws GenieException On any internal error
     */
    @GetMapping(value = "/{id}/execution", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public JobExecutionResource getJobExecution(
        @PathVariable("id") final String id
    ) throws GenieException {
        log.info("[getJobExecution] Called for job execution with id {}", id);
        return this.jobExecutionResourceAssembler.toResource(this.jobSearchService.getJobExecution(id));
    }

    /**
     * Get the metadata information about a job.
     *
     * @param id The id of the job
     * @return The job metadata
     * @throws GenieException On any internal error
     * @since 3.3.5
     */
    @GetMapping(value = "/{id}/metadata", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public JobMetadataResource getJobMetadata(@PathVariable("id") final String id) throws GenieException {
        log.info("[getJobMetadata] Called for job metadata with id {}", id);
        return this.jobMetadataResourceAssembler.toResource(this.jobSearchService.getJobMetadata(id));
    }

    /**
     * Get the cluster the job was run on or is currently running on.
     *
     * @param id The id of the job to get the cluster for
     * @return The cluster
     * @throws GenieException Usually GenieNotFound exception when either the job or the cluster aren't found
     */
    @GetMapping(value = "/{id}/cluster", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public ClusterResource getJobCluster(
        @PathVariable("id") final String id
    ) throws GenieException {
        log.info("[getJobCluster] Called for job with id {}", id);
        return this.clusterResourceAssembler.toResource(this.jobSearchService.getJobCluster(id));
    }

    /**
     * Get the command the job was run with or is currently running with.
     *
     * @param id The id of the job to get the command for
     * @return The command
     * @throws GenieException Usually GenieNotFound exception when either the job or the command aren't found
     */
    @GetMapping(value = "/{id}/command", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public CommandResource getJobCommand(
        @PathVariable("id") final String id) throws GenieException {
        log.info("[getJobCommand] Called for job with id {}", id);
        return this.commandResourceAssembler.toResource(this.jobSearchService.getJobCommand(id));
    }

    /**
     * Get the applications used ot run the job.
     *
     * @param id The id of the job to get the applications for
     * @return The applications
     * @throws GenieException Usually GenieNotFound exception when either the job or the applications aren't found
     */
    @GetMapping(value = "/{id}/applications", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<ApplicationResource> getJobApplications(
        @PathVariable("id") final String id) throws GenieException {
        log.info("[getJobApplications] Called for job with id {}", id);

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
    @GetMapping(
        value = {
            "/{id}/output",
            "/{id}/output/",
            "/{id}/output/**"
        },
        produces = MediaType.ALL_VALUE
    )
    public void getJobOutput(
        @PathVariable("id") final String id,
        @RequestHeader(name = JobConstants.GENIE_FORWARDED_FROM_HEADER, required = false) final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws IOException, ServletException, GenieException {
        log.info("[getJobOutput] Called for job with id: {}", id);
        final String path = ControllerUtils.getRemainingPath(request);

        // if forwarded from isn't null it's already been forwarded to this node. Assume data is on this node.
        if (this.jobsProperties.getForwarding().isEnabled() && forwardedFrom == null) {
            // TODO: It's possible that could use the JobMonitorCoordinator to check this in memory
            //       However that could get into problems where the job finished or died
            //       and it would return false on check if the job with given id is running on that node
            final String jobHostname = this.jobSearchService.getJobHost(id);
            if (!this.hostname.equals(jobHostname)) {
                log.info("Job {} is not or was not run on this node. Forwarding to {}", id, jobHostname);
                final String forwardHost = this.buildForwardHost(jobHostname);
                try {
                    this.restTemplate.execute(
                        forwardHost + JOB_API_TEMPLATE + "/output/{path}",
                        HttpMethod.GET,
                        forwardRequest -> copyRequestHeaders(request, forwardRequest),
                        (ResponseExtractor<Void>) forwardResponse -> {
                            response.setStatus(forwardResponse.getStatusCode().value());
                            copyResponseHeaders(response, forwardResponse);
                            // Documentation I could find pointed to the HttpEntity reading the bytes off
                            // the stream so this should resolve memory problems if the file returned is large
                            ByteStreams.copy(forwardResponse.getBody(), response.getOutputStream());
                            return null;
                        },
                        id,
                        path == null ? EMPTY_STRING : path
                    );
                } catch (final HttpStatusCodeException e) {
                    log.error("Failed getting the remote job output from {}. Error: {}", forwardHost, e.getMessage());
                    response.sendError(e.getStatusCode().value(), e.getStatusText());
                } catch (final Exception e) {
                    log.error("Failed getting the remote job output from {}. Error: {}", forwardHost, e.getMessage());
                    response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value(), e.getMessage());
                }

                //No need to search on this node
                return;
            }
        }

        log.info("Job {} is running or was run on this node. Fetching requested resource...", id);
        if (StringUtils.isNotBlank(path)) {
            request.setAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_IS_ROOT_DIRECTORY, false);
        } else {
            request.setAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_IS_ROOT_DIRECTORY, true);
        }
        log.debug("PATH = {}", path);
        request.setAttribute(HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE, path);
        request.setAttribute(GenieResourceHttpRequestHandler.GENIE_JOB_ID_ATTRIBUTE, id);

        this.resourceHttpRequestHandler.handleRequest(request, response);
    }

    private String buildForwardHost(final String jobHostname) {
        return this.jobsProperties.getForwarding().getScheme()
            + "://"
            + jobHostname
            + ":"
            + this.jobsProperties.getForwarding().getPort();
    }

    private void copyRequestHeaders(final HttpServletRequest request, final ClientHttpRequest forwardRequest) {
        // Copy all the headers (necessary for ACCEPT and security headers especially). Do not copy the cookie header.
        final HttpHeaders headers = forwardRequest.getHeaders();
        final Enumeration<String> headerNames = request.getHeaderNames();
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                final String headerName = headerNames.nextElement();
                if (!NAME_HEADER_COOKIE.equals(headerName)) {
                    final String headerValue = request.getHeader(headerName);
                    log.debug("Request Header: name = {} value = {}", headerName, headerValue);
                    headers.add(headerName, headerValue);
                }
            }
        }
        // Lets add the cookie as an header
        final Cookie[] cookies = request.getCookies();
        if (cookies != null && cookies.length > 0) {
            StringBuilder builder = null;
            for (final Cookie cookie : request.getCookies()) {
                if (builder == null) {
                    builder = new StringBuilder();
                } else {
                    builder.append(",");
                }
                builder.append(cookie.getName()).append("=").append(cookie.getValue());
            }
            if (builder != null) {
                final String cookieValue = builder.toString();
                headers.add(NAME_HEADER_COOKIE, cookieValue);
                log.debug("Request Header: name = {} value = {}", NAME_HEADER_COOKIE, cookieValue);
            }
        }
        // This method only called when need to forward so add the forwarded from header
        headers.add(JobConstants.GENIE_FORWARDED_FROM_HEADER, request.getRequestURL().toString());
    }

    private void copyResponseHeaders(final HttpServletResponse response, final ClientHttpResponse forwardResponse) {
        final HttpHeaders headers = forwardResponse.getHeaders();
        for (final Map.Entry<String, String> header : headers.toSingleValueMap().entrySet()) {
            //
            // Do not add transfer encoding header since it forces Apache to truncate the response. Ideally we should
            // only copy headers that are needed.
            //
            if (!TRANSFER_ENCODING_HEADER.equalsIgnoreCase(header.getKey())) {
                response.setHeader(header.getKey(), header.getValue());
            }
        }
    }

    /*
     * Helper to find the owner for a job.
     * Owner is defined as
     * 1. For v3 jobs the server that spawned the process running the job
     * 2. For v4 jobs the server owning the active connection to the agent
     *
     */
    private String getJobOwnerHostname(
        @NotBlank(message = "No job id entered. Unable to find the job owner.") final String jobId
    ) throws GenieNotFoundException {
        if (jobPersistenceService.isV4(jobId)) {
            return this.agentRoutingService
                .getHostnameForAgentConnection(jobId)
                .orElseThrow(() -> new GenieNotFoundException(
                        "No hostname found for v4 job - " + jobId
                    )
                );
        } else {
            return this.jobSearchService.getJobHost(jobId);
        }
    }
}
