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
package com.netflix.genie.web.apis.rest.v3.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.io.ByteStreams;
import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.Job;
import com.netflix.genie.common.dto.JobExecution;
import com.netflix.genie.common.dto.JobMetadata;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.dto.JobStatusMessages;
import com.netflix.genie.common.dto.search.JobSearchResult;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.external.dtos.v4.ApiClientMetadata;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobRequestMetadata;
import com.netflix.genie.common.internal.dtos.v4.converters.DtoConverters;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieJobNotFoundException;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.util.GenieHostInfo;
import com.netflix.genie.web.agent.services.AgentRoutingService;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ApplicationModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.ClusterModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.CommandModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.EntityModelAssemblers;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobExecutionModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobMetadataModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobRequestModelAssembler;
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.JobSearchResultModelAssembler;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.dtos.JobSubmission;
import com.netflix.genie.web.exceptions.checked.NotFoundException;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.JobCoordinatorService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobLaunchService;
import com.netflix.genie.web.services.LegacyAttachmentService;
import com.netflix.genie.web.util.JobExecutionModeSelector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.data.web.PagedResourcesAssembler;
import org.springframework.hateoas.EntityModel;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.MediaTypes;
import org.springframework.hateoas.PagedModel;
import org.springframework.hateoas.server.mvc.WebMvcLinkBuilder;
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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import javax.annotation.Nullable;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.util.Arrays;
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
    private static final String JOB_API_BASE_PATH = "/api/v3/jobs/";
    private static final String COMMA = ",";

    private final JobLaunchService jobLaunchService;
    private final JobCoordinatorService jobCoordinatorService;
    private final ApplicationModelAssembler applicationModelAssembler;
    private final ClusterModelAssembler clusterModelAssembler;
    private final CommandModelAssembler commandModelAssembler;
    private final JobModelAssembler jobModelAssembler;
    private final JobRequestModelAssembler jobRequestModelAssembler;
    private final JobExecutionModelAssembler jobExecutionModelAssembler;
    private final JobMetadataModelAssembler jobMetadataModelAssembler;
    private final JobSearchResultModelAssembler jobSearchResultModelAssembler;
    private final String hostname;
    private final RestTemplate restTemplate;
    private final JobDirectoryServerService jobDirectoryServerService;
    private final JobsProperties jobsProperties;
    private final AgentRoutingService agentRoutingService;
    private final PersistenceService persistenceService;
    private final Environment environment;
    private final AttachmentService attachmentService;

    // TODO: V3 Execution only
    private final LegacyAttachmentService legacyAttachmentService;
    private final JobExecutionModeSelector jobExecutionModeSelector;

    // Metrics
    private final Counter submitJobWithoutAttachmentsRate;
    private final Counter submitJobWithAttachmentsRate;

    /**
     * Constructor.
     * @param jobLaunchService          The {@link JobLaunchService} implementation to use
     * @param dataServices              The {@link DataServices} instance to use
     * @param jobCoordinatorService     The job coordinator service to use.
     * @param entityModelAssemblers     The encapsulation of all the V3 resource assemblers
     * @param genieHostInfo             Information about the host that the Genie process is running on
     * @param restTemplate              The rest template for http requests
     * @param jobDirectoryServerService The service to handle serving back job directory resources
     * @param jobsProperties            All the properties associated with jobs
     * @param registry                  The metrics registry to use
     * @param agentRoutingService       Agent routing service
     * @param environment               The application environment to pull dynamic properties from
     * @param attachmentService         The attachment service to use to save attachments.
     * @param legacyAttachmentService   The attachment service to use to save attachments.
     * @param jobExecutionModeSelector  The execution mode (agent vs. embedded) mode selector
     */
    @Autowired
    @SuppressWarnings("checkstyle:parameternumber")
    public JobRestController(
        final JobLaunchService jobLaunchService,
        final DataServices dataServices,
        final JobCoordinatorService jobCoordinatorService,
        final EntityModelAssemblers entityModelAssemblers,
        final GenieHostInfo genieHostInfo,
        @Qualifier("genieRestTemplate") final RestTemplate restTemplate,
        final JobDirectoryServerService jobDirectoryServerService,
        final JobsProperties jobsProperties,
        final MeterRegistry registry,
        final AgentRoutingService agentRoutingService,
        final Environment environment,
        final AttachmentService attachmentService,
        final LegacyAttachmentService legacyAttachmentService,
        final JobExecutionModeSelector jobExecutionModeSelector
    ) {
        this.jobLaunchService = jobLaunchService;
        this.jobCoordinatorService = jobCoordinatorService;
        this.applicationModelAssembler = entityModelAssemblers.getApplicationModelAssembler();
        this.clusterModelAssembler = entityModelAssemblers.getClusterModelAssembler();
        this.commandModelAssembler = entityModelAssemblers.getCommandModelAssembler();
        this.jobModelAssembler = entityModelAssemblers.getJobModelAssembler();
        this.jobRequestModelAssembler = entityModelAssemblers.getJobRequestModelAssembler();
        this.jobExecutionModelAssembler = entityModelAssemblers.getJobExecutionModelAssembler();
        this.jobMetadataModelAssembler = entityModelAssemblers.getJobMetadataModelAssembler();
        this.jobSearchResultModelAssembler = entityModelAssemblers.getJobSearchResultModelAssembler();
        this.hostname = genieHostInfo.getHostname();
        this.restTemplate = restTemplate;
        this.jobDirectoryServerService = jobDirectoryServerService;
        this.jobsProperties = jobsProperties;
        this.agentRoutingService = agentRoutingService;
        this.persistenceService = dataServices.getPersistenceService();
        this.environment = environment;
        this.attachmentService = attachmentService;

        // TODO: V3 Only. Remove.
        this.legacyAttachmentService = legacyAttachmentService;
        this.jobExecutionModeSelector = jobExecutionModeSelector;

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
     * @throws GenieException        For any error
     * @throws GenieCheckedException For V4 Agent Execution errors
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
        @Valid @RequestBody final JobRequest jobRequest,
        @RequestHeader(value = FORWARDED_FOR_HEADER, required = false) @Nullable final String clientHost,
        @RequestHeader(value = HttpHeaders.USER_AGENT, required = false) @Nullable final String userAgent,
        final HttpServletRequest httpServletRequest
    ) throws GenieException, GenieCheckedException {
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
     * @throws GenieException        For any error
     * @throws GenieCheckedException For V4 Agent Execution errors
     */
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ResponseEntity<Void> submitJob(
        @Valid @RequestPart("request") final JobRequest jobRequest,
        @RequestPart("attachment") final MultipartFile[] attachments,
        @RequestHeader(value = FORWARDED_FOR_HEADER, required = false) @Nullable final String clientHost,
        @RequestHeader(value = HttpHeaders.USER_AGENT, required = false) @Nullable final String userAgent,
        final HttpServletRequest httpServletRequest
    ) throws GenieException, GenieCheckedException {
        log.info(
            "[submitJob] Called multipart method to submit job: {}, with {} attachments",
            jobRequest,
            attachments.length
        );
        this.submitJobWithAttachmentsRate.increment();
        return this.handleSubmitJob(jobRequest, attachments, clientHost, userAgent, httpServletRequest);
    }

    private ResponseEntity<Void> handleSubmitJob(
        final JobRequest jobRequest,
        @Nullable final MultipartFile[] attachments,
        @Nullable final String clientHost,
        @Nullable final String userAgent,
        final HttpServletRequest httpServletRequest
    ) throws GenieException, GenieCheckedException {
        // TODO: This is quick and dirty and we may want to handle it better overall for the system going forward
        //       e.g. should it be in an filter that we can do for more APIs?
        //            should value be cached rather than checking every time?
        if (!this.environment.getProperty(JobConstants.JOB_SUBMISSION_ENABLED_PROPERTY_KEY, Boolean.class, true)) {
            // Job Submission is disabled
            throw new GenieServerUnavailableException(
                this.environment.getProperty(
                    JobConstants.JOB_SUBMISSION_DISABLED_MESSAGE_KEY,
                    JobConstants.JOB_SUBMISSION_DISABLED_DEFAULT_MESSAGE
                )
            );
        }

        // get client's host from the context
        final String localClientHost;
        if (StringUtils.isNotBlank(clientHost)) {
            localClientHost = clientHost.split(COMMA)[0];
        } else {
            localClientHost = httpServletRequest.getRemoteAddr();
        }

        final boolean agentExecution = this.jobExecutionModeSelector.executeWithAgent(jobRequest, httpServletRequest);
        final String jobId;
        if (agentExecution) {
            jobId = this.agentExecution(jobRequest, attachments, localClientHost, userAgent);
        } else {
            jobId = this.embeddedExecution(jobRequest, attachments, localClientHost, userAgent);
        }

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
    public EntityModel<Job> getJob(@PathVariable("id") final String id) throws GenieException {
        log.info("[getJob] Called for job with id: {}", id);
        return this.jobModelAssembler.toModel(this.persistenceService.getJob(id));
    }

    /**
     * Get the status of the given job if it exists.
     *
     * @param id The id of the job to get status for
     * @return The status of the job as one of: {@link JobStatus}
     * @throws NotFoundException When no job with {@literal id} exists
     */
    @GetMapping(value = "/{id}/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public JsonNode getJobStatus(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getJobStatus] Called for job with id: {}", id);
        final JsonNodeFactory factory = JsonNodeFactory.instance;
        return factory
            .objectNode()
            .set(
                "status",
                factory.textNode(DtoConverters.toV3JobStatus(this.persistenceService.getJobStatus(id)).toString())
            );
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
    public PagedModel<EntityModel<JobSearchResult>> findJobs(
        @RequestParam(value = "id", required = false) @Nullable final String id,
        @RequestParam(value = "name", required = false) @Nullable final String name,
        @RequestParam(value = "user", required = false) @Nullable final String user,
        @RequestParam(value = "status", required = false) @Nullable final Set<String> statuses,
        @RequestParam(value = "tag", required = false) @Nullable final Set<String> tags,
        @RequestParam(value = "clusterName", required = false) @Nullable final String clusterName,
        @RequestParam(value = "clusterId", required = false) @Nullable final String clusterId,
        @RequestParam(value = "commandName", required = false) @Nullable final String commandName,
        @RequestParam(value = "commandId", required = false) @Nullable final String commandId,
        @RequestParam(value = "minStarted", required = false) @Nullable final Long minStarted,
        @RequestParam(value = "maxStarted", required = false) @Nullable final Long maxStarted,
        @RequestParam(value = "minFinished", required = false) @Nullable final Long minFinished,
        @RequestParam(value = "maxFinished", required = false) @Nullable final Long maxFinished,
        @RequestParam(value = "grouping", required = false) @Nullable final String grouping,
        @RequestParam(value = "groupingInstance", required = false) @Nullable final String groupingInstance,
        @PageableDefault(sort = {"created"}, direction = Sort.Direction.DESC) final Pageable page,
        final PagedResourcesAssembler<JobSearchResult> assembler
    ) throws GenieException {
        log.info(
            "[getJobs] Called with "
                + "[id | jobName | user | statuses | clusterName "
                + "| clusterId | minStarted | maxStarted | minFinished | maxFinished | grouping | groupingInstance "
                + "| page]\n"
                + "{} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}",
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
        final Link self = WebMvcLinkBuilder
            .linkTo(
                WebMvcLinkBuilder
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

        return assembler.toModel(
            this.persistenceService.findJobs(
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
            this.jobSearchResultModelAssembler,
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
     * @throws GenieException    For any error
     * @throws NotFoundException When no job with {@literal id} exists
     * @throws IOException       on redirect error
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void killJob(
        @PathVariable("id") final String id,
        @RequestHeader(name = JobConstants.GENIE_FORWARDED_FROM_HEADER, required = false)
        @Nullable final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException, NotFoundException, IOException {
        log.info("[killJob] Called for job id: {}. Forwarded from: {}", id, forwardedFrom);

        if (this.persistenceService.getJobStatus(id).isFinished()) {
            // Job is already done no need to kill
            return;
        }

        // If forwarded from is null this request hasn't been forwarded at all. Check we're on the right node
        if (this.jobsProperties.getForwarding().isEnabled() && forwardedFrom == null) {
            final String jobHostname;
            try {
                jobHostname = this.getJobOwnerHostname(id, this.persistenceService.isV4(id));
            } catch (final GenieJobNotFoundException e) {
                throw new GenieNotFoundException("Job " + id + " not found", e);
            }

            if (!this.hostname.equals(jobHostname)) {
                log.info("Job {} is not on this node. Forwarding kill request to {}", id, jobHostname);
                final String forwardHost = this.buildForwardHost(jobHostname);
                try {
                    //Need to forward job
                    this.restTemplate.execute(
                        forwardHost + JOB_API_BASE_PATH + id,
                        HttpMethod.DELETE,
                        forwardRequest -> copyRequestHeaders(request, forwardRequest),
                        (final ClientHttpResponse forwardResponse) -> {
                            response.setStatus(HttpStatus.ACCEPTED.value());
                            copyResponseHeaders(response, forwardResponse);
                            return null;
                        }
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
    public EntityModel<JobRequest> getJobRequest(
        @PathVariable("id") final String id) throws GenieException {
        log.info("[getJobRequest] Called for job request with id {}", id);
        return this.jobRequestModelAssembler.toModel(this.persistenceService.getV3JobRequest(id));
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
    public EntityModel<JobExecution> getJobExecution(
        @PathVariable("id") final String id
    ) throws GenieException {
        log.info("[getJobExecution] Called for job execution with id {}", id);
        return this.jobExecutionModelAssembler.toModel(this.persistenceService.getJobExecution(id));
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
    public EntityModel<JobMetadata> getJobMetadata(@PathVariable("id") final String id) throws GenieException {
        log.info("[getJobMetadata] Called for job metadata with id {}", id);
        return this.jobMetadataModelAssembler.toModel(this.persistenceService.getJobMetadata(id));
    }

    /**
     * Get the cluster the job was run on or is currently running on.
     *
     * @param id The id of the job to get the cluster for
     * @return The cluster
     * @throws NotFoundException When either the job or the cluster aren't found
     */
    @GetMapping(value = "/{id}/cluster", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public EntityModel<Cluster> getJobCluster(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getJobCluster] Called for job with id {}", id);
        return this.clusterModelAssembler.toModel(DtoConverters.toV3Cluster(this.persistenceService.getJobCluster(id)));
    }

    /**
     * Get the command the job was run with or is currently running with.
     *
     * @param id The id of the job to get the command for
     * @return The command
     * @throws NotFoundException When either the job or the command aren't found
     */
    @GetMapping(value = "/{id}/command", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public EntityModel<Command> getJobCommand(@PathVariable("id") final String id) throws NotFoundException {
        log.info("[getJobCommand] Called for job with id {}", id);
        return this.commandModelAssembler.toModel(DtoConverters.toV3Command(this.persistenceService.getJobCommand(id)));
    }

    /**
     * Get the applications used ot run the job.
     *
     * @param id The id of the job to get the applications for
     * @return The applications
     * @throws NotFoundException When either the job or the applications aren't found
     */
    @GetMapping(value = "/{id}/applications", produces = MediaTypes.HAL_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public List<EntityModel<Application>> getJobApplications(
        @PathVariable("id") final String id
    ) throws NotFoundException {
        log.info("[getJobApplications] Called for job with id {}", id);

        return this.persistenceService
            .getJobApplications(id)
            .stream()
            .map(DtoConverters::toV3Application)
            .map(this.applicationModelAssembler::toModel)
            .collect(Collectors.toList());
    }

    /**
     * Get the job output directory.
     *
     * @param id            The id of the job to get output for
     * @param forwardedFrom The host this request was forwarded from if present
     * @param request       the servlet request
     * @param response      the servlet response
     * @throws NotFoundException When no job with {@literal id} exists
     * @throws GenieException    on any Genie internal error
     */
    @GetMapping(
        value = {
            "/{id}/output",
            "/{id}/output/",
            "/{id}/output/**"
        }
    )
    public void getJobOutput(
        @PathVariable("id") final String id,
        @RequestHeader(name = JobConstants.GENIE_FORWARDED_FROM_HEADER, required = false)
        @Nullable final String forwardedFrom,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException, NotFoundException {

        final String path = ControllerUtils.getRemainingPath(request);
        log.info("[getJobOutput] Called to get output path \"{}\" for job with id \"{}\"", path, id);

        final URL baseUrl;
        try {
            baseUrl = forwardedFrom == null
                ? ControllerUtils.getRequestRoot(request, path)
                : ControllerUtils.getRequestRoot(new URL(forwardedFrom), path);
        } catch (final MalformedURLException e) {
            throw new GenieServerException("Unable to parse base request url", e);
        }

        final ArchiveStatus archiveStatus = this.persistenceService.getJobArchiveStatus(id);

        if (archiveStatus == ArchiveStatus.PENDING) {
            final boolean isV4 = this.persistenceService.isV4(id);
            final String jobHostname;
            try {
                jobHostname = this.getJobOwnerHostname(id, isV4);
            } catch (NotFoundException e) {
                throw new GenieServerException("Failed to route request", e);
            }

            final boolean shouldForward = !this.hostname.equals(jobHostname);
            final boolean canForward = forwardedFrom == null && this.jobsProperties.getForwarding().isEnabled();

            if (shouldForward && canForward) {
                // Forward request to another node
                forwardRequest(id, path, jobHostname, request, response);
                return;
            } else if (!canForward && shouldForward) {
                // Should forward but can't
                throw new GenieServerException("Job files are not local, but forwarding is disabled");
            }
        }

        // In any other case, delegate the request to the service
        log.debug("Fetching requested resource \"{}\" for job \"{}\"", path, id);
        this.jobDirectoryServerService.serveResource(id, baseUrl, path, request, response);
    }

    private void forwardRequest(
        final String id,
        final String path,
        final String jobHostname,
        final HttpServletRequest request,
        final HttpServletResponse response
    ) throws GenieException {
        log.info("Job {} is not run on this node. Forwarding to {}", id, jobHostname);
        final String forwardHost = this.buildForwardHost(jobHostname);

        try {
            this.restTemplate.execute(
                forwardHost + JOB_API_BASE_PATH + id + "/output/" + path,
                HttpMethod.GET,
                forwardRequest -> copyRequestHeaders(request, forwardRequest),
                (ResponseExtractor<Void>) forwardResponse -> {
                    response.setStatus(forwardResponse.getStatusCode().value());
                    copyResponseHeaders(response, forwardResponse);
                    // Documentation I could find pointed to the HttpEntity reading the bytes off
                    // the stream so this should resolve memory problems if the file returned is large
                    ByteStreams.copy(forwardResponse.getBody(), response.getOutputStream());
                    return null;
                }
            );
        } catch (final HttpClientErrorException.NotFound e) {
            throw new GenieNotFoundException("Not Found (via: " + forwardHost + ")", e);
        } catch (final HttpStatusCodeException e) {
            throw new GenieException(e.getStatusCode().value(), "Proxied request failed: " + e.getMessage(), e);
        } catch (final Exception e) {
            log.error("Failed getting the remote job output from {}. Error: {}", forwardHost, e.getMessage());
            throw new GenieServerException("Proxied request error:" + e.getMessage(), e);
        }
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
        @NotBlank(message = "No job id entered. Unable to find the job owner.") final String jobId,
        final boolean isV4
    ) throws NotFoundException {
        if (isV4) {
            return this.agentRoutingService
                .getHostnameForAgentConnection(jobId)
                .orElseThrow(() -> new NotFoundException("No hostname found for v4 job - " + jobId));
        } else {
            return this.persistenceService.getJobHost(jobId);
        }
    }

    // TODO: Remove this once fully moved to agent execution
    private String embeddedExecution(
        final JobRequest jobRequest,
        @Nullable final MultipartFile[] attachments,
        final String clientHost,
        @Nullable final String userAgent
    ) throws GenieException {
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

            jobRequest.getCommandArgs().ifPresent(builder::withCommandArgs);
            jobRequest.getCpu().ifPresent(builder::withCpu);
            jobRequest.getMemory().ifPresent(builder::withMemory);
            jobRequest.getGroup().ifPresent(builder::withGroup);
            jobRequest.getSetupFile().ifPresent(builder::withSetupFile);
            jobRequest.getDescription().ifPresent(builder::withDescription);
            jobRequest.getEmail().ifPresent(builder::withEmail);
            jobRequest.getTimeout().ifPresent(builder::withTimeout);
            jobRequest.getMetadata().ifPresent(builder::withMetadata);
            jobRequest.getGrouping().ifPresent(builder::withGrouping);
            jobRequest.getGroupingInstance().ifPresent(builder::withGroupingInstance);

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
                log.info(
                    "Attachment for job: {} name: {} Size: {}",
                    jobId,
                    attachment.getOriginalFilename(),
                    attachment.getSize()
                );
                try {
                    String originalFilename = attachment.getOriginalFilename();
                    if (originalFilename == null) {
                        originalFilename = UUID.randomUUID().toString();
                    }
                    this.legacyAttachmentService.save(jobId, originalFilename, attachment.getInputStream());
                } catch (final IOException ioe) {
                    throw new GenieServerException("Failed to save job attachment", ioe);
                }
            }
        }

        final JobMetadata metadata = new JobMetadata
            .Builder()
            .withClientHost(clientHost)
            .withUserAgent(userAgent)
            .withNumAttachments(numAttachments)
            .withTotalSizeOfAttachments(totalSizeOfAttachments)
            .build();

        this.jobCoordinatorService.coordinateJob(jobRequestWithId, metadata);
        return jobId;
    }

    private String agentExecution(
        final JobRequest jobRequest,
        @Nullable final MultipartFile[] attachments,
        final String clientHost,
        @Nullable final String userAgent
    ) throws GenieException, GenieCheckedException {
        // Get attachments metadata
        int numAttachments = 0;
        long totalSizeOfAttachments = 0L;
        if (attachments != null) {
            numAttachments = attachments.length;
            for (final MultipartFile attachment : attachments) {
                totalSizeOfAttachments += attachment.getSize();
            }
        }

        final JobRequestMetadata metadata = new JobRequestMetadata(
            new ApiClientMetadata(clientHost, userAgent),
            null,
            numAttachments,
            totalSizeOfAttachments
        );

        final JobSubmission.Builder jobSubmissionBuilder = new JobSubmission.Builder(
            DtoConverters.toV4JobRequest(jobRequest),
            metadata
        );

        if (attachments != null) {
            jobSubmissionBuilder.withAttachments(
                this.attachmentService.saveAttachments(
                    jobRequest.getId().orElse(null),
                    Arrays
                        .stream(attachments)
                        .map(MultipartFile::getResource)
                        .collect(Collectors.toSet())
                )
            );
        }

        return this.jobLaunchService.launchJob(jobSubmissionBuilder.build());
    }
}
