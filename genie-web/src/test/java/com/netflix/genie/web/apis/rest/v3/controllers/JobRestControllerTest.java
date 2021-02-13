/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieNotFoundException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.exceptions.GenieServerUnavailableException;
import com.netflix.genie.common.external.dtos.v4.ArchiveStatus;
import com.netflix.genie.common.external.dtos.v4.JobStatus;
import com.netflix.genie.common.internal.exceptions.checked.GenieCheckedException;
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
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.RootModelAssembler;
import com.netflix.genie.web.data.services.DataServices;
import com.netflix.genie.web.data.services.PersistenceService;
import com.netflix.genie.web.properties.JobsProperties;
import com.netflix.genie.web.services.AttachmentService;
import com.netflix.genie.web.services.JobDirectoryServerService;
import com.netflix.genie.web.services.JobKillService;
import com.netflix.genie.web.services.JobLaunchService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.mock.web.DelegatingServletOutputStream;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Unit tests for the Job rest controller.
 *
 * @author tgianos
 * @since 3.0.0
 */
// TODO move these tests over to JobRestControllerSpec
class JobRestControllerTest {

    //Mocked variables
    private AgentRoutingService agentRoutingService;
    private PersistenceService persistenceService;
    private String hostname;
    private RestTemplate restTemplate;
    private JobDirectoryServerService jobDirectoryServerService;
    private JobsProperties jobsProperties;
    private Environment environment;
    private JobKillService jobKillService;

    private JobRestController controller;

    private static Stream<Arguments> forwardJobOutputTestArguments() {
        final HttpHeaders headers = new HttpHeaders();
        return Stream.of(
            Arguments.of(
                HttpClientErrorException.create(HttpStatus.NOT_FOUND, "Not found", headers, null, null),
                GenieNotFoundException.class
            ),
            Arguments.of(
                HttpClientErrorException.create(HttpStatus.INTERNAL_SERVER_ERROR, "Error", headers, null, null),
                GenieException.class
            ),
            Arguments.of(
                new RuntimeException("..."),
                GenieException.class
            )
        );
    }

    @BeforeEach
    void setup() {
        this.persistenceService = Mockito.mock(PersistenceService.class);
        this.agentRoutingService = Mockito.mock(AgentRoutingService.class);
        this.hostname = UUID.randomUUID().toString();
        this.restTemplate = Mockito.mock(RestTemplate.class);
        this.jobDirectoryServerService = Mockito.mock(JobDirectoryServerService.class);
        this.jobsProperties = JobsProperties.getJobsPropertiesDefaults();
        this.environment = Mockito.mock(Environment.class);
        this.jobKillService = Mockito.mock(JobKillService.class);

        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        final Counter counter = Mockito.mock(Counter.class);
        Mockito.when(registry.counter(Mockito.anyString())).thenReturn(counter);

        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(this.persistenceService);

        this.controller = new JobRestController(
            Mockito.mock(JobLaunchService.class),
            dataServices,
            this.createMockResourceAssembler(),
            new GenieHostInfo(this.hostname),
            this.restTemplate,
            this.jobDirectoryServerService,
            this.jobsProperties,
            registry,
            this.agentRoutingService,
            this.environment,
            Mockito.mock(AttachmentService.class),
            this.jobKillService
        );
    }

    @Test
    void wontForwardJobOutputRequestIfNotEnabled() throws GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(false);

        final String jobId = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito
            .when(request.getRequestURL())
            .thenReturn(new StringBuffer("https://localhost:8443/api/v3/jobs/1234/output"));
        Mockito
            .doNothing()
            .when(this.jobDirectoryServerService)
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
        Mockito.when(this.persistenceService.getJobStatus(jobId)).thenReturn(JobStatus.RUNNING);

        this.controller.getJobOutput(jobId, null, request, response);

        Mockito.verify(this.agentRoutingService, Mockito.never()).getHostnameForAgentConnection(jobId);
        Mockito
            .verify(this.jobDirectoryServerService, Mockito.times(1))
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
    }

    @Test
    void wontForwardJobOutputRequestIfAlreadyForwarded() throws GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final String forwardedFrom = "https://localhost:8443/api/v3/jobs/1234/output";
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito
            .doNothing()
            .when(this.jobDirectoryServerService)
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
        Mockito.when(this.persistenceService.getJobStatus(jobId)).thenReturn(JobStatus.RUNNING);

        this.controller.getJobOutput(jobId, forwardedFrom, request, response);

        Mockito
            .verify(this.jobDirectoryServerService, Mockito.times(1))
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
    }

    @Test
    void wontForwardJobOutputRequestIfOnCorrectHost() throws GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito
            .when(request.getRequestURL())
            .thenReturn(new StringBuffer("https://" + this.hostname + "/api/v3/jobs/1234/output"));
        Mockito
            .doNothing()
            .when(this.jobDirectoryServerService)
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );

        Mockito.when(this.persistenceService.getJobArchiveStatus(jobId)).thenReturn(ArchiveStatus.PENDING);
        Mockito
            .when(this.agentRoutingService.getHostnameForAgentConnection(jobId))
            .thenReturn(Optional.of(this.hostname));

        this.controller.getJobOutput(jobId, null, request, response);

        Mockito.verify(this.agentRoutingService, Mockito.times(1)).getHostnameForAgentConnection(jobId);
        Mockito
            .verify(this.restTemplate, Mockito.never())
            .execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyString()
            );
        Mockito
            .verify(this.jobDirectoryServerService, Mockito.times(1))
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
    }

    @Test
    void wontThrow404ForJobOutputRequestIfAgentNotFound() throws GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito
            .when(request.getRequestURL())
            .thenReturn(new StringBuffer("https://" + this.hostname + "/api/v3/jobs/1234/output"));

        Mockito.when(this.persistenceService.getJobArchiveStatus(jobId)).thenReturn(ArchiveStatus.PENDING);
        Mockito.when(this.agentRoutingService.getHostnameForAgentConnection(jobId)).thenReturn(Optional.empty());

        Assertions.assertThatThrownBy(
            () -> this.controller.getJobOutput(jobId, null, request, response)
        ).isInstanceOf(GenieServerException.class);

        Mockito.verify(this.agentRoutingService, Mockito.times(1)).getHostnameForAgentConnection(Mockito.eq(jobId));
    }

    @Test
    void wontForwardJobOutputRequestIfForwardingRequestWasForwarded() throws GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito
            .when(request.getRequestURL())
            .thenReturn(new StringBuffer("https://" + this.hostname + "/api/v3/jobs/1234/output"));
        Mockito
            .doNothing()
            .when(this.jobDirectoryServerService)
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );

        Mockito.when(this.persistenceService.getJobArchiveStatus(jobId)).thenReturn(ArchiveStatus.PENDING);
        Mockito
            .when(this.agentRoutingService.getHostnameForAgentConnection(jobId))
            .thenReturn(Optional.of(UUID.randomUUID().toString()));

        Assertions.assertThatThrownBy(
            () -> this.controller.getJobOutput(jobId, "https://some-node:1234", request, response)
        ).isInstanceOf(GenieServerException.class);

        Mockito.verify(this.agentRoutingService, Mockito.times(1)).getHostnameForAgentConnection(jobId);
        Mockito
            .verify(this.restTemplate, Mockito.never())
            .execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.anyString(),
                Mockito.anyString()
            );
    }

    @ParameterizedTest(name = "Exception: {0} throws {1}")
    @MethodSource("forwardJobOutputTestArguments")
    void canHandleForwardJobOutputRequestWithError(
        final Throwable forwardingException,
        final Class<? extends Throwable> expectedException
    ) throws GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito
            .when(request.getRequestURL())
            .thenReturn(new StringBuffer("http://" + this.hostname + ":8080/api/v3/jobs/1234/output"));
        Mockito
            .doNothing()
            .when(this.jobDirectoryServerService)
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
        Mockito.when(this.persistenceService.getJobArchiveStatus(jobId)).thenReturn(ArchiveStatus.PENDING);

        final String jobHostName = UUID.randomUUID().toString();
        Mockito
            .when(this.agentRoutingService.getHostnameForAgentConnection(jobId))
            .thenReturn(Optional.of(jobHostName));

        //Mock parts of the http request
        final String http = "http";
        Mockito.when(request.getScheme()).thenReturn(http);
        final int port = 8080;
        Mockito.when(request.getServerPort()).thenReturn(port);
        final String requestURI = "/" + jobId + "/" + UUID.randomUUID().toString();
        Mockito.when(request.getRequestURI()).thenReturn(requestURI);
        Mockito.when(request.getHeaderNames()).thenReturn(null);

        Mockito.when(
            this.restTemplate.execute(
                Mockito.anyString(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any()
            )
        )
            .thenThrow(forwardingException);

        Assertions.assertThatThrownBy(
            () -> this.controller.getJobOutput(jobId, null, request, response)
        ).isInstanceOf(expectedException);

        Mockito.verify(this.agentRoutingService, Mockito.times(1)).getHostnameForAgentConnection(jobId);
        Mockito.verify(this.restTemplate, Mockito.times(1))
            .execute(
                Mockito.eq("http://" + jobHostName + ":8080/api/v3/jobs/" + jobId + "/output/"),
                Mockito.eq(HttpMethod.GET),
                Mockito.any(),
                Mockito.any()
            );

    }

    @Test
    void canHandleForwardJobOutputRequestWithSuccess() throws IOException, GenieException, GenieCheckedException {
        this.jobsProperties.getForwarding().setEnabled(true);
        final String jobId = UUID.randomUUID().toString();
        final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        final HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        final String jobHostName = UUID.randomUUID().toString();

        Mockito
            .when(request.getRequestURL())
            .thenReturn(new StringBuffer("http://localhost:8080/api/v3/jobs/1234/output"));
        Mockito
            .doNothing()
            .when(this.jobDirectoryServerService)
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
        Mockito.when(this.persistenceService.getJobArchiveStatus(jobId)).thenReturn(ArchiveStatus.PENDING);
        Mockito
            .when(this.agentRoutingService.getHostnameForAgentConnection(jobId))
            .thenReturn(Optional.of(jobHostName));

        //Mock parts of the http request
        final String http = "http";
        Mockito.when(request.getScheme()).thenReturn(http);
        final int port = 8080;
        Mockito.when(request.getServerPort()).thenReturn(port);
        final String requestURI = "/" + jobId + "/" + UUID.randomUUID().toString();
        Mockito.when(request.getRequestURI()).thenReturn(requestURI);

        final Set<String> headerNames = Sets.newHashSet(HttpHeaders.ACCEPT);
        Mockito.when(request.getHeaderNames()).thenReturn(Collections.enumeration(headerNames));
        Mockito.when(request.getHeader(HttpHeaders.ACCEPT)).thenReturn(MediaType.APPLICATION_JSON_VALUE);

        //Mock parts of forward response
        final HttpResponse forwardResponse = Mockito.mock(HttpResponse.class);
        final StatusLine statusLine = Mockito.mock(StatusLine.class);
        Mockito.when(forwardResponse.getStatusLine()).thenReturn(statusLine);
        final int successCode = 200;
        Mockito.when(statusLine.getStatusCode()).thenReturn(successCode);
        final Header contentTypeHeader = Mockito.mock(Header.class);
        Mockito.when(contentTypeHeader.getName()).thenReturn(HttpHeaders.CONTENT_TYPE);
        Mockito.when(contentTypeHeader.getValue()).thenReturn(MediaType.TEXT_PLAIN_VALUE);
        Mockito.when(forwardResponse.getAllHeaders()).thenReturn(new Header[]{contentTypeHeader});

        final String text = UUID.randomUUID().toString() + UUID.randomUUID().toString() + UUID.randomUUID().toString();
        final ByteArrayInputStream bis = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
        final HttpEntity entity = Mockito.mock(HttpEntity.class);
        Mockito.when(entity.getContent()).thenReturn(bis);
        Mockito.when(forwardResponse.getEntity()).thenReturn(entity);

        final DelegatingServletOutputStream bos = new DelegatingServletOutputStream(new ByteArrayOutputStream());
        Mockito.when(response.getOutputStream()).thenReturn(bos);

        final ClientHttpRequestFactory factory = Mockito.mock(ClientHttpRequestFactory.class);
        final ClientHttpRequest clientHttpRequest = Mockito.mock(ClientHttpRequest.class);
        Mockito.when(clientHttpRequest.execute())
            .thenReturn(new MockClientHttpResponse(text.getBytes(StandardCharsets.UTF_8), HttpStatus.OK));
        Mockito.when(clientHttpRequest.getHeaders())
            .thenReturn(new HttpHeaders());
        Mockito.when(factory.createRequest(Mockito.any(), Mockito.any())).thenReturn(clientHttpRequest);
        final RestTemplate template = new RestTemplate(factory);
        final MeterRegistry registry = Mockito.mock(MeterRegistry.class);
        final Counter counter = Mockito.mock(Counter.class);
        Mockito.when(registry.counter(Mockito.anyString())).thenReturn(counter);

        final DataServices dataServices = Mockito.mock(DataServices.class);
        Mockito.when(dataServices.getPersistenceService()).thenReturn(this.persistenceService);

        final JobRestController jobController = new JobRestController(
            Mockito.mock(JobLaunchService.class),
            dataServices,
            this.createMockResourceAssembler(),
            new GenieHostInfo(this.hostname),
            template,
            this.jobDirectoryServerService,
            this.jobsProperties,
            registry,
            this.agentRoutingService,
            this.environment,
            Mockito.mock(AttachmentService.class),
            this.jobKillService
        );
        jobController.getJobOutput(jobId, null, request, response);

        Assertions.assertThat(bos.getTargetStream().toString()).isEqualTo(text);
        Mockito.verify(request, Mockito.times(1)).getHeader(HttpHeaders.ACCEPT);
        Mockito.verify(this.agentRoutingService, Mockito.times(1)).getHostnameForAgentConnection(jobId);
        Mockito.verify(response, Mockito.never()).sendError(Mockito.anyInt());
        Mockito
            .verify(this.jobDirectoryServerService, Mockito.never())
            .serveResource(
                Mockito.eq(jobId),
                Mockito.any(URL.class),
                Mockito.anyString(),
                Mockito.eq(request),
                Mockito.eq(response)
            );
    }

    /**
     * Make sure when job submission is disabled it won't run the job and will return the proper error message.
     */
    @Test
    void whenJobSubmissionIsDisabledItThrowsCorrectError() {
        final String errorMessage = UUID.randomUUID().toString();
        Mockito.when(
            this.environment.getProperty(
                JobConstants.JOB_SUBMISSION_ENABLED_PROPERTY_KEY,
                Boolean.class,
                true
            )
        ).thenReturn(false);
        Mockito.when(
            this.environment.getProperty(
                JobConstants.JOB_SUBMISSION_DISABLED_MESSAGE_KEY,
                JobConstants.JOB_SUBMISSION_DISABLED_DEFAULT_MESSAGE
            )
        ).thenReturn(errorMessage);

        Assertions
            .assertThatExceptionOfType(GenieServerUnavailableException.class)
            .isThrownBy(() ->
                this.controller.submitJob(
                    Mockito.mock(JobRequest.class),
                    null,
                    null,
                    Mockito.mock(HttpServletRequest.class)
                ))
            .withMessage(errorMessage);
    }

    private EntityModelAssemblers createMockResourceAssembler() {
        return new EntityModelAssemblers(
            Mockito.mock(ApplicationModelAssembler.class),
            Mockito.mock(ClusterModelAssembler.class),
            Mockito.mock(CommandModelAssembler.class),
            Mockito.mock(JobExecutionModelAssembler.class),
            Mockito.mock(JobMetadataModelAssembler.class),
            Mockito.mock(JobRequestModelAssembler.class),
            Mockito.mock(JobModelAssembler.class),
            Mockito.mock(JobSearchResultModelAssembler.class),
            Mockito.mock(RootModelAssembler.class)
        );
    }
}
