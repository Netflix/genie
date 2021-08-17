/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.web.agent.launchers.impl

import brave.Span
import brave.Tracer
import brave.propagation.TraceContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.benmanes.caffeine.cache.Cache
import com.netflix.genie.common.external.dtos.v4.JobEnvironment
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents
import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.agent.launchers.dtos.TitusBatchJobRequest
import com.netflix.genie.web.agent.launchers.dtos.TitusBatchJobResponse
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
import com.netflix.genie.web.properties.TitusAgentLauncherProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.http.HttpStatus
import org.springframework.mock.env.MockEnvironment
import org.springframework.retry.backoff.FixedBackOffPolicy
import org.springframework.retry.policy.NeverRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.util.unit.DataSize
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.RestClientException
import org.springframework.web.client.RestTemplate
import spock.lang.Specification
import spock.lang.Unroll

class TitusAgentLauncherImplSpec extends Specification {

    public static final String TITUS_ENDPOINT_PREFIX = "https://example-titus-endpoint.tld:1234"
    public static final String TITUS_PATH = "/api/v3/jobs"
    public static final String TITUS_ENDPOINT = TITUS_ENDPOINT_PREFIX + TITUS_PATH
    public static final String JOB_ID = "job-id"
    public static final String USER = "job-genie-user"
    public static final String TITUS_JOB_ID = UUID.randomUUID().toString()

    JobSpecification.ExecutionResource job
    JobSpecification jobSpecification
    ResolvedJob resolvedJob
    JobMetadata jobMetadata
    JobEnvironment jobEnvironment

    RestTemplate restTemplate
    Cache cache
    GenieHostInfo genieHostInfo
    TitusAgentLauncherProperties launcherProperties
    SimpleMeterRegistry registry
    TitusAgentLauncherImpl launcher
    MockEnvironment environment
    TitusAgentLauncherImpl.TitusJobRequestAdapter adapter

    Tracer tracer
    BraveTracePropagator tracePropagator
    Span currentSpan

    int requestedCPU
    long requestedMemory

    void setup() {
        this.requestedCPU = 3
        this.requestedMemory = 1024

        this.job = Mock(JobSpecification.ExecutionResource) {
            getId() >> JOB_ID
        }
        this.jobSpecification = Mock(JobSpecification) {
            getJob() >> job
        }
        this.jobMetadata = Mock(JobMetadata) {
            getUser() >> USER
        }
        this.jobEnvironment = Mock(JobEnvironment) {
            getCpu() >> { return requestedCPU }
            getMemory() >> { return requestedMemory }
        }
        this.resolvedJob = Mock(ResolvedJob) {
            getJobSpecification() >> jobSpecification
            getJobMetadata() >> jobMetadata
            getJobEnvironment() >> jobEnvironment
        }
        this.adapter = Mock(TitusAgentLauncherImpl.TitusJobRequestAdapter)

        this.restTemplate = Mock(RestTemplate)
        this.cache = Mock(Cache)
        this.genieHostInfo = new GenieHostInfo("hostname")
        this.launcherProperties = new TitusAgentLauncherProperties()
        this.launcherProperties.setEndpoint(URI.create(TITUS_ENDPOINT_PREFIX))
        this.registry = new SimpleMeterRegistry()
        this.environment = new MockEnvironment()
        this.tracer = Mock(Tracer)
        this.tracePropagator = Mock(BraveTracePropagator)

        this.tracer = Mock(Tracer)
        this.tracePropagator = Mock(BraveTracePropagator)
        UUID trace = UUID.randomUUID()
        this.currentSpan = Mock(Span) {
            context() >> TraceContext.newBuilder()
                .traceIdHigh(trace.getMostSignificantBits())
                .traceId(trace.getLeastSignificantBits())
                .spanId(UUID.randomUUID().getLeastSignificantBits())
                .sampled(true)
                .build()
        }

        /*
         *  Note: The retry template used here is "transparent" in the sense it doesn't do
         *        Anything. We don't really want to test how retries work as that is up to
         *        the configured retry policy and backoff not so much the business logic
         *        of this class. For now this is fine
         */
        def retryTemplate = new RetryTemplate()
        retryTemplate.setRetryPolicy(new NeverRetryPolicy())

        this.launcher = new TitusAgentLauncherImpl(
            this.restTemplate,
            retryTemplate,
            this.adapter,
            this.cache,
            this.genieHostInfo,
            this.launcherProperties,
            new BraveTracingComponents(
                this.tracer,
                this.tracePropagator,
                Mock(BraveTracingCleanup),
                Mock(BraveTagAdapter)
            ),
            this.environment,
            this.registry
        )
    }

    def "Launch successfully"() {
        setup:
        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * cache.put(JOB_ID, TITUS_JOB_ID)
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * this.tracer.currentSpan() >> this.currentSpan
        1 * this.tracePropagator.injectForAgent(_ as TraceContext) >> new HashMap<>()

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getOwner().getTeamEmail() == launcherProperties.getOwnerEmail()
        requestCapture.getApplicationName() == launcherProperties.getApplicationName()
        requestCapture.getCapacityGroup() == launcherProperties.getCapacityGroup()
        requestCapture.getAttributes().get("genie.user") == USER
        requestCapture.getAttributes().get("genie.sourceHost") == "hostname"
        requestCapture.getAttributes().get("genie.endpoint") == launcherProperties.getGenieServerHost()
        requestCapture.getAttributes().get("genie.jobId") == JOB_ID
        requestCapture.getContainer().getResources().getCpu() == 3 + 1
        requestCapture.getContainer().getResources().getGpu() == 0
        requestCapture.getContainer().getResources().getMemoryMB() == DataSize.ofGigabytes(4).toMegabytes()
        requestCapture.getContainer().getResources().getDiskMB() == DataSize.ofGigabytes(10).toMegabytes()
        requestCapture.getContainer().getResources().getNetworkMbps() == DataSize.ofMegabytes(7).toMegabytes() * 8
        requestCapture.getContainer().getSecurityProfile().getAttributes() == launcherProperties.getSecurityAttributes()
        requestCapture.getContainer().getSecurityProfile().getSecurityGroups() == launcherProperties.getSecurityGroups()
        requestCapture.getContainer().getSecurityProfile().getIamRole() == launcherProperties.getIAmRole()
        requestCapture.getContainer().getImage().getName() == launcherProperties.getImageName()
        requestCapture.getContainer().getImage().getTag() == launcherProperties.getImageTag()
        requestCapture.getContainer().getEntryPoint() == [
            "/bin/genie-agent"
        ]
        requestCapture.getContainer().getCommand() == [
            "exec",
            "--api-job",
            "--launchInJobDirectory",
            "--job-id", JOB_ID,
            "--server-host", launcherProperties.getGenieServerHost(),
            "--server-port", launcherProperties.getGenieServerPort().toString()
        ]
        requestCapture.getContainer().getEnv() == launcherProperties.getAdditionalEnvironment()
        requestCapture.getContainer().getAttributes() == this.launcherProperties.getContainerAttributes()
        requestCapture.getBatch().getSize() == 1
        requestCapture.getBatch().getRetryPolicy().getImmediate().getRetries() == launcherProperties.getRetries()
        requestCapture.getBatch().getRuntimeLimitSec() == launcherProperties.getRuntimeLimit().getSeconds()
        requestCapture.getDisruptionBudget().getSelfManaged().getRelocationTimeMs() == launcherProperties.getRuntimeLimit().toMillis()
    }

    @Unroll
    def "Titus client error: #response"() {
        when:
        launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            return response
        }
        1 * cache.put(JOB_ID, "-")
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        thrown(AgentLaunchException)

        where:
        _ | response
        _ | toTitusResponse("{ \"id\" : \"\" }")
        _ | toTitusResponse("{ \"statusCode\" : 500, \"message\" : \"Internal error\" }")
        _ | toTitusResponse("{ }")
        _ | null
    }

    @Unroll
    def "Titus client exception: #exception"() {
        when:
        launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            throw exception
        }
        1 * cache.put(JOB_ID, "-")
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        thrown(AgentLaunchException)

        where:
        _ | exception
        _ | new RestClientException("...")
        _ | new IOException("...")
        _ | new RuntimeException("...")
    }

    private static TitusBatchJobResponse toTitusResponse(String s) {
        return new ObjectMapper().readValue(s, TitusBatchJobResponse.class)
    }

    @Unroll
    def "Check resources allocation: #description"() {
        setup:
        this.requestedCPU = reqCPU
        this.requestedMemory = reqMem
        this.launcherProperties.setAdditionalBandwidth(DataSize.ofMegabytes(addlBw))
        this.launcherProperties.setAdditionalCPU(addlCPU)
        this.launcherProperties.setAdditionalDiskSize(DataSize.ofGigabytes(addlDisk))
        this.launcherProperties.setAdditionalGPU(addlGPU)
        this.launcherProperties.setAdditionalMemory(DataSize.ofGigabytes(addlMem))
        this.launcherProperties.setMinimumBandwidth(DataSize.ofMegabytes(minBw))
        this.launcherProperties.setMinimumCPU(minCPU)
        this.launcherProperties.setMinimumDiskSize(DataSize.ofGigabytes(minDisk))
        this.launcherProperties.setMinimumGPU(minGPU)
        this.launcherProperties.setMinimumMemory(DataSize.ofGigabytes(minMem))

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getResources().getCpu() == 4
        requestCapture.getContainer().getResources().getGpu() == 2
        requestCapture.getContainer().getResources().getMemoryMB() == DataSize.ofGigabytes(10).toMegabytes()
        requestCapture.getContainer().getResources().getDiskMB() == DataSize.ofGigabytes(20).toMegabytes()
        requestCapture.getContainer().getResources().getNetworkMbps() == DataSize.ofMegabytes(7).toMegabytes() * 8

        where:
        reqMem | reqCPU | addlBw | addlCPU | addlDisk | addlGPU | addlMem | minBw | minCPU | minDisk | minGPU | minMem | description
        512    | 1      | 1      | 1       | 1        | 1       | 1       | 7     | 4      | 20      | 2      | 10     | "Fall back to minimum values"
        1024   | 3      | 7      | 1       | 20       | 2       | 9       | 1     | 1      | 10      | 0      | 4      | "Use requested plus additional"
    }

    @Unroll
    def "Check memory allocation logic"() {
        if (envMinimumMemory != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_MEMORY_PROPERTY, envMinimumMemory)
        }
        if (envAdditionalMemory != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_MEMORY_PROPERTY, envAdditionalMemory)
        }
        this.requestedMemory = jobRequestMemory
        this.launcherProperties.setMinimumMemory(minimumMemory)
        this.launcherProperties.setAdditionalMemory(additionalMemory)

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getResources().getMemoryMB() == expectedMemory

        where:
        jobRequestMemory                    | minimumMemory         | envMinimumMemory | additionalMemory      | envAdditionalMemory | expectedMemory
        DataSize.parse("2GB").toMegabytes() | DataSize.parse("1GB") | null             | DataSize.parse("1MB") | null                | DataSize.parse("2GB").toMegabytes() + 1
        DataSize.parse("1GB").toMegabytes() | DataSize.parse("2GB") | null             | DataSize.parse("1MB") | null                | DataSize.parse("2GB").toMegabytes()
        DataSize.parse("2GB").toMegabytes() | DataSize.parse("1GB") | "3GB"            | DataSize.parse("1MB") | null                | DataSize.parse("3GB").toMegabytes()
        DataSize.parse("3GB").toMegabytes() | DataSize.parse("1GB") | "2GB"            | DataSize.parse("1MB") | "10MB"              | DataSize.parse("3GB").toMegabytes() + 10
    }

    @Unroll
    def "Check cpu allocation logic"() {
        if (envMinimumCPU != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_CPU_PROPERTY, envMinimumCPU)
        }
        if (envAdditionalCPU != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_CPU_PROPERTY, envAdditionalCPU)
        }
        this.requestedCPU = jobRequestCPU
        this.launcherProperties.setMinimumCPU(minimumCPU)
        this.launcherProperties.setAdditionalCPU(additionalCPU)

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getResources().getCpu() == expectedCPU

        where:
        jobRequestCPU | minimumCPU | envMinimumCPU | additionalCPU | envAdditionalCPU | expectedCPU
        2             | 1          | null          | 1             | null             | 3
        1             | 2          | null          | 1             | null             | 2
        2             | 1          | "3"           | 1             | null             | 3
        3             | 1          | "2"           | 1             | "2"              | 5
    }

    @Unroll
    def "Check disk size allocation logic"() {
        if (envMinimumDiskSize != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_DISK_SIZE_PROPERTY, envMinimumDiskSize)
        }
        if (envAdditionalDiskSize != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_DISK_SIZE_PROPERTY, envAdditionalDiskSize)
        }
        this.launcherProperties.setMinimumDiskSize(minimumDiskSize)
        this.launcherProperties.setAdditionalDiskSize(additionalDiskSize)

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getResources().getDiskMB() == expectedDiskSize

        where:
        minimumDiskSize       | envMinimumDiskSize | additionalDiskSize    | envAdditionalDiskSize | expectedDiskSize
        DataSize.parse("1GB") | null               | DataSize.parse("1MB") | null                  | DataSize.parse("1GB").toMegabytes()
        DataSize.parse("2GB") | null               | DataSize.parse("1MB") | null                  | DataSize.parse("2GB").toMegabytes()
        DataSize.parse("1GB") | "3GB"              | DataSize.parse("1MB") | null                  | DataSize.parse("3GB").toMegabytes()
        DataSize.parse("1GB") | "2GB"              | DataSize.parse("1MB") | "10GB"                | DataSize.parse("10GB").toMegabytes()
    }

    @Unroll
    def "Check network allocation logic"() {
        if (envMinimumBandwidth != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_BANDWIDTH_PROPERTY, envMinimumBandwidth)
        }
        if (envAdditionalBandwidth != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_BANDWIDTH_PROPERTY, envAdditionalBandwidth)
        }
        this.launcherProperties.setMinimumBandwidth(minimumBandwidth)
        this.launcherProperties.setAdditionalBandwidth(additionalBandwidth)

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getResources().getNetworkMbps() == expectedBandwidth

        where:
        minimumBandwidth      | envMinimumBandwidth | additionalBandwidth   | envAdditionalBandwidth | expectedBandwidth
        DataSize.parse("1GB") | null                | DataSize.parse("1MB") | null                   | DataSize.parse("1GB").toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        DataSize.parse("2GB") | null                | DataSize.parse("1MB") | null                   | DataSize.parse("2GB").toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        DataSize.parse("1GB") | "3GB"               | DataSize.parse("1MB") | null                   | DataSize.parse("3GB").toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        DataSize.parse("1GB") | "2GB"               | DataSize.parse("1MB") | "10GB"                 | DataSize.parse("10GB").toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
    }

    @Unroll
    def "Check gpu allocation logic"() {
        if (envMinimumGPU != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_GPU_PROPERTY, envMinimumGPU)
        }
        if (envAdditionalGPU != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_GPU_PROPERTY, envAdditionalGPU)
        }
        this.launcherProperties.setMinimumGPU(minimumGPU)
        this.launcherProperties.setAdditionalGPU(additionalGPU)

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getResources().getGpu() == expectedGPU

        where:
        minimumGPU | envMinimumGPU | additionalGPU | envAdditionalGPU | expectedGPU
        1          | null          | 1             | null             | 1
        2          | null          | 1             | null             | 2
        1          | "3"           | 1             | null             | 3
        1          | "2"           | 1             | "5"              | 5
    }

    @Unroll
    def "Check image and retry resolution"() {
        if (envImageName != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.IMAGE_NAME_PROPERTY, envImageName)
        }
        if (envImageTag != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.IMAGE_TAG_PROPERTY, envImageTag)
        }
        if (envRetries != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.RETRIES_PROPERTY, envRetries)
        }
        this.launcherProperties.setImageName(imageName)
        this.launcherProperties.setImageTag(imageTag)
        this.launcherProperties.setRetries(retries)

        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = launcher.launchAgent(resolvedJob, null)

        then:
        1 * restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * cache.put(JOB_ID, TITUS_JOB_ID)

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getImage().getName() == expectedImageName
        requestCapture.getContainer().getImage().getTag() == expectedImageTag
        requestCapture.getBatch().getRetryPolicy().getImmediate().getRetries() == expectedRetries

        where:
        imageName     | envImageName          | imageTag         | envImageTag | retries | envRetries | expectedImageName     | expectedImageTag | expectedRetries
        "genie-agent" | null                  | "latest.release" | null        | 4       | null       | "genie-agent"         | "latest.release" | 4
        "genie-agent" | "genie-agent-tgianos" | "latest.release" | "4.0.82"    | 4       | "1"        | "genie-agent-tgianos" | "4.0.82"         | 1
    }

    def "Check additional environment resolution"() {
        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * this.cache.put(JOB_ID, TITUS_JOB_ID)
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getEnv().isEmpty()

        when:
        def prop1Key = "${UUID.randomUUID()}.${UUID.randomUUID()}.${UUID.randomUUID()}".toString()
        def prop1Value = UUID.randomUUID().toString()
        this.environment.withProperty(
            "${TitusAgentLauncherProperties.ADDITIONAL_ENVIRONMENT_PROPERTY}.${prop1Key}",
            prop1Value
        )
        launcherExt = this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * this.cache.put(JOB_ID, TITUS_JOB_ID)
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getEnv().size() == 1
        requestCapture.getContainer().getEnv().get(prop1Key) == prop1Value

        when:
        def prop2Key = UUID.randomUUID().toString()
        def prop2Value = UUID.randomUUID().toString()
        this.environment.withProperty(
            "${TitusAgentLauncherProperties.ADDITIONAL_ENVIRONMENT_PROPERTY}.${prop2Key}".toString(),
            prop2Value
        )
        launcherExt = this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * this.cache.put(JOB_ID, TITUS_JOB_ID)
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getEnv().size() == 2
        requestCapture.getContainer().getEnv().get(prop1Key) == prop1Value
        requestCapture.getContainer().getEnv().get(prop2Key) == prop2Value
    }

    def "Check container attributes resolution"() {
        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture

        when:
        Optional<JsonNode> launcherExt = this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * this.cache.put(JOB_ID, TITUS_JOB_ID)
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getAttributes().isEmpty()

        when:
        def prop1Key = "${UUID.randomUUID()}.${UUID.randomUUID()}.${UUID.randomUUID()}".toString()
        def prop1Value = UUID.randomUUID().toString()
        def prop2Key = "${UUID.randomUUID()}-${UUID.randomUUID()}".toString()
        def prop2Value = UUID.randomUUID().toString()
        this.environment.withProperty(
            "${TitusAgentLauncherProperties.CONTAINER_ATTRIBUTES_PROPERTY}.${prop1Key}",
            prop1Value
        )
        this.environment.withProperty(
            "${TitusAgentLauncherProperties.CONTAINER_ATTRIBUTES_PROPERTY}.${prop2Key}",
            prop2Value
        )
        launcherExt = this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        1 * this.cache.put(JOB_ID, TITUS_JOB_ID)
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getContainer().getAttributes().size() == 2
        requestCapture.getContainer().getAttributes().get(prop1Key) == prop1Value
        requestCapture.getContainer().getAttributes().get(prop2Key) == prop2Value
    }

    def "Retry policy works as expected"() {
        def retryCodes = EnumSet.of(HttpStatus.REQUEST_TIMEOUT, HttpStatus.SERVICE_UNAVAILABLE)
        def maxRetries = 2
        def policy = new TitusAgentLauncherImpl.TitusAPIRetryPolicy(retryCodes, maxRetries)
        def retryTemplate = new RetryTemplate()
        def backoffPolicy = new FixedBackOffPolicy()
        backoffPolicy.setBackOffPeriod(1L)
        retryTemplate.setRetryPolicy(policy)
        retryTemplate.setBackOffPolicy(backoffPolicy)
        def mockResponse = Mock(TitusBatchJobResponse)

        when:
        retryTemplate.execute(
            { arg ->
                this.restTemplate.postForObject(TITUS_ENDPOINT, Mock(TitusBatchJobRequest), TitusBatchJobResponse.class)
            }
        )

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR)
        }
        thrown(HttpServerErrorException)

        when:
        retryTemplate.execute(
            { arg ->
                this.restTemplate.postForObject(TITUS_ENDPOINT, Mock(TitusBatchJobRequest), TitusBatchJobResponse.class)
            }
        )

        then:
        2 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            throw new HttpClientErrorException(HttpStatus.REQUEST_TIMEOUT)
        }
        thrown(HttpClientErrorException)

        when:
        def response = retryTemplate.execute(
            { arg ->
                this.restTemplate.postForObject(TITUS_ENDPOINT, Mock(TitusBatchJobRequest), TitusBatchJobResponse.class)
            }
        )

        then:
        2 * this.restTemplate.postForObject(
            TITUS_ENDPOINT,
            _ as TitusBatchJobRequest,
            TitusBatchJobResponse.class
        ) >>> [] >> { throw new HttpClientErrorException(HttpStatus.REQUEST_TIMEOUT) } >> mockResponse
        response == mockResponse
        noExceptionThrown()

        when:
        retryTemplate.execute(
            { arg ->
                this.restTemplate.postForObject(TITUS_ENDPOINT, Mock(TitusBatchJobRequest), TitusBatchJobResponse.class)
            }
        )

        then:
        2 * this.restTemplate.postForObject(
            TITUS_ENDPOINT,
            _ as TitusBatchJobRequest,
            TitusBatchJobResponse.class
        ) >>> [

        ] >> {
            throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE)
        } >> {
            throw new HttpClientErrorException(HttpStatus.REQUEST_TIMEOUT)
        }
        thrown(HttpClientErrorException)
    }
}
