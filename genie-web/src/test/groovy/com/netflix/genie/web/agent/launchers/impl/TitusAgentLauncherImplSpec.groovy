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
import com.netflix.genie.common.internal.dtos.ComputeResources
import com.netflix.genie.common.internal.dtos.Image
import com.netflix.genie.common.internal.dtos.JobEnvironment
import com.netflix.genie.common.internal.dtos.JobMetadata
import com.netflix.genie.common.internal.dtos.JobSpecification
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

@SuppressWarnings("GroovyAccessibility")
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
    ComputeResources computeResources
    Image image

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
        this.requestedMemory = 1024L
        this.launcherProperties = new TitusAgentLauncherProperties()
        this.launcherProperties.setEndpoint(URI.create(TITUS_ENDPOINT_PREFIX))

        this.job = Mock(JobSpecification.ExecutionResource) {
            getId() >> JOB_ID
        }
        this.jobSpecification = Mock(JobSpecification) {
            getJob() >> job
        }
        this.jobMetadata = Mock(JobMetadata) {
            getUser() >> USER
        }
        this.computeResources = Mock(ComputeResources) {
            getCpu() >> Optional.of(this.requestedCPU)
            getGpu() >> Optional.empty()
            getMemoryMb() >> Optional.of(this.requestedMemory)
            getDiskMb() >> Optional.empty()
            getNetworkMbps() >> Optional.empty()
        }
        this.image = Mock(Image) {
            getName() >> Optional.empty()
            getTag() >> Optional.empty()
        }

        this.jobEnvironment = Mock(JobEnvironment) {
            getComputeResources() >> this.computeResources
            getImages() >> [(this.launcherProperties.getAgentImageKey()): this.image]
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
        requestCapture.getContainer().getResources().getCpu() == this.requestedCPU + 1
        requestCapture.getContainer().getResources().getGpu() == TitusAgentLauncherImpl.DEFAULT_JOB_GPU
        requestCapture.getContainer().getResources().getMemoryMB() == DataSize.ofGigabytes(4).toMegabytes()
        requestCapture.getContainer().getResources().getDiskMB() ==
            TitusAgentLauncherImpl.DEFAULT_JOB_DISK + this.launcherProperties.getAdditionalDiskSize().toMegabytes()
        requestCapture.getContainer().getResources().getNetworkMbps() == TitusAgentLauncherImpl.DEFAULT_JOB_NETWORK + this.launcherProperties.getAdditionalBandwidth().toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        requestCapture.getContainer().getSecurityProfile().getAttributes() == launcherProperties.getSecurityAttributes()
        requestCapture.getContainer().getSecurityProfile().getSecurityGroups() == launcherProperties.getSecurityGroups()
        requestCapture.getContainer().getSecurityProfile().getIamRole() == launcherProperties.getIAmRole()
        requestCapture.getContainer().getImage().getName() == launcherProperties.getImageName()
        requestCapture.getContainer().getImage().getTag() == launcherProperties.getImageTag()
        requestCapture.getContainer().getEntryPoint() == ["/bin/genie-agent"]
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
    def "Check memory allocation logic"() {
        if (envMinimumMemory != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_MEMORY_PROPERTY, envMinimumMemory)
        }
        if (envAdditionalMemory != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_MEMORY_PROPERTY, envAdditionalMemory)
        }
        this.launcherProperties.setMinimumMemory(minimumMemory)
        this.launcherProperties.setAdditionalMemory(additionalMemory)

        ComputeResources compute = new ComputeResources.Builder()
            .withMemoryMb((Long) jobRequestMemory)
            .build()
        ResolvedJob resolved = Mock(ResolvedJob) {
            getJobEnvironment() >> Mock(JobEnvironment) {
                getComputeResources() >> compute
            }
        }

        when:
        TitusBatchJobRequest.Resources resource = this.launcher.getTitusResources(resolved)

        then:
        resource.getMemoryMB() == expectedMemory

        where:
        jobRequestMemory                       | minimumMemory            | envMinimumMemory | additionalMemory          | envAdditionalMemory | expectedMemory
        DataSize.ofGigabytes(2L).toMegabytes() | DataSize.ofGigabytes(1L) | null             | DataSize.ofMegabytes(1L)  | null                | DataSize.ofGigabytes(2L).toMegabytes() + 1
        DataSize.ofGigabytes(1L).toMegabytes() | DataSize.ofGigabytes(2L) | null             | DataSize.ofMegabytes(1L)  | null                | DataSize.ofGigabytes(2L).toMegabytes()
        DataSize.ofGigabytes(2L).toMegabytes() | DataSize.ofGigabytes(1L) | "3GB"            | DataSize.ofMegabytes(1L)  | null                | DataSize.ofGigabytes(3L).toMegabytes()
        DataSize.ofGigabytes(3L).toMegabytes() | DataSize.ofGigabytes(1L) | "2GB"            | DataSize.ofMegabytes(1L)  | "10MB"              | DataSize.ofGigabytes(3L).toMegabytes() + 10
        null                                   | DataSize.ofGigabytes(1L) | null             | DataSize.ofMegabytes(10L) | "1MB"               | TitusAgentLauncherImpl.DEFAULT_JOB_MEMORY + 1
    }

    @Unroll
    def "Check cpu allocation logic"() {
        if (envMinimumCPU != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_CPU_PROPERTY, envMinimumCPU)
        }
        if (envAdditionalCPU != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_CPU_PROPERTY, envAdditionalCPU)
        }
        this.launcherProperties.setMinimumCPU(minimumCPU)
        this.launcherProperties.setAdditionalCPU(additionalCPU)

        ComputeResources compute = new ComputeResources.Builder()
            .withCpu(jobRequestCPU)
            .build()
        ResolvedJob resolved = Mock(ResolvedJob) {
            getJobEnvironment() >> Mock(JobEnvironment) {
                getComputeResources() >> compute
            }
        }

        when:
        TitusBatchJobRequest.Resources resource = this.launcher.getTitusResources(resolved)

        then:
        resource.getCpu() == expectedCPU

        where:
        jobRequestCPU | minimumCPU | envMinimumCPU | additionalCPU | envAdditionalCPU | expectedCPU
        2             | 1          | null          | 1             | null             | 3
        1             | 2          | null          | 1             | null             | 2
        2             | 1          | "3"           | 1             | null             | 3
        3             | 1          | "2"           | 1             | "2"              | 5
        null          | 1          | null          | 1             | null             | 2
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

        ComputeResources compute = new ComputeResources.Builder()
            .withDiskMb((Long) requestedDiskSize)
            .build()
        ResolvedJob resolved = Mock(ResolvedJob) {
            getJobEnvironment() >> Mock(JobEnvironment) {
                getComputeResources() >> compute
            }
        }

        when:
        TitusBatchJobRequest.Resources resource = this.launcher.getTitusResources(resolved)

        then:
        resource.getDiskMB() == expectedDiskSize

        where:
        requestedDiskSize                        | minimumDiskSize       | envMinimumDiskSize | additionalDiskSize    | envAdditionalDiskSize | expectedDiskSize
        null                                     | DataSize.parse("1GB") | null               | DataSize.parse("1MB") | null                  | TitusAgentLauncherImpl.DEFAULT_JOB_DISK + 1
        null                                     | DataSize.parse("2GB") | null               | DataSize.parse("1MB") | null                  | TitusAgentLauncherImpl.DEFAULT_JOB_DISK + 1
        null                                     | DataSize.parse("1GB") | "3GB"              | DataSize.parse("1MB") | null                  | TitusAgentLauncherImpl.DEFAULT_JOB_DISK + 1
        null                                     | DataSize.parse("1GB") | "2GB"              | DataSize.parse("1MB") | "10GB"                | TitusAgentLauncherImpl.DEFAULT_JOB_DISK + DataSize.ofGigabytes(10L).toMegabytes()
        null                                     | DataSize.parse("1GB") | "20GB"             | DataSize.parse("1MB") | null                  | DataSize.ofGigabytes(20L).toMegabytes()
        DataSize.ofGigabytes(100L).toMegabytes() | DataSize.parse("1GB") | "20GB"             | DataSize.parse("1MB") | null                  | DataSize.ofGigabytes(100L).toMegabytes() + 1
    }

    @Unroll
    def "Check network allocation logic"() {
        if (envMinimum != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_BANDWIDTH_PROPERTY, envMinimum)
        }
        if (envAdditional != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_BANDWIDTH_PROPERTY, envAdditional)
        }
        this.launcherProperties.setMinimumBandwidth(minimum)
        this.launcherProperties.setAdditionalBandwidth(additional)

        ComputeResources compute = new ComputeResources.Builder()
            .withNetworkMbps((Long) requested)
            .build()
        ResolvedJob resolved = Mock(ResolvedJob) {
            getJobEnvironment() >> Mock(JobEnvironment) {
                getComputeResources() >> compute
            }
        }

        when:
        TitusBatchJobRequest.Resources resource = this.launcher.getTitusResources(resolved)

        then:
        resource.getNetworkMbps() == expected

        where:
        requested                               | minimum               | envMinimum | additional            | envAdditional | expected
        null                                    | DataSize.parse("1GB") | null       | DataSize.parse("1MB") | null          | TitusAgentLauncherImpl.DEFAULT_JOB_NETWORK + DataSize.ofMegabytes(1L).toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        null                                    | DataSize.parse("2GB") | null       | DataSize.parse("1MB") | null          | DataSize.ofGigabytes(2L).toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        null                                    | DataSize.parse("1GB") | "3GB"      | DataSize.parse("1MB") | null          | DataSize.ofGigabytes(3L).toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        null                                    | DataSize.parse("1GB") | "2GB"      | DataSize.parse("1MB") | "10GB"        | TitusAgentLauncherImpl.DEFAULT_JOB_NETWORK + DataSize.ofGigabytes(10L).toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
        DataSize.ofMegabytes(32L).toMegabytes() | DataSize.parse("1MB") | "2MB"      | DataSize.parse("1MB") | null          | DataSize.ofMegabytes(32L).toMegabytes() + DataSize.ofMegabytes(1L).toMegabytes() * TitusAgentLauncherImpl.MEGABYTE_TO_MEGABIT
    }

    @Unroll
    def "Check gpu allocation logic"() {
        if (envMinimum != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.MINIMUM_GPU_PROPERTY, envMinimum)
        }
        if (envAdditional != null) {
            this.environment.withProperty(TitusAgentLauncherProperties.ADDITIONAL_GPU_PROPERTY, envAdditional)
        }
        this.launcherProperties.setMinimumGPU(minimum)
        this.launcherProperties.setAdditionalGPU(additional)

        ComputeResources compute = new ComputeResources.Builder()
            .withGpu((Integer) requested)
            .build()
        ResolvedJob resolved = Mock(ResolvedJob) {
            getJobEnvironment() >> Mock(JobEnvironment) {
                getComputeResources() >> compute
            }
        }

        when:
        TitusBatchJobRequest.Resources resource = this.launcher.getTitusResources(resolved)

        then:
        resource.getGpu() == expected

        where:
        requested | minimum | envMinimum | additional | envAdditional | expected
        null      | 1       | null       | 1          | null          | TitusAgentLauncherImpl.DEFAULT_JOB_GPU + 1
        null      | 4       | null       | 1          | null          | 4
        null      | 1       | "3"        | 1          | null          | 3
        null      | 1       | "2"        | 1          | "5"           | TitusAgentLauncherImpl.DEFAULT_JOB_GPU + 5
        7         | 1       | null       | 1          | null          | 8
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

    def "Check container attributes resolution"() {
        TitusBatchJobResponse response = toTitusResponse("{ \"id\" : \"" + TITUS_JOB_ID + "\" }")
        TitusBatchJobRequest requestCapture
        String dualstack = "Ipv6AndIpv4"
        String nonsense = "whatever"

        when:
        this.environment.withProperty(TitusAgentLauncherProperties.CONTAINER_NETWORK_CONFIGURATION, dualstack)
        this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        requestCapture != null
        requestCapture.getNetworkConfiguration() != null
        requestCapture.getNetworkConfiguration().getNetworkMode() == dualstack

        when:
        this.environment.withProperty(TitusAgentLauncherProperties.CONTAINER_NETWORK_CONFIGURATION, nonsense)
        this.launcher.launchAgent(this.resolvedJob, null)

        then:
        1 * this.restTemplate.postForObject(TITUS_ENDPOINT, _ as TitusBatchJobRequest, TitusBatchJobResponse.class) >> {
            args ->
                requestCapture = args[1] as TitusBatchJobRequest
                return response
        }
        1 * this.adapter.modifyJobRequest(_ as TitusBatchJobRequest, this.resolvedJob)
        requestCapture != null
        requestCapture.getNetworkConfiguration() == null
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
