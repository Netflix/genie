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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.benmanes.caffeine.cache.Cache
import com.netflix.genie.common.external.dtos.v4.JobEnvironment
import com.netflix.genie.common.external.dtos.v4.JobMetadata
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.dtos.TitusBatchJobRequest
import com.netflix.genie.web.dtos.TitusBatchJobResponse
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
import com.netflix.genie.web.properties.TitusAgentLauncherProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.util.unit.DataSize
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

    int requestedCPU
    long requestedMemory

    void setup() {

        requestedCPU = 3
        requestedMemory = 1024

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
            getCpu() >> {return requestedCPU}
            getMemory() >> {return requestedMemory}
        }
        this.resolvedJob = Mock(ResolvedJob) {
            getJobSpecification() >> jobSpecification
            getJobMetadata() >> jobMetadata
            getJobEnvironment() >> jobEnvironment
        }

        this.restTemplate = Mock(RestTemplate)
        this.cache = Mock(Cache)
        this.genieHostInfo = new GenieHostInfo("hostname")
        this.launcherProperties = new TitusAgentLauncherProperties()
        this.launcherProperties.setEndpoint(URI.create(TITUS_ENDPOINT_PREFIX))
        this.registry = new SimpleMeterRegistry()

        this.launcher = new TitusAgentLauncherImpl(
            restTemplate,
            cache,
            genieHostInfo,
            launcherProperties,
            registry
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

        expect:
        launcherExt.isPresent()
        requestCapture != null
        requestCapture.getOwner().getTeamEmail() == launcherProperties.getOwnerEmail()
        requestCapture.getApplicationName() == launcherProperties.getApplicationName()
        requestCapture.getCapacityGroup() == launcherProperties.getCapacityGroup()
        requestCapture.getAttributes().get("genie_user") == USER
        requestCapture.getAttributes().get("genie_source_host") == "hostname"
        requestCapture.getAttributes().get("genie_endpoint") == launcherProperties.getGenieServerHost()
        requestCapture.getAttributes().get("genie_job_id") == JOB_ID
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
            "/bin/genie-agent",
            "exec",
            "--api-job",
            "--launchInJobDirectory",
            "--job-id", JOB_ID,
            "--server-host", launcherProperties.getGenieServerHost(),
            "--server-port", launcherProperties.getGenieServerPort().toString()
        ]
        requestCapture.getContainer().getEnv() == launcherProperties.getAdditionalEnvironment()
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

}
