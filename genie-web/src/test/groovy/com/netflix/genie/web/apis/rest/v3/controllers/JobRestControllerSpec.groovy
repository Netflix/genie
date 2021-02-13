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
package com.netflix.genie.web.apis.rest.v3.controllers

import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.exceptions.GenieServerUnavailableException
import com.netflix.genie.common.exceptions.GenieUserLimitExceededException
import com.netflix.genie.common.internal.util.GenieHostInfo
import com.netflix.genie.web.agent.services.AgentRoutingService
import com.netflix.genie.web.apis.rest.v3.hateoas.assemblers.EntityModelAssemblers
import com.netflix.genie.web.data.services.DataServices
import com.netflix.genie.web.data.services.PersistenceService
import com.netflix.genie.web.dtos.JobSubmission
import com.netflix.genie.web.properties.JobsActiveLimitProperties
import com.netflix.genie.web.properties.JobsProperties
import com.netflix.genie.web.services.AttachmentService
import com.netflix.genie.web.services.JobDirectoryServerService
import com.netflix.genie.web.services.JobKillService
import com.netflix.genie.web.services.JobLaunchService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.core.env.Environment
import org.springframework.web.client.RestTemplate
import spock.lang.Specification

import javax.servlet.http.HttpServletRequest

class JobRestControllerSpec extends Specification {
    JobRestController controller
    JobsProperties jobsProperties
    Environment environment
    PersistenceService persistenceService
    JobLaunchService jobLaunchService

    void setup() {
        this.jobsProperties = JobsProperties.getJobsPropertiesDefaults()
        this.environment = Mock(Environment)
        this.persistenceService = Mock(PersistenceService)
        this.jobLaunchService = Mock(JobLaunchService)

        this.controller = new JobRestController(
            jobLaunchService,
            new DataServices(this.persistenceService),
            Mock(EntityModelAssemblers),
            Mock(GenieHostInfo),
            Mock(RestTemplate),
            Mock(JobDirectoryServerService),
            jobsProperties,
            new SimpleMeterRegistry(),
            Mock(AgentRoutingService),
            environment,
            Mock(AttachmentService),
            Mock(JobKillService),
        )
    }

    def "Reject jobs due submit disabled"() {
        when:
        controller.submitJob(Mock(JobRequest), "1.2.3.4", "test-client", Mock(HttpServletRequest))

        then:
        1 * environment.getProperty("genie.jobs.submission.enabled", _, _) >> false
        thrown(GenieServerUnavailableException)
    }

    def "Reject jobs due to user limit exceeded"() {
        setup:
        JobRequest jobRequest = Mock(JobRequest)
        JobsActiveLimitProperties activeLimitProperties = jobsProperties.getActiveLimit()
        activeLimitProperties.setEnabled(true)
        activeLimitProperties.setCount(1)

        when:
        controller.submitJob(jobRequest, "1.2.3.4", "test-client", Mock(HttpServletRequest))

        then:
        1 * environment.getProperty("genie.jobs.submission.enabled", _, _) >> true
        1 * jobRequest.getUser() >> "user-name"
        1 * persistenceService.getActiveJobCountForUser("user-name") >> 1
        thrown(GenieUserLimitExceededException)
    }

    def "Check job request metadata"() {
        def request = Mock(HttpServletRequest)
        JobSubmission jobSubmission = null

        JobRequest jobRequest = new JobRequest.Builder(
            "name",
            "user",
            "version",
            [] as List,
            ["type:foo"] as Set
        ).build()

        when:
        controller.submitJob(jobRequest, "", "test-client", request)

        then:
        1 * environment.getProperty("genie.jobs.submission.enabled", _, _) >> true
        1 * request.getRemoteAddr() >> "8.8.8.8"
        1 * request.getHeaderNames() >> Collections.enumeration(["foo", "bar", "GENIE_FOO"])
        1 * request.getHeader("GENIE_FOO") >> "GENIE_BAR"
        1 * jobLaunchService.launchJob(_ as JobSubmission) >> {
            JobSubmission js ->
                jobSubmission = js
                throw new RuntimeException("End test")
        }
        thrown(RuntimeException)

        expect:
        jobSubmission != null
        jobSubmission.getJobRequestMetadata().getNumAttachments() == 0
        jobSubmission.getJobRequestMetadata().getTotalSizeOfAttachments() == 0
        !jobSubmission.getJobRequestMetadata().getAgentClientMetadata().isPresent()
        jobSubmission.getJobRequestMetadata().getApiClientMetadata().isPresent()
        jobSubmission.getJobRequestMetadata().getRequestHeaders() == [GENIE_FOO: "GENIE_BAR"]
    }
}
