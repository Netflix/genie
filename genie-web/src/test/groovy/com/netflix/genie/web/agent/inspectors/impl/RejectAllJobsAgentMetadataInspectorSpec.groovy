/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.web.agent.inspectors.impl

import com.netflix.genie.common.internal.dtos.v4.AgentClientMetadata
import com.netflix.genie.common.internal.jobs.JobConstants
import com.netflix.genie.web.agent.inspectors.InspectionReport
import org.springframework.core.env.Environment
import spock.lang.Specification

class RejectAllJobsAgentMetadataInspectorSpec extends Specification {
    RejectAllJobsAgentMetadataInspector inspector
    Environment environment
    AgentClientMetadata agentClientMetadata

    void setup() {
        this.agentClientMetadata = Mock(AgentClientMetadata)
        this.environment = Mock(Environment)
        this.inspector = new RejectAllJobsAgentMetadataInspector(environment)
    }

    def "Inspect"() {
        String rejectMessage = "No!"
        InspectionReport inspectionReport

        when:
        inspectionReport = inspector.inspect(agentClientMetadata)

        then:
        1 * environment.getProperty(JobConstants.JOB_SUBMISSION_ENABLED_PROPERTY_KEY, Boolean, true) >> true
        inspectionReport.getDecision() == InspectionReport.Decision.ACCEPT
        inspectionReport.getMessage() == RejectAllJobsAgentMetadataInspector.JOB_SUBMISSION_IS_ENABLED_MESSAGE

        when:
        inspectionReport = inspector.inspect(agentClientMetadata)

        then:
        1 * environment.getProperty(JobConstants.JOB_SUBMISSION_ENABLED_PROPERTY_KEY, Boolean, true) >> false
        1 * environment.getProperty(JobConstants.JOB_SUBMISSION_DISABLED_MESSAGE_KEY, String, JobConstants.JOB_SUBMISSION_DISABLED_DEFAULT_MESSAGE) >> rejectMessage
        inspectionReport.getDecision() == InspectionReport.Decision.REJECT
        inspectionReport.getMessage() == rejectMessage
    }
}
