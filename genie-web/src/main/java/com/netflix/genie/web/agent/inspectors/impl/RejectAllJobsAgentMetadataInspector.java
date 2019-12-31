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
package com.netflix.genie.web.agent.inspectors.impl;

import com.netflix.genie.common.internal.dtos.v4.AgentClientMetadata;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.agent.inspectors.AgentMetadataInspector;
import com.netflix.genie.web.agent.inspectors.InspectionReport;
import org.springframework.core.env.Environment;

import javax.validation.Valid;

/**
 * An {@link AgentMetadataInspector} that accepts or rejects all agents based on the value of an environment property.
 * This mechanism allows temporarily disabling all new inbound jobs while at the same time keeping the servers running.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class RejectAllJobsAgentMetadataInspector implements AgentMetadataInspector {
    private static final String JOB_SUBMISSION_IS_ENABLED_MESSAGE = "Job submission is enabled";
    private Environment environment;

    /**
     * Constructor.
     *
     * @param environment the environment
     */
    public RejectAllJobsAgentMetadataInspector(final Environment environment) {
        this.environment = environment;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InspectionReport inspect(@Valid final AgentClientMetadata agentClientMetadata) {
        final boolean jobSubmissionEnabled = this.environment.getProperty(
            JobConstants.JOB_SUBMISSION_ENABLED_PROPERTY_KEY,
            Boolean.class,
            true
        );

        if (jobSubmissionEnabled) {
            return new InspectionReport(
                InspectionReport.Decision.ACCEPT,
                JOB_SUBMISSION_IS_ENABLED_MESSAGE
            );
        } else {
            final String message = environment.getProperty(
                JobConstants.JOB_SUBMISSION_DISABLED_MESSAGE_KEY,
                String.class,
                JobConstants.JOB_SUBMISSION_DISABLED_DEFAULT_MESSAGE
            );
            return new InspectionReport(
                InspectionReport.Decision.REJECT,
                message
            );
        }
    }
}
