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
package com.netflix.genie.web.dtos;


import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.dto.v4.JobRequest;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;

import java.util.Set;

/**
 * Represents a job that reached a final state (successful or not) and provides acceess to all available job details.
 *
 * @author mprimi
 * @since 4.0.0
 */

public class CompletedJobSummary {

    public static class Builder {
        private final JobStatus jobFinalStatus;
        private final JobRequest jobRequest;

        public Builder(
            final JobStatus jobFinalStatus,
            final JobRequest jobRequest
        ) {
            this.jobFinalStatus = jobFinalStatus;
            this.jobRequest = jobRequest;
        }

        public CompletedJobSummary build() {
            return new CompletedJobSummary();
        }

        public Builder withSpecification(final JobSpecification jobSpecification) {

            // TODO
            return this;
        }

        public Builder withCluster(final Cluster cluster) {
            return this;
        }

        public Builder withCommand(final Command command) {
            //TODO
            return this;
        }

        public Builder withApplications(final Set<Application> applications) {
            //TODO
            return this;
        }
    }
}
