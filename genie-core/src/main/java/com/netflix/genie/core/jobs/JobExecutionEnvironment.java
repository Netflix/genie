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
package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Encapsulates the details of the job, cluster , command and applications needed to run a job.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Getter
public class JobExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionEnvironment.class);

    private JobRequest jobRequest;
    private Cluster cluster;
    private Command command;
    private List<Application> applications = new ArrayList<>();
    private String jobWorkingDir;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    public JobExecutionEnvironment(final Builder builder) {
        this.jobRequest = builder.jobRequest;
        this.cluster = builder.cluster;
        this.command = builder.command;
        this.applications.addAll(builder.applications);
        this.jobWorkingDir = builder.jobWorkingDir;
    }

    /**
     * A builder to create Job Execution Environment objects.
     *
     * @author tgianos
     * @since 3.0.0
     */
    public static class Builder {
        private JobRequest jobRequest;
        private Cluster cluster;
        private Command command;
        private List<Application> applications = new ArrayList<>();
        private String jobWorkingDir;

        /**
         * Set the jobRequest for the jobs' execution.
         *
         * @param request The job request object.
         * @return The builder
         */
        public Builder withJobRequest(final JobRequest request) {
            this.jobRequest = request;
            return this;
        }

        /**
         * Set the cluster for the jobs' execution.
         *
         * @param clusterObj The cluster object.
         * @return The builder
         */
        public Builder withCluster(final Cluster clusterObj) {
            this.cluster = clusterObj;
            return this;
        }

        /**
         * Set the command for the jobs' execution.
         *
         * @param commandObj The command object.
         * @return The builder
         */
        public Builder withCommand(final Command commandObj) {
            this.command = commandObj;
            return this;
        }

        /**
         * Set the applications needed for the jobs' execution.
         *
         * @param applicationList The list of application objects.
         * @return The builder
         */
        public Builder withApplications(final List<Application> applicationList) {
            if (applicationList != null) {
                this.applications.addAll(applicationList);
            }
            return this;
        }

        /**
         * Set the working directory for the jobs' execution.
         *
         * @param dir The directory location for the job
         * @return The builder
         */
        public Builder withJobWorkingDir(final String dir) {
            this.jobWorkingDir = dir;
            return this;
        }

        /**
         * Build the job execution environment object.
         *
         * @return Create the final read-only JobRequest instance
         * @throws GenieException If there is any problem.
         */
        public JobExecutionEnvironment build() throws GenieException {
            return new JobExecutionEnvironment(this);
        }
    }
}
