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
package com.netflix.genie.web.jobs;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.internal.dto.v4.Application;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.exceptions.GenieException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates the details of the job, cluster , command and applications needed to run a job.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Getter
@Slf4j
public final class JobExecutionEnvironment {

    private final JobRequest jobRequest;
    private final Cluster cluster;
    private final Command command;
    private final List<Application> applications = new ArrayList<>();
    private final int memory;
    private final File jobWorkingDir;

    /**
     * Constructor used by the builder build() method.
     *
     * @param builder The builder to use
     */
    private JobExecutionEnvironment(final Builder builder) {
        this.jobRequest = builder.bJobRequest;
        this.cluster = builder.bCluster;
        this.command = builder.bCommand;
        this.applications.addAll(builder.bApplications);
        this.memory = builder.bMemory;
        this.jobWorkingDir = builder.bJobWorkingDir;
    }

    /**
     * Get the applications for this instance.
     *
     * @return A list of applications for this instance.
     */
    public List<Application> getApplications() {
        return Collections.unmodifiableList(this.applications);
    }

    /**
     * A builder to create Job Execution Environment objects.
     *
     * @author amsharma
     * @since 3.0.0
     */
    public static class Builder {
        private final JobRequest bJobRequest;
        private final Cluster bCluster;
        private final Command bCommand;
        private final List<Application> bApplications = new ArrayList<>();
        private final int bMemory;
        private final File bJobWorkingDir;

        /**
         * Constructor.
         *
         * @param request    The job request object.
         * @param clusterObj The cluster object.
         * @param commandObj The command object.
         * @param memory     The amount of memory (in MB) to use to run the job
         * @param dir        The directory location for this job.
         */
        public Builder(
            @NotNull(message = "Job Request cannot be null")
            final JobRequest request,
            @NotNull(message = "Cluster cannot be null")
            final Cluster clusterObj,
            @NotNull(message = "Command cannot be null")
            final Command commandObj,
            @Min(value = 1, message = "Amount of memory can't be less than 1 MB")
            final int memory,
            @NotBlank(message = "Job working directory cannot be empty")
            final File dir
        ) {
            this.bJobRequest = request;
            this.bCluster = clusterObj;
            this.bCommand = commandObj;
            this.bMemory = memory;
            this.bJobWorkingDir = dir;
        }

        /**
         * Set the applications needed for the jobs' execution.
         *
         * @param applications The list of application objects.
         * @return The builder
         */
        public Builder withApplications(final List<Application> applications) {
            this.bApplications.clear();
            if (applications != null) {
                this.bApplications.addAll(applications);
            }
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
