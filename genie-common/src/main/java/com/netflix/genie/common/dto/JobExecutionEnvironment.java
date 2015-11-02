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
package com.netflix.genie.common.dto;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates the details of the job, cluster , command and applications needed to run a job.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobExecutionEnvironment {

    @NotNull(message = "Job cannot be null. Needed for execution.")
    private Job job;

    @NotNull(message = "Cluster cannot be null. Need it to run job.")
    private Cluster cluster;

    @NotNull(message = "Command cannot null. Need it to run job.")
    private Command command;

    private List<Application> applicationList = new ArrayList<>();

    /**
     * Constructor used by the builder.
     *
     * @param builder The builder to get data from
     */
    protected JobExecutionEnvironment(
        final JobExecutionEnvironmentBuilder builder
    ) {
        this.job = builder.job;
        this.cluster = builder.cluster;
        this.command = builder.command;
        this.applicationList.clear();
        if (builder.applicationList != null) {
            this.applicationList.addAll(builder.applicationList);
        }
    }

    /**
     * Get the job information from the execution environment.
     *
     * @return the job
     */
    public Job getJob() {
        return job;
    }

    /**
     * Get the cluster information from the execution environment.
     *
     * @return the cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * Get the command information from the execution environment.
     *
     * @return the command
     */
    public Command getCommand() {
        return command;
    }

    /**
     * Get all the application information from the execution environment.
     *
     * @return the read-only list of applications
     */
    public List<Application> getApplicationList() {
        return Collections.unmodifiableList(applicationList);
    }

    /**
     * A builder to create job execution environment.
     *
     * @author amsharma
     * @since 3.0.0
     */
    public static class JobExecutionEnvironmentBuilder {
        private final Job job;
        private final Cluster cluster;
        private final Command command;
        private List<Application> applicationList = new ArrayList<>();

        /**
         * Constructor which has required fields.
         *
         * @param job        The details of the job to be executed
         * @param cluster        The details of the cluster on which job has to be executed
         * @param command     The details of the command which the job runs
         */
        public JobExecutionEnvironmentBuilder(
            final Job job,
            final Cluster cluster,
            final Command command
        ) {
            this.job = job;
            this.cluster = cluster;
            this.command = command;
        }

        /**
         * Set the job type for the command.
         *
         * @param applications list of applications that the command would need to execute
         * @return The builder
         */
        public JobExecutionEnvironmentBuilder withApplications(
                final List<Application> applications) {
            this.applicationList.clear();
            if (applications != null) {
                this.applicationList.addAll(applications);
            }
            return this;
        }

        /**
         * Build the JobExecutionEnvironment.
         *
         * @return Create the final read-only JobExecutionEnvironment instance
         */
        public JobExecutionEnvironment build() {
            return new JobExecutionEnvironment(this);
        }
    }
}
