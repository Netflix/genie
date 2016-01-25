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
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.core.services.ApplicationService;
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Encapsulates the details of the job, cluster , command and applications needed to run a job.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionEnvironment.class);

    private JobRequest jobRequest;
    private Cluster cluster;
    private Command command;
    private List<Application> applications = new ArrayList<>();
    private String jobWorkingDir;
    private String hostname;
    private int processId;

    /**
     * Initializes by Taking in a job request and figures out the cluster, command and applications
     * to run the job.
     *
     * @param clusterService implementation of ClusterService interface
     * @param commandService implementation of CommandService interface
     * @param applicationService implementation of ApplicationService interface
     * @param clusterLoadBalancer implementation of the ClusterLoadBalancer interface
     * @param jobReq The jobRequest object
     * @param genieBaseWorkingDir Base working directory for all genie jobs
     * @throws GenieException exception if there is an error
     */
    public void init(
        final ClusterService clusterService,
        final CommandService commandService,
        final ApplicationService applicationService,
        final ClusterLoadBalancer clusterLoadBalancer,
        @NotNull(message = "Cannot construct environment from null job request")
        final JobRequest jobReq,
        final String genieBaseWorkingDir
    ) throws GenieException {

        this.jobRequest = jobReq;
        final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
        enumStatuses.add(CommandStatus.ACTIVE);

        this.cluster = clusterLoadBalancer
                        .selectCluster(clusterService.chooseClusterForJobRequest(jobRequest));

        // Find the command for the job
        for (final Command cmd : clusterService.getCommandsForCluster(
                        this.cluster.getId(),
                        enumStatuses
        )) {
            if (cmd.getTags().containsAll(this.jobRequest.getCommandCriteria())) {
                this.command = cmd;
                break;
            }
        }

        //Avoiding NPE
        if (this.command == null) {
            final String msg = "No command found for params. Unable to continue.";
            LOG.error(msg);
            throw new GeniePreconditionException(msg);
        }

        this.applications.addAll(commandService.getApplicationsForCommand(this.command.getId()));
        this.jobWorkingDir = genieBaseWorkingDir + "/" + jobRequest.getId();
    }

    /**
     * Get the job information from the execution environment.
     *
     * @return the job
     */
    public JobRequest getJobRequest() {
        return jobRequest;
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
    public List<Application> getApplications() {

        return Collections.unmodifiableList(applications);
    }

    /**
     * Get the current working directory for the job to run.
     *
     * @return the working directory for the job
     */
    public String getJobWorkingDir() {
        return jobWorkingDir;
    }

    /**
     * Get the hostname on which the jobs runs.
     *
     * @return the hostname for the job
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Set the hostname of the Job to be run.
     *
     * @param hostname host on which the job runs
     */
    public void setHostname(final String hostname) {
        this.hostname = hostname;
    }

    /**
     * Get the pid of the Job running.
     *
     * @return the process id for the job
     */
    public int getProcessId() {
        return processId;
    }

    /**
     * Set the pid of the Job running.
     *
     * @param processId The pid for the job running.
     */
    public void setProcessId(final int processId) {
        this.processId = processId;
    }
}
