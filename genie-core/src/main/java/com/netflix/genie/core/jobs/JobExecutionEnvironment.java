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
import com.netflix.genie.core.services.ClusterLoadBalancer;
import com.netflix.genie.core.services.ClusterService;
import com.netflix.genie.core.services.CommandService;
import com.netflix.genie.core.services.ApplicationService;

import javax.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * Encapsulates the details of the job, cluster , command and applications needed to run a job.
 *
 * @author amsharma
 * @since 3.0.0
 */
public class JobExecutionEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionEnvironment.class);

    private final ClusterService clusterService;
    private final CommandService commandService;
    private final ApplicationService applicationService;
    private final ClusterLoadBalancer clusterLoadBalancer;

    @NotNull(message = "Job cannot be null. Needed for execution.")
    private JobRequest jobRequest;

    @NotNull(message = "Cluster cannot be null. Need it to run job.")
    private Cluster cluster;

    @NotNull(message = "Command cannot null. Need it to run job.")
    private Command command;

    private List<Application> applications = new ArrayList<>();

    /**
     * Constructor Takes in a job request and figures out the cluster, command and applications
     * to run the job.
     *
     * @param clusterService implementation of ClusterService interface
     * @param commandService implementation of CommandService interface
     * @param applicationService implementation of ApplicationService interface
     * @param clusterLoadBalancer implementation of the ClusterLoadBalancer interface
     * @param jobRequest The jobRequest object
     * @throws GenieException exception if there is an error
     */
    @Autowired
    public JobExecutionEnvironment(
        final ClusterService clusterService,
        final CommandService commandService,
        final ApplicationService applicationService,
        final ClusterLoadBalancer clusterLoadBalancer,
        @NotNull(message = "Cannot construct environment from null job request")
        final JobRequest jobRequest
    ) throws GenieException {

        this.clusterService = clusterService;
        this.commandService = commandService;
        this.applicationService = applicationService;
        this.clusterLoadBalancer = clusterLoadBalancer;

        this.jobRequest = jobRequest;
        final Set<CommandStatus> enumStatuses = EnumSet.noneOf(CommandStatus.class);
        enumStatuses.add(CommandStatus.ACTIVE);

        this.cluster = this.clusterLoadBalancer
                        .selectCluster(this.clusterService.chooseClusterForJobRequest(jobRequest));

        // Find the command for the job
        for (final Command cmd : this.clusterService.getCommandsForCluster(
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

        this.applications.addAll(this.commandService.getApplicationsForCommand(this.command.getId()));
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
}
