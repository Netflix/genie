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
package com.netflix.genie.core.jpa.entities;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.util.JsonUtils;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Set;

/**
 * Representation of the original Genie Job request.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Entity
@Table(name = "job_requests")
@Getter
@Setter
public class JobRequestEntity extends SetupFileEntity {

    private static final long serialVersionUID = -1895413051636217614L;
    private static final TypeReference<Set<String>> SET_STRING_TYPE_REFERENCE = new TypeReference<Set<String>>() {
    };
    private static final TypeReference<List<String>> LIST_STRING_TYPE_REFERENCE = new TypeReference<List<String>>() {
    };
    private static final TypeReference<List<ClusterCriteria>> LIST_CLUSTER_CRITERIA_TYPE_REFERENCE
        = new TypeReference<List<ClusterCriteria>>() {
    };
    private static final String EMPTY_JSON_ARRAY = "[]";

    @Basic
    @Column(name = "command_args", nullable = false, length = 10000)
    @Size(min = 1, max = 10000, message = "Must have command line arguments and be no longer than 10000 characters")
    private String commandArgs;

    @Basic
    @Column(name = "group_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String group;

    @Basic(optional = false)
    @Column(name = "cluster_criterias", nullable = false, length = 2048)
    @Size(min = 1, max = 2048, message = "Maximum length is 1024 characters min 1")
    private String clusterCriterias = EMPTY_JSON_ARRAY;

    @Basic(optional = false)
    @Column(name = "command_criteria", nullable = false, length = 1024)
    @Size(min = 1, max = 1024, message = "Maximum length is 1024 characters min 1")
    private String commandCriteria = EMPTY_JSON_ARRAY;

    @Basic
    @Column(name = "dependencies", length = 30000)
    @Size(max = 30000, message = "Max length in the database is 30000 characters")
    private String dependencies = EMPTY_JSON_ARRAY;

    @Basic
    @Column(name = "disable_log_archival")
    private boolean disableLogArchival;

    @Basic
    @Column(name = "email")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String email;

    @Basic(optional = false)
    @Column(name = "cpu", nullable = false)
    @Min(value = 1, message = "Can't have less than 1 CPU")
    private int cpu = 1;

    @Basic(optional = false)
    @Column(name = "memory", nullable = false)
    @Min(value = 1, message = "Can't have less than 1 MB of memory allocated")
    private int memory = 1536; // 1.5 GB in MB

    @Basic(optional = false)
    @Column(name = "applications", length = 2048)
    @Size(min = 1, max = 2048)
    private String applications = EMPTY_JSON_ARRAY;

    @Basic(optional = false)
    @Column(name = "timeout", nullable = false)
    @Min(value = 1)
    private int timeout = 604800; // Seven days in seconds

    @OneToOne(
        mappedBy = "request",
        fetch = FetchType.LAZY,
        cascade = CascadeType.ALL,
        orphanRemoval = true
    )
    private JobEntity job;

    @OneToOne(
        mappedBy = "request",
        fetch = FetchType.LAZY,
        cascade = CascadeType.ALL,
        orphanRemoval = true
    )
    private JobRequestMetadataEntity jobRequestMetadata;

    /**
     * Gets the group name of the user who submitted the job.
     *
     * @return group
     */
    public String getGroup() {
        return this.group;
    }

    /**
     * Sets the group of the user who submits the job.
     *
     * @param group group of the user submitting the job
     */
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * Gets the cluster criteria which was specified to pick a cluster to run
     * the job.
     *
     * @return clusterCriterias
     * @throws GenieException on any error
     */
    public List<ClusterCriteria> getClusterCriteriasAsList() throws GenieException {
        return JsonUtils.unmarshall(this.clusterCriterias, LIST_CLUSTER_CRITERIA_TYPE_REFERENCE);
    }

    /**
     * Sets the list of cluster criteria specified to pick a cluster.
     *
     * @param clusterCriteriasList The criteria list. Not null or empty.
     * @throws GenieException If any precondition isn't met.
     */
    public void setClusterCriteriasFromList(
        @NotEmpty final List<ClusterCriteria> clusterCriteriasList
    ) throws GenieException {
        this.clusterCriterias = JsonUtils.marshall(clusterCriteriasList);
    }

    /**
     * Get the cluster criteria's as a JSON string.
     *
     * @return The criteria's from the original request as a JSON string
     */
    protected String getClusterCriterias() {
        return this.clusterCriterias;
    }

    /**
     * Set the cluster criterias JSON string.
     *
     * @param clusterCriterias The cluster criterias.
     */
    protected void setClusterCriterias(@NotBlank final String clusterCriterias) {
        this.clusterCriterias = clusterCriterias;
    }

    /**
     * Gets the commandArgs specified to run the job.
     *
     * @return commandArgs
     */
    public String getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Parameters specified to be run and fed as command line arguments to the
     * job run.
     *
     * @param commandArgs Arguments to be used to run the command with. Not null/empty/blank.
     */
    public void setCommandArgs(@NotBlank final String commandArgs) {
        this.commandArgs = commandArgs;
    }

    /**
     * Get the file dependencies as a set of strings.
     *
     * @return The file dependencies for the job
     * @throws GenieException On any exception
     */
    public Set<String> getDependenciesAsSet() throws GenieException {
        return JsonUtils.unmarshall(this.dependencies, SET_STRING_TYPE_REFERENCE);
    }

    /**
     * Sets the dependencies for the job request from a set of strings.
     *
     * @param dependenciesSet Dependent files for the job
     * @throws GenieException for any processing error
     */
    public void setDependenciesFromSet(final Set<String> dependenciesSet) throws GenieException {
        this.dependencies = dependenciesSet == null ? EMPTY_JSON_ARRAY : JsonUtils.marshall(dependenciesSet);
    }

    /**
     * Gets the dependencies for the job as JSON array.
     *
     * @return dependencies
     */
    protected String getDependencies() {
        return this.dependencies;
    }

    /**
     * Sets the dependencies for the job.
     *
     * @param dependencies Dependent files for the job in csv format
     */
    public void setDependencies(final String dependencies) {
        this.dependencies = dependencies;
    }

    /**
     * Is the log archival disabled.
     *
     * @return true if it's disabled
     */
    public boolean isDisableLogArchival() {
        return this.disableLogArchival;
    }

    /**
     * Set whether the log archival is disabled or not.
     *
     * @param disableLogArchival True if disabling is desired
     */
    public void setDisableLogArchival(final boolean disableLogArchival) {
        this.disableLogArchival = disableLogArchival;
    }

    /**
     * Gets the commandArgs specified to run the job.
     *
     * @return commandArgs
     */
    public String getEmail() {
        return this.email;
    }

    /**
     * Set user Email address for the job.
     *
     * @param email user email address
     */
    public void setEmail(final String email) {
        this.email = email;
    }

    /**
     * Gets the command criteria which was specified to pick a command to run
     * the job.
     *
     * @return command criteria as a set of strings
     * @throws GenieException on any processing error
     */
    public Set<String> getCommandCriteriaAsSet() throws GenieException {
        return JsonUtils.unmarshall(this.commandCriteria, SET_STRING_TYPE_REFERENCE);
    }

    /**
     * Sets the set of command criteria specified to pick a command.
     *
     * @param commandCriteriaSet The criteria set. Not null/empty
     * @throws GenieException If any precondition isn't met.
     */
    public void setCommandCriteriaFromSet(@NotEmpty final Set<String> commandCriteriaSet) throws GenieException {
        this.commandCriteria = JsonUtils.marshall(commandCriteriaSet);
    }

    /**
     * Get the command criteria specified to run this job in string format.
     *
     * @return command criteria as a JSON array string
     */
    protected String getCommandCriteria() {
        return this.commandCriteria;
    }

    /**
     * Set the command criteria string of JSON.
     *
     * @param commandCriteria A set of command criteria tags as a JSON array
     */
    protected void setCommandCriteria(final String commandCriteria) {
        this.commandCriteria = commandCriteria;
    }

    /**
     * Get the number of CPU's requested to run this job.
     *
     * @return The number of CPU's
     */
    public int getCpu() {
        return this.cpu;
    }

    /**
     * Set the number of CPU's requested to run this job with.
     *
     * @param cpu The number of CPU's. Minimum 1.
     */
    public void setCpu(@Min(1) final int cpu) {
        this.cpu = cpu;
    }

    /**
     * Get the memory requested to run this job with.
     *
     * @return The amount of memory the user requested for this job in MB
     */
    public int getMemory() {
        return this.memory;
    }

    /**
     * Set the amount of memory requested to run this job with.
     *
     * @param memory The amount of memory requested in MB. Minimum 1.
     */
    public void setMemory(@Min(1) final int memory) {
        this.memory = memory;
    }

    /**
     * Get the applications to use for this job as a List of ids.
     *
     * @return The applications for the job
     * @throws GenieException On any exception
     */
    public List<String> getApplicationsAsList() throws GenieException {
        return JsonUtils.unmarshall(this.applications, LIST_STRING_TYPE_REFERENCE);
    }

    /**
     * Sets the dependencies for the job request from a set of strings.
     *
     * @param applicationsList Application IDs for the job
     * @throws GenieException for any processing error
     */
    public void setApplicationsFromList(final List<String> applicationsList) throws GenieException {
        this.applications = applicationsList == null ? EMPTY_JSON_ARRAY : JsonUtils.marshall(applicationsList);
    }

    /**
     * Gets the applications for the job as JSON array.
     *
     * @return applications
     */
    protected String getApplications() {
        return this.applications;
    }

    /**
     * Sets the dependencies for the job.
     *
     * @param applications Applications for the job in JSON array string
     */
    protected void setApplications(final String applications) {
        this.applications = applications;
    }

    /**
     * Get the job associated with this job request.
     *
     * @return The job
     */
    public JobEntity getJob() {
        return this.job;
    }

    /**
     * Set the job for this request.
     *
     * @param job The job
     */
    public void setJob(@NotNull final JobEntity job) {
        this.job = job;
        job.setRequest(this);
    }

    /**
     * Set the additional metadata for this request.
     *
     * @param jobRequestMetadata The metadata
     */
    public void setJobRequestMetadata(@NotNull final JobRequestMetadataEntity jobRequestMetadata) {
        this.jobRequestMetadata = jobRequestMetadata;
        this.jobRequestMetadata.setRequest(this);
    }

    /**
     * Get a DTO representing this job request.
     *
     * @return The read-only DTO.
     * @throws GenieException For any processing error
     */
    public JobRequest getDTO() throws GenieException {
        return new JobRequest.Builder(
            this.getName(),
            this.getUser(),
            this.getVersion(),
            this.commandArgs,
            this.getClusterCriteriasAsList(),
            this.getCommandCriteriaAsSet()
        )
            .withCreated(this.getCreated())
            .withId(this.getId())
            .withDescription(this.getDescription())
            .withDisableLogArchival(this.disableLogArchival)
            .withEmail(this.email)
            .withDependencies(this.getDependenciesAsSet())
            .withGroup(this.group)
            .withSetupFile(this.getSetupFile())
            .withTags(this.getTags())
            .withCpu(this.cpu)
            .withMemory(this.memory)
            .withUpdated(this.getUpdated())
            .withApplications(this.getApplicationsAsList())
            .withTimeout(this.timeout)
            .build();
    }
}
