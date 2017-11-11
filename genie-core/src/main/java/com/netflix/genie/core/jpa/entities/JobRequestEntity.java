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
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Type;
import org.hibernate.validator.constraints.NotEmpty;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Representation of the original Genie Job request.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Getter
@Setter
@Entity
@Table(name = "job_requests")
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

    @Basic(optional = false)
    @Column(name = "command_args", nullable = false, length = 10000)
    @Size(min = 1, max = 10000, message = "Must have command line arguments and be no longer than 10000 characters")
    private String commandArgs;

    @Basic
    @Column(name = "group_name")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String group;

    @Lob
    @Basic(optional = false)
    @Type(type = "org.hibernate.type.TextType")
    @Column(name = "cluster_criterias", nullable = false)
    @Size(min = 1, message = "Cluster criterias cannot be empty")
    private String clusterCriterias = EMPTY_JSON_ARRAY;

    @Lob
    @Basic(optional = false)
    @Type(type = "org.hibernate.type.TextType")
    @Column(name = "command_criteria", nullable = false)
    @Size(min = 1, message = "Command criteria cannot be empty")
    private String commandCriteria = EMPTY_JSON_ARRAY;

    @Lob
    @Type(type = "org.hibernate.type.TextType")
    @Basic(optional = false)
    @Column(name = "dependencies", nullable = false)
    private String dependencies = EMPTY_JSON_ARRAY;

    @Lob
    @Type(type = "org.hibernate.type.TextType")
    @Basic(optional = false)
    @Column(name = "configs", nullable = false)
    private String configs = EMPTY_JSON_ARRAY;

    @Basic(optional = false)
    @Column(name = "disable_log_archival", nullable = false)
    private boolean disableLogArchival;

    @Basic
    @Column(name = "email")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String email;

    @Basic
    @Column(name = "cpu")
    @Min(value = 1, message = "Can't have less than 1 CPU")
    private Integer cpu;

    @Basic
    @Column(name = "memory")
    @Min(value = 1, message = "Can't have less than 1 MB of memory allocated")
    private Integer memory;

    @Basic(optional = false)
    @Column(name = "applications", length = 2048)
    @Size(min = 1, max = 2048)
    private String applications = EMPTY_JSON_ARRAY;

    @Basic
    @Column(name = "timeout")
    @Min(value = 1)
    private Integer timeout;

    /**
     * Gets the group name of the user who submitted the job.
     *
     * @return group
     */
    public Optional<String> getGroup() {
        return Optional.ofNullable(this.group);
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
    protected void setClusterCriterias(final String clusterCriterias) {
        this.clusterCriterias = StringUtils.isBlank(clusterCriterias) ? EMPTY_JSON_ARRAY : clusterCriterias;
    }

    /**
     * Get the file configs as a set of strings.
     *
     * @return The file configs for the job
     * @throws GenieException On any exception
     */
    public Set<String> getConfigsAsSet() throws GenieException {
        return JsonUtils.unmarshall(this.configs, SET_STRING_TYPE_REFERENCE);
    }

    /**
     * Sets the configs for the job request from a set of strings.
     *
     * @param configsSet Configuration files for the job
     * @throws GenieException for any processing error
     */
    public void setConfigsFromSet(final Set<String> configsSet) throws GenieException {
        this.configs = configsSet == null ? EMPTY_JSON_ARRAY : JsonUtils.marshall(configsSet);
    }

    /**
     * Gets the configs for the job as JSON array.
     *
     * @return configs
     */
    protected String getConfigs() {
        return this.configs;
    }

    /**
     * Sets the configs for the job.
     *
     * @param configs Dependent files for the job in csv format
     */
    protected void setConfigs(final String configs) {
        this.configs = StringUtils.isBlank(configs) ? EMPTY_JSON_ARRAY : configs;
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
    protected void setDependencies(final String dependencies) {
        this.dependencies = StringUtils.isBlank(dependencies) ? EMPTY_JSON_ARRAY : dependencies;
    }

    /**
     * Gets the email address needed to send an email on job completion.
     *
     * @return the email as an optional
     */
    public Optional<String> getEmail() {
        return Optional.ofNullable(this.email);
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
        this.commandCriteria = StringUtils.isBlank(commandCriteria) ? EMPTY_JSON_ARRAY : commandCriteria;
    }

    /**
     * Get the number of CPU's requested to run this job.
     *
     * @return The number of CPU's as an Optional
     */
    public Optional<Integer> getCpu() {
        return Optional.ofNullable(this.cpu);
    }

    /**
     * Get the memory requested to run this job with.
     *
     * @return The amount of memory the user requested for this job in MB as an Optional
     */
    public Optional<Integer> getMemory() {
        return Optional.ofNullable(this.memory);
    }

    /**
     * Get the timeout (in seconds) requested by the user for this job.
     *
     * @return The number of seconds before a timeout as an Optional
     */
    public Optional<Integer> getTimeout() {
        return Optional.ofNullable(this.timeout);
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
     * Get a DTO representing this job request.
     *
     * @return The read-only DTO.
     * @throws GenieException For any processing error
     */
    public JobRequest getDTO() throws GenieException {
        final JobRequest.Builder builder = new JobRequest.Builder(
            this.getName(),
            this.getUser(),
            this.getVersion(),
            this.commandArgs,
            this.getClusterCriteriasAsList(),
            this.getCommandCriteriaAsSet()
        )
            .withCreated(this.getCreated())
            .withId(this.getId())
            .withDisableLogArchival(this.disableLogArchival)
            .withEmail(this.email)
            .withConfigs(this.getConfigsAsSet())
            .withDependencies(this.getDependenciesAsSet())
            .withGroup(this.group)
            .withTags(this.getTags())
            .withCpu(this.cpu)
            .withMemory(this.memory)
            .withUpdated(this.getUpdated())
            .withApplications(this.getApplicationsAsList())
            .withTimeout(this.timeout);

        this.getDescription().ifPresent(builder::withDescription);
        this.getSetupFile().ifPresent(builder::withSetupFile);

        return builder.build();
    }
}
