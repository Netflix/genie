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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.validator.constraints.NotBlank;
import org.hibernate.validator.constraints.NotEmpty;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.util.Collection;
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
public class JobRequestEntity extends CommonFields {

    @Basic(optional = false)
    @Column(name = "command_args", nullable = false, length = 1024)
    @Size(min = 1, max = 1024, message = "Must have command line arguments and can't be longer than 1024 characters")
    private String commandArgs;

    @Basic
    @Column(name = "group_name", length = 255)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String group;

    @Basic
    @Column(name = "setup_file", length = 1024)
    @Size(max = 1024, message = "File length for setup file is too long. Max is 1024 characters")
    private String setupFile;

    @Basic(optional = false)
    @Column(name = "cluster_criterias", nullable = false, length = 2048)
    @Size(min = 1, max = 2048, message = "Maximum length is 1024 characters min 1")
    private String clusterCriterias;

    @Basic(optional = false)
    @Column(name = "command_criteria", nullable = false, length = 1024)
    @Size(min = 1, max = 1024, message = "Maximum length is 1024 characters min 1")
    private String commandCriteria;

    @Lob
    @Column(name = "file_dependencies")
    private String fileDependencies;

    @Basic
    @Column(name = "disable_log_archival")
    private boolean disableLogArchival;

    @Basic
    @Column(name = "email", length = 255)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String email;

    @Basic
    @Column(name = "tags", length = 1024)
    @Size(max = 1024, message = "Max length in database is 1024 characters")
    private String tags;

    @Basic
    @Column(name = "client_host", length = 255)
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String clientHost;

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
        return this.unmarshall(this.clusterCriterias);
    }

    /**
     * Get the cluster criteria's as a JSON string.
     *
     * @return The criteria's from the original request as a JSON string
     */
    public String getClusterCriterias() {
        return this.clusterCriterias;
    }

    /**
     * Sets the list of cluster criteria specified to pick a cluster.
     *
     * @param clusterCriteriasList The criteria list. Not null or empty.
     * @throws GenieException If any precondition isn't met.
     */
    public void setClusterCriteriasFromList(
            @NotEmpty(message = "No cluster criterias entered") final List<ClusterCriteria> clusterCriteriasList
    ) throws GenieException {
        this.clusterCriterias = this.marshall(clusterCriteriasList);
    }

    /**
     * Set the cluster criterias JSON string.
     *
     * @param clusterCriterias The cluster criterias.
     */
    public void setClusterCriterias(@NotBlank(message = "Can't be empty") final String clusterCriterias) {
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
     * @param commandArgs Arguments to be used to run the command with. Not
     *                    null/empty/blank.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setCommandArgs(final String commandArgs) throws GeniePreconditionException {
        if (StringUtils.isBlank(commandArgs)) {
            throw new GeniePreconditionException("No command args entered.");
        }
        this.commandArgs = commandArgs;
    }

    /**
     * Get the file dependencies as a set of strings.
     *
     * @return The file dependencies for the job
     * @throws GenieException On any exception
     */
    public Set<String> getFileDependenciesAsSet() throws GenieException {
        return this.unmarshall(this.fileDependencies);
    }

    /**
     * Gets the fileDependencies for the job as JSON array.
     *
     * @return fileDependencies
     */
    public String getFileDependencies() {
        return this.fileDependencies;
    }

    /**
     * Sets the fileDependencies for the job.
     *
     * @param fileDependencies Dependent files for the job in csv format
     */
    public void setFileDependencies(final String fileDependencies) {
        this.fileDependencies = fileDependencies;
    }

    /**
     * Sets the fileDependencies for the job request from a set of strings.
     *
     * @param fileDependenciesSet Dependent files for the job
     * @throws GenieException for any processing error
     */
    public void setFileDependenciesFromSet(final Set<String> fileDependenciesSet) throws GenieException {
        this.fileDependencies = this.marshall(fileDependenciesSet);
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
     * Gets client hostname from which this job is run.
     *
     * @return clientHost
     */
    public String getClientHost() {
        return this.clientHost;
    }

    /**
     * Set the client host for the job.
     *
     * @param clientHost The client host anme.
     */
    public void setClientHost(final String clientHost) {
        this.clientHost = clientHost;
    }

    /**
     * Gets the command criteria which was specified to pick a command to run
     * the job.
     *
     * @return command criteria as a set of strings
     * @throws GenieException on any processing error
     */
    public Set<String> getCommandCriteriaAsSet() throws GenieException {
        return this.unmarshall(this.commandCriteria);
    }

    /**
     * Get the command criteria specified to run this job in string format.
     *
     * @return command criteria as a JSON array string
     */
    public String getCommandCriteria() {
        return this.commandCriteria;
    }

    /**
     * Sets the set of command criteria specified to pick a command.
     *
     * @param commandCriteriaSet The criteria set. Not null/empty
     * @throws GenieException If any precondition isn't met.
     */
    public void setCommandCriteriaFromSet(
            @NotEmpty(message = "At least one command criteria required") final Set<String> commandCriteriaSet
    ) throws GenieException {
        this.commandCriteria = this.marshall(commandCriteriaSet);
    }

    /**
     * Set the command criteria string of JSON.
     *
     * @param commandCriteria A set of command criteria tags as a JSON array
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setCommandCriteria(final String commandCriteria) throws GeniePreconditionException {
        this.commandCriteria = commandCriteria;
    }

    /**
     * Gets the envPropFile name.
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getSetupFile() {
        return this.setupFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile contains the list of env variables to set while
     *                    running this job.
     */
    public void setSetupFile(final String envPropFile) {
        this.setupFile = envPropFile;
    }

    /**
     * Gets the tags allocated to this job.
     *
     * @return the tags as JSON array
     */
    public String getTags() {
        return this.tags;
    }

    /**
     * Gets the tags allocated to this job as a set of strings.
     *
     * @return the tags
     * @throws GenieException For any processing error
     */
    public Set<String> getTagsAsSet() throws GenieException {
        return this.unmarshall(this.tags);
    }

    /**
     * Sets the tags allocated to this job.
     *
     * @param tags the tags to set as JSON array.
     */
    public void setTags(final String tags) {
        this.tags = tags;
    }

    /**
     * Sets the tags allocated to this job.
     *
     * @param tagsSet the tags to set.
     * @throws GenieException for any processing error
     */
    public void setTagsFromSet(final Set<String> tagsSet) throws GenieException {
        this.tags = marshall(tagsSet);
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
                .withFileDependencies(this.getFileDependenciesAsSet())
                .withGroup(this.group)
                .withSetupFile(this.setupFile)
                .withTags(this.getTagsAsSet())
                .withUpdated(this.getUpdated())
                .build();
    }

    protected String marshall(final Object value) throws GenieException {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(value);
        } catch (final JsonProcessingException jpe) {
            throw new GenieServerException(jpe);
        }
    }

    protected <T extends Collection> T unmarshall(final String source) throws GenieException {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(source, new TypeReference<T>() {
            });
        } catch (final IOException ioe) {
            throw new GenieServerException(ioe);
        }
    }
}
