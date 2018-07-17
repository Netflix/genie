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
package com.netflix.genie.web.jobs.workflow.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.dto.v4.Command;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.controllers.DtoConverters;
import com.netflix.genie.web.jobs.JobExecutionEnvironment;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the workflow task for handling Applications that a job needs.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class InitialSetupTask extends GenieBaseTask {

    static final String SETUP_TASK_TIMER_NAME = "genie.jobs.tasks.initialSetupTask.timer";
    private static final String GENIE_VERSION_EXPORT = "export GENIE_VERSION=3";
    private static final String LINE_SEPARATOR = System.lineSeparator();

    /**
     * Constructor.
     *
     * @param registry The metrics registry to use
     */
    public InitialSetupTask(@NotNull final MeterRegistry registry) {
        super(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(@NotNull final Map<String, Object> context) throws GenieException, IOException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final JobExecutionEnvironment jobExecEnv
                = (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
            final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
            final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);
            final String jobId = jobExecEnv
                .getJobRequest()
                .getId()
                .orElseThrow(() -> new GeniePreconditionException("No job id found. Unable to continue"));
            log.info("Starting Initial Setup Task for job {}", jobId);

            this.createJobDirStructure(jobWorkingDirectory);

            // set the env variables in the launcher script
            this.createJobDirEnvironmentVariables(writer, jobWorkingDirectory);
            this.createApplicationEnvironmentVariables(writer);

            // create environment variables for the command
            final Command command = jobExecEnv.getCommand();
            this.createCommandEnvironmentVariables(writer, command);

            // create environment variables for the cluster
            final Cluster cluster = jobExecEnv.getCluster();
            this.createClusterEnvironmentVariables(writer, cluster);

            // create environment variable for the job itself
            this.createJobEnvironmentVariables(
                writer,
                jobId,
                jobExecEnv.getJobRequest().getName(),
                jobExecEnv.getMemory()
            );

            // create environment variables for the job request
            this.createJobRequestEnvironmentVariables(writer, jobExecEnv.getJobRequest());

            //Export the Genie Version
            writer.write(GENIE_VERSION_EXPORT);
            writer.write(LINE_SEPARATOR);
            writer.write(LINE_SEPARATOR);

            log.info("Finished Initial Setup Task for job {}", jobId);
            MetricsUtils.addSuccessTags(tags);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.getRegistry()
                .timer(SETUP_TASK_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }

    @VisibleForTesting
    void createJobDirStructure(final String jobWorkingDirectory) throws GenieException, IOException {
        // create top level directory structure for the job

        // Genie Directory {basedir/genie}
        this.createDirectory(jobWorkingDirectory + JobConstants.FILE_PATH_DELIMITER + JobConstants.GENIE_PATH_VAR);

        // Genie Logs directory {basedir/genie/logs}
        this.createDirectory(jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.LOGS_PATH_VAR);

        // Genie applications directory {basedir/genie/applications}
        this.createDirectory(jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.APPLICATION_PATH_VAR);

        // Genie command directory {basedir/genie/command}
        this.createDirectory(jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.COMMAND_PATH_VAR);

        // Genie cluster directory {basedir/genie/cluster}
        this.createDirectory(jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.CLUSTER_PATH_VAR);

        // Create std out file
        final File stdout = new File(jobWorkingDirectory, JobConstants.STDOUT_LOG_FILE_NAME);
        if (!stdout.exists() && !stdout.createNewFile()) {
            throw new GenieServerException("Unable to create std out file at " + stdout);
        }
        final File stderr = new File(jobWorkingDirectory, JobConstants.STDERR_LOG_FILE_NAME);
        if (!stderr.exists() && !stderr.createNewFile()) {
            throw new GenieServerException("Unable to create std err file at " + stderr);
        }
    }

    @VisibleForTesting
    void createJobDirEnvironmentVariables(final Writer writer, final String jobWorkingDirectory) throws IOException {
        // set environment variable for the job directory
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_JOB_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + jobWorkingDirectory
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // Append new line
        writer.write(LINE_SEPARATOR);
    }

    @VisibleForTesting
    void createApplicationEnvironmentVariables(final Writer writer) throws IOException {
        // create environment variable for the application directory
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_APPLICATION_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + "${"
            + JobConstants.GENIE_JOB_DIR_ENV_VAR
            + "}"
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.APPLICATION_PATH_VAR
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // Append new line
        writer.write(LINE_SEPARATOR);
    }

    @VisibleForTesting
    void createCommandEnvironmentVariables(final Writer writer, final Command command) throws IOException {
        final String commandId = command.getId();
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_COMMAND_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + "${"
            + JobConstants.GENIE_JOB_DIR_ENV_VAR
            + "}"
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.COMMAND_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + commandId
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // Append new line
        writer.write(LINE_SEPARATOR);

        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_COMMAND_ID_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + commandId
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(System.lineSeparator());

        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_COMMAND_NAME_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + command.getMetadata().getName()
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(System.lineSeparator());

        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_COMMAND_TAGS_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + this.tagsToString(
                DtoConverters.toV3Tags(
                    command.getId(),
                    command.getMetadata().getName(),
                    command.getMetadata().getTags()
                )
            )
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(System.lineSeparator());
    }

    @VisibleForTesting
    void createClusterEnvironmentVariables(final Writer writer, final Cluster cluster) throws IOException {
        final String clusterId = cluster.getId();

        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_CLUSTER_DIR_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + "${"
            + JobConstants.GENIE_JOB_DIR_ENV_VAR
            + "}"
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.CLUSTER_PATH_VAR
            + JobConstants.FILE_PATH_DELIMITER
            + clusterId
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // Append new line
        writer.write(System.lineSeparator());

        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_CLUSTER_ID_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + clusterId
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(System.lineSeparator());

        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_CLUSTER_NAME_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + cluster.getMetadata().getName()
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(System.lineSeparator());

        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_CLUSTER_TAGS_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + this.tagsToString(
                DtoConverters.toV3Tags(
                    cluster.getId(),
                    cluster.getMetadata().getName(),
                    cluster.getMetadata().getTags()
                )
            )
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(System.lineSeparator());
    }

    @VisibleForTesting
    void createJobEnvironmentVariables(
        final Writer writer,
        final String jobId,
        final String jobName,
        final int memory
    ) throws IOException {
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_JOB_ID_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + jobId
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // Append new line
        writer.write(System.lineSeparator());

        // create environment variable for the job name
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_JOB_NAME_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + jobName
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // Append new line
        writer.write(LINE_SEPARATOR);

        // create environment variable for the job name
        writer.write(
            JobConstants.EXPORT
                + JobConstants.GENIE_JOB_MEMORY_ENV_VAR
                + JobConstants.EQUALS_SYMBOL
                + memory
                + LINE_SEPARATOR
        );

        // Append new line
        writer.write(LINE_SEPARATOR);
    }

    @VisibleForTesting
    void createJobRequestEnvironmentVariables(
        final Writer writer,
        final JobRequest jobRequest
    ) throws IOException {

        // create environment variable for the command tags/criteria in the job request
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_REQUESTED_COMMAND_TAGS_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + tagsToString(jobRequest.getCommandCriteria())
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        final List<ClusterCriteria> clusterCriterias = jobRequest.getClusterCriterias();

        final List<String> clusterCriteriasStrings = new ArrayList<>(clusterCriterias.size());

        for (ClusterCriteria clusterCriteria : clusterCriterias) {
            clusterCriteriasStrings.add("[" + tagsToString(clusterCriteria.getTags()) + "]");
        }

        // Append new line
        writer.write(System.lineSeparator());

        // create environment variable for the list of cluster tags/criteria in the job request as a single
        // value in the form "[[x,y,z],[a,b,c]]"
        writer.write(JobConstants.EXPORT
            + JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR
            + JobConstants.EQUALS_SYMBOL
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + "[" + StringUtils.join(clusterCriteriasStrings, ',') + "]"
            + JobConstants.DOUBLE_QUOTE_SYMBOL
            + LINE_SEPARATOR);

        // create environment variables for individual tags/criteria in the job request
        for (int i = 0; i < clusterCriterias.size(); i++) {
            final ClusterCriteria clusterCriteria = clusterCriterias.get(i);

            // create environment variable for the job name
            writer.write(JobConstants.EXPORT
                + JobConstants.GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR + "_" + i
                + JobConstants.EQUALS_SYMBOL
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + tagsToString(clusterCriteria.getTags())
                + JobConstants.DOUBLE_QUOTE_SYMBOL
                + LINE_SEPARATOR);
        }

        // Append new line
        writer.write(LINE_SEPARATOR);
    }

    /**
     * Helper to convert a set of tags into a string that is a suitable value for a shell environment variable.
     * Adds double quotes as necessary (i.e. in case of spaces, newlines), performs escaping of in-tag quotes.
     * Input tags are sorted to produce a deterministic output value.
     *
     * @param tags a set of tags or null
     * @return a CSV string
     */
    @VisibleForTesting
    String tagsToString(final Set<String> tags) {
        final ArrayList<String> sortedTags = new ArrayList<>(tags == null ? Collections.emptySet() : tags);
        // Sort tags for the sake of determinism (e.g., tests)
        sortedTags.sort(Comparator.naturalOrder());
        final String joinedString = StringUtils.join(sortedTags, ',');
        // Escape quotes
        return StringUtils.replaceAll(StringUtils.replaceAll(joinedString, "\'", "\\\'"), "\"", "\\\"");
    }
}
