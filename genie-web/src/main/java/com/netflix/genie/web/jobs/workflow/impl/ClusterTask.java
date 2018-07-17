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

import com.google.common.collect.Sets;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.web.jobs.AdminResources;
import com.netflix.genie.web.jobs.FileType;
import com.netflix.genie.web.jobs.JobExecutionEnvironment;
import com.netflix.genie.web.services.impl.GenieFileTransferService;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the workflow task for processing cluster information a job needs.
 *
 * @author amsharma
 * @since 3.0.0
 */
//TODO add unit test for this Task (see InitialSetupTaskUnitTest for reference)
@Slf4j
public class ClusterTask extends GenieBaseTask {

    private static final String CLUSTER_TASK_TIMER_NAME = "genie.jobs.tasks.clusterTask.timer";
    private final GenieFileTransferService fts;

    /**
     * Constructor.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer service
     */
    public ClusterTask(@NotNull final MeterRegistry registry, @NotNull final GenieFileTransferService fts) {
        super(registry);
        this.fts = fts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(@NotNull final Map<String, Object> context) throws GenieException, IOException {
        final Set<Tag> tags = Sets.newHashSet();
        final long start = System.nanoTime();
        try {
            final JobExecutionEnvironment jobExecEnv =
                (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
            final Cluster cluster = jobExecEnv.getCluster();
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, cluster.getMetadata().getName()));
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, cluster.getId()));
            final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
            final String genieDir = jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR;
            final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);
            log.info("Starting Cluster Task for job {}", jobExecEnv.getJobRequest().getId().orElse(NO_ID_FOUND));

            final String clusterId = cluster.getId();

            // Create the directory for this application under applications in the cwd
            createEntityInstanceDirectory(
                genieDir,
                clusterId,
                AdminResources.CLUSTER
            );

            // Create the config directory for this id
            createEntityInstanceConfigDirectory(
                genieDir,
                clusterId,
                AdminResources.CLUSTER
            );

            // Create the dependencies directory for this id
            createEntityInstanceDependenciesDirectory(
                genieDir,
                clusterId,
                AdminResources.CLUSTER
            );

            // Get the set up file for cluster and add it to source in launcher script
            final Optional<String> setupFile = cluster.getResources().getSetupFile();
            if (setupFile.isPresent()) {
                final String clusterSetupFile = setupFile.get();
                if (StringUtils.isNotBlank(clusterSetupFile)) {
                    final String localPath = super.buildLocalFilePath(
                        jobWorkingDirectory,
                        clusterId,
                        clusterSetupFile,
                        FileType.SETUP,
                        AdminResources.CLUSTER
                    );

                    this.fts.getFile(clusterSetupFile, localPath);

                    super.generateSetupFileSourceSnippet(
                        clusterId,
                        "Cluster:",
                        localPath,
                        writer,
                        jobWorkingDirectory);
                }
            }

            // Iterate over and get all configuration files
            for (final String configFile : cluster.getResources().getConfigs()) {
                final String localPath = super.buildLocalFilePath(
                    jobWorkingDirectory,
                    clusterId,
                    configFile,
                    FileType.CONFIG,
                    AdminResources.CLUSTER
                );
                this.fts.getFile(configFile, localPath);
            }

            // Iterate over and get all dependencies
            for (final String dependencyFile : cluster.getResources().getDependencies()) {
                final String localPath = super.buildLocalFilePath(
                    jobWorkingDirectory,
                    clusterId,
                    dependencyFile,
                    FileType.DEPENDENCIES,
                    AdminResources.CLUSTER
                );
                this.fts.getFile(dependencyFile, localPath);
            }
            log.info("Finished Cluster Task for job {}", jobExecEnv.getJobRequest().getId().orElse(NO_ID_FOUND));
            MetricsUtils.addSuccessTags(tags);
        } catch (final Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.getRegistry()
                .timer(CLUSTER_TASK_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
