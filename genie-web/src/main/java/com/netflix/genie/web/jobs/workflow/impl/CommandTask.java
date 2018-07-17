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
import com.netflix.genie.common.internal.dto.v4.Command;
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
 * Implementation of the workflow task for processing command information.
 *
 * @author amsharma
 * @since 3.0.0
 */
//TODO add unit test for this Task (see InitialSetupTaskUnitTest for reference)
@Slf4j
public class CommandTask extends GenieBaseTask {

    private static final String COMMAND_TASK_TIMER_NAME = "genie.jobs.tasks.commandTask.timer";
    private final GenieFileTransferService fts;

    /**
     * Constructor.
     *
     * @param registry The metrics registry to use
     * @param fts      File transfer service
     */
    public CommandTask(@NotNull final MeterRegistry registry, @NotNull final GenieFileTransferService fts) {
        super(registry);
        this.fts = fts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(@NotNull final Map<String, Object> context) throws GenieException, IOException {
        final long start = System.nanoTime();
        final Set<Tag> tags = Sets.newHashSet();
        try {
            final JobExecutionEnvironment jobExecEnv =
                (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
            final Command command = jobExecEnv.getCommand();
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_NAME, command.getMetadata().getName()));
            tags.add(Tag.of(MetricsConstants.TagKeys.COMMAND_ID, command.getId()));
            final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
            final String genieDir = jobWorkingDirectory
                + JobConstants.FILE_PATH_DELIMITER
                + JobConstants.GENIE_PATH_VAR;
            final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);

            log.info("Starting Command Task for job {}", jobExecEnv.getJobRequest().getId().orElse(NO_ID_FOUND));

            final String commandId = command.getId();

            // Create the directory for this command under command dir in the cwd
            createEntityInstanceDirectory(
                genieDir,
                commandId,
                AdminResources.COMMAND
            );

            // Create the config directory for this id
            createEntityInstanceConfigDirectory(
                genieDir,
                commandId,
                AdminResources.COMMAND
            );

            // Create the dependencies directory for this id
            createEntityInstanceDependenciesDirectory(
                genieDir,
                commandId,
                AdminResources.COMMAND
            );

            // Get the setup file if specified and add it as source command in launcher script
            final Optional<String> setupFile = command.getResources().getSetupFile();
            if (setupFile.isPresent()) {
                final String commandSetupFile = setupFile.get();
                if (StringUtils.isNotBlank(commandSetupFile)) {
                    final String localPath = super.buildLocalFilePath(
                        jobWorkingDirectory,
                        commandId,
                        commandSetupFile,
                        FileType.SETUP,
                        AdminResources.COMMAND
                    );

                    fts.getFile(commandSetupFile, localPath);

                    super.generateSetupFileSourceSnippet(
                        commandId,
                        "Command:",
                        localPath,
                        writer,
                        jobWorkingDirectory);
                }
            }

            // Iterate over and get all configuration files
            for (final String configFile : command.getResources().getConfigs()) {
                final String localPath = super.buildLocalFilePath(
                    jobWorkingDirectory,
                    commandId,
                    configFile,
                    FileType.CONFIG,
                    AdminResources.COMMAND
                );
                fts.getFile(configFile, localPath);
            }

            // Iterate over and get all dependencies
            for (final String dependencyFile : command.getResources().getDependencies()) {
                final String localPath = super.buildLocalFilePath(
                    jobWorkingDirectory,
                    commandId,
                    dependencyFile,
                    FileType.DEPENDENCIES,
                    AdminResources.COMMAND
                );
                fts.getFile(dependencyFile, localPath);
            }
            log.info("Finished Command Task for job {}", jobExecEnv.getJobRequest().getId().orElse(NO_ID_FOUND));
            MetricsUtils.addSuccessTags(tags);
        } catch (Throwable t) {
            MetricsUtils.addFailureTagsWithException(tags, t);
            throw t;
        } finally {
            this.getRegistry()
                .timer(COMMAND_TASK_TIMER_NAME, tags)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
    }
}
