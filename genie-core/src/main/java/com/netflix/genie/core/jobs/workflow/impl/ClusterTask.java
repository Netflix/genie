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
package com.netflix.genie.core.jobs.workflow.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.jobs.AdminResources;
import com.netflix.genie.core.jobs.FileType;
import com.netflix.genie.core.jobs.JobConstants;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;
import com.netflix.genie.core.services.impl.GenieFileTransferService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

/**
 * Implementation of the workflow task for processing cluster information a job needs.
 *
 * @author amsharma
 * @since 3.0.0
 */
@Slf4j
public class ClusterTask extends GenieBaseTask {
    /**
     * {@inheritDoc}
     */
    @Override
    public void executeTask(
        @NotNull
        final Map<String, Object> context
    ) throws GenieException, IOException {
        log.debug("Executing Cluster Task in the workflow.");

        final GenieFileTransferService fts =
            (GenieFileTransferService) context.get(JobConstants.FILE_TRANSFER_SERVICE_KEY);
        final JobExecutionEnvironment jobExecEnv =
            (JobExecutionEnvironment) context.get(JobConstants.JOB_EXECUTION_ENV_KEY);
        final String jobWorkingDirectory = jobExecEnv.getJobWorkingDir().getCanonicalPath();
        final String genieDir = jobWorkingDirectory
            + JobConstants.FILE_PATH_DELIMITER
            + JobConstants.GENIE_PATH_VAR;
        final Writer writer = (Writer) context.get(JobConstants.WRITER_KEY);

        // Create the directory for this application under applications in the cwd
        createEntityInstanceDirectory(
            genieDir,
            jobExecEnv.getCluster().getId(),
            AdminResources.CLUSTER
        );

        // Create the config directory for this id
        createEntityInstanceConfigDirectory(
            genieDir,
            jobExecEnv.getCluster().getId(),
            AdminResources.CLUSTER
        );

        // Get the set up file for cluster and add it to source in launcher script
        final String clusterSetupFile = jobExecEnv.getCluster().getSetupFile();

        if (clusterSetupFile != null && StringUtils.isNotBlank(clusterSetupFile)) {
            final String localPath = super.buildLocalFilePath(
                jobWorkingDirectory,
                jobExecEnv.getCluster().getId(),
                clusterSetupFile,
                FileType.SETUP,
                AdminResources.CLUSTER
            );

            fts.getFile(clusterSetupFile, localPath);

            super.generateSetupFileSourceSnippet(
                jobExecEnv.getCluster().getId(),
                "Cluster:",
                localPath,
                writer,
                jobWorkingDirectory);
        }

        // Iterate over and get all configuration files
        for (final String configFile: jobExecEnv.getCluster().getConfigs()) {
            final String localPath = super.buildLocalFilePath(
                jobWorkingDirectory,
                jobExecEnv.getCluster().getId(),
                configFile,
                FileType.CONFIG,
                AdminResources.CLUSTER
            );
            fts.getFile(configFile, localPath);
        }
    }
}
