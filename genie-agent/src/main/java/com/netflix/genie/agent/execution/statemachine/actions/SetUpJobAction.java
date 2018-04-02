/*
 *
 *  Copyright 2018 Netflix, Inc.
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

package com.netflix.genie.agent.execution.statemachine.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.utils.EnvUtils;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.dto.v4.JobSpecification;
import com.netflix.genie.common.jobs.JobConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Action performed when in state SETUP_JOB.
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
@Component
@Lazy
class SetUpJobAction extends BaseStateAction implements StateAction.SetUpJob {

    private DownloadService downloadService;

    SetUpJobAction(
        final ExecutionContext executionContext,
        final DownloadService downloadService
    ) {
        super(executionContext);
        this.downloadService = downloadService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(
        final ExecutionContext executionContext
    ) {
        try {
            performJobSetup(executionContext);
        } catch (final SetUpJobException e) {
            throw new RuntimeException("Failed to set up job", e);
        }

        // TODO: turn off console logging if the job is interactive

        return Events.SETUP_JOB_COMPLETE;
    }

    private void performJobSetup(final ExecutionContext executionContext) throws SetUpJobException {

        log.info("Setting up job...");

        final JobSpecification jobSpec = executionContext.getJobSpecification();

        if (jobSpec == null) {
            throw new SetUpJobException("Job specification not set");
        }

        // Create job directory
        final File jobDirectory = createJobDirectory(jobSpec);

        executionContext.setJobDirectory(jobDirectory);

        // Create job directory structure
        createJobDirectoryStructure(jobSpec, jobDirectory);

        final List<File> setupFiles = Lists.newArrayList();

        // Create download manifest for dependencies, configs, setup files for cluster, applications, command, job
        final DownloadService.Manifest jobDownloadsManifest = createDownloadManifest(jobDirectory, jobSpec, setupFiles);

        // Download all files into place
        try {
            downloadService.download(jobDownloadsManifest);
        } catch (final DownloadException e) {
            throw new SetUpJobException("Failed to download job dependencies", e);
        }

        // Create additional environment variables
        final Map<String, String> extraEnvironmentVariables = createAdditionalEnvironmentMap(jobDirectory, jobSpec);

        // Source set up files and collect resulting environment variables into a file
        final File jobEnvironmentFile = createJobEnvironmentFile(
            jobDirectory,
            setupFiles,
            jobSpec.getEnvironmentVariables(),
            extraEnvironmentVariables
        );

        // Collect environment variables into a map
        final Map<String, String> jobEnvironment = createJobEnvironmentMap(jobEnvironmentFile);

        executionContext.setJobEnvironment(jobEnvironment);
    }

    private File createJobDirectory(
        final JobSpecification jobSpec
    ) throws SetUpJobException {
        final File parentDir = jobSpec.getJobDirectoryLocation();
        final String jobId = jobSpec.getJob().getId();

        if (StringUtils.isBlank(jobId)) {
            throw new SetUpJobException("Job directory parent is blank");
        } else if (!parentDir.exists() || !parentDir.isDirectory()) {
            throw new SetUpJobException("Invalid destination for job directory: " + parentDir);
        } else if (!parentDir.isAbsolute()) {
            throw new SetUpJobException("Job directory parent path is not absolute: " + parentDir);
        }

        final File jobDir = new File(parentDir, jobId);

        if (jobDir.exists()) {
            throw new SetUpJobException("Job directory already exists: " + jobDir);
        }

        try {
            Files.createDirectory(jobDir.toPath());
        } catch (final IOException e) {
            throw new SetUpJobException("Failed to create job directory", e);
        }

        return jobDir;
    }

    private void createJobDirectoryStructure(
        final JobSpecification jobSpec,
        final File jobDirectory
    ) throws SetUpJobException {

        // Get DTOs
        final List<JobSpecification.ExecutionResource> applications = jobSpec.getApplications();
        final JobSpecification.ExecutionResource cluster = jobSpec.getCluster();
        final JobSpecification.ExecutionResource command = jobSpec.getCommand();

        // Make a list of all entity dirs
        final List<Path> entityDirectories = Lists.newArrayList(
            PathUtils.jobClusterDirectoryPath(jobDirectory, cluster.getId()),
            PathUtils.jobCommandDirectoryPath(jobDirectory, command.getId()),
            jobDirectory.toPath()
        );

        applications.stream()
            .map(JobSpecification.ExecutionResource::getId)
            .map(appId -> PathUtils.jobApplicationDirectoryPath(jobDirectory, appId))
            .forEach(entityDirectories::add);

        // Make a list of directories to create
        // (only "leaf" paths, since createDirectories creates missing intermediate dirs
        final List<Path> directoriesToCreate = Lists.newArrayList();

        // Add config and dependencies for each entity
        entityDirectories.forEach(
            entityDirectory -> {
                directoriesToCreate.add(PathUtils.jobEntityDependenciesPath(entityDirectory));
                directoriesToCreate.add(PathUtils.jobEntityConfigPath(entityDirectory));
            }
        );

        // Add logs dir
        directoriesToCreate.add(PathUtils.jobGenieLogsDirectoryPath(jobDirectory));

        // Create directories
        for (final Path path : directoriesToCreate) {
            try {
                Files.createDirectories(path);
            } catch (final Exception e) {
                throw new SetUpJobException("Failed to create directory: " + path, e);
            }
        }
    }

    private DownloadService.Manifest createDownloadManifest(
        final File jobDirectory,
        final JobSpecification jobSpec,
        final List<File> setupFiles
    ) throws SetUpJobException {
        // Construct map of files to download and their expected locations in the job directory
        final DownloadService.Manifest.Builder downloadManifestBuilder = downloadService.newManifestBuilder();

        // Track URIs for all setup files
        final List<URI> setupFileUris = Lists.newArrayList();

        // Applications
        final List<JobSpecification.ExecutionResource> applications = jobSpec.getApplications();
        for (final JobSpecification.ExecutionResource application : applications) {
            final Path applicationDirectory =
                PathUtils.jobApplicationDirectoryPath(jobDirectory, application.getId());
            addEntitiesFilesToManifest(applicationDirectory, downloadManifestBuilder, application, setupFileUris);
        }

        // Cluster
        final JobSpecification.ExecutionResource cluster = jobSpec.getCluster();
        final String clusterId = cluster.getId();
        final Path clusterDirectory =
            PathUtils.jobClusterDirectoryPath(jobDirectory, clusterId);
        addEntitiesFilesToManifest(clusterDirectory, downloadManifestBuilder, cluster, setupFileUris);

        // Command
        final JobSpecification.ExecutionResource command = jobSpec.getCommand();
        final String commandId = command.getId();
        final Path commandDirectory =
            PathUtils.jobCommandDirectoryPath(jobDirectory, commandId);
        addEntitiesFilesToManifest(commandDirectory, downloadManifestBuilder, command, setupFileUris);

        // Job
        final JobSpecification.ExecutionResource jobRequest = jobSpec.getJob();
        addEntitiesFilesToManifest(jobDirectory.toPath(), downloadManifestBuilder, jobRequest, setupFileUris);

        // Build manifest
        final DownloadService.Manifest manifest = downloadManifestBuilder.build();

        // Populate list of setup files with expected location on disk after download
        for (final URI setupFileUri : setupFileUris) {
            final File setupFile = manifest.getTargetLocation(setupFileUri);
            if (setupFile == null) {
                throw new SetUpJobException("Failed to look up target location for setup file: " + setupFileUri);
            }
            setupFiles.add(setupFile);
        }

        return manifest;
    }

    private void addEntitiesFilesToManifest(
        final Path entityLocalDirectory,
        final DownloadService.Manifest.Builder downloadManifestBuilder,
        final JobSpecification.ExecutionResource executionResource,
        final List<URI> setupFilesUris
    ) throws SetUpJobException {

        try {
            final ExecutionEnvironment resourceExecutionEnvironment = executionResource.getExecutionEnvironment();

            if (resourceExecutionEnvironment.getSetupFile().isPresent()) {
                final URI setupFileUri = new URI(resourceExecutionEnvironment.getSetupFile().get());
                log.debug(
                    "Adding setup file to download manifest: {} -> {}",
                    setupFileUri,
                    entityLocalDirectory
                );
                downloadManifestBuilder.addFileWithTargetDirectory(
                    setupFileUri,
                    entityLocalDirectory.toFile()
                );
                setupFilesUris.add(setupFileUri);
            }

            final Path entityDependenciesLocalDirectory = PathUtils.jobEntityDependenciesPath(entityLocalDirectory);
            for (final String dependencyUriString : resourceExecutionEnvironment.getDependencies()) {
                log.debug(
                    "Adding dependency to download manifest: {} -> {}",
                    dependencyUriString,
                    entityDependenciesLocalDirectory
                );
                downloadManifestBuilder.addFileWithTargetDirectory(
                    new URI(dependencyUriString), entityDependenciesLocalDirectory.toFile()
                );
            }

            final Path entityConfigsDirectory = PathUtils.jobEntityConfigPath(entityLocalDirectory);
            for (final String configUriString : resourceExecutionEnvironment.getConfigs()) {
                log.debug(
                    "Adding config file to download manifest: {} -> {}",
                    configUriString,
                    entityConfigsDirectory
                );
                downloadManifestBuilder.addFileWithTargetDirectory(
                    new URI(configUriString),
                    entityConfigsDirectory.toFile()
                );
            }
        } catch (final URISyntaxException e) {
            throw new SetUpJobException("Failed to compose download manifest", e);
        }
    }


    private Map<String, String> createAdditionalEnvironmentMap(
        final File jobDirectory,
        final JobSpecification jobSpec
    ) {
        final ImmutableMap.Builder<String, String> mapBuilder = new ImmutableMap.Builder<>();

        mapBuilder.put(
            JobConstants.GENIE_JOB_DIR_ENV_VAR,
            jobDirectory.toString()
        );

        mapBuilder.put(
            JobConstants.GENIE_APPLICATION_DIR_ENV_VAR,
            PathUtils.jobApplicationsDirectoryPath(jobDirectory).toString()
        );

        mapBuilder.put(
            JobConstants.GENIE_COMMAND_DIR_ENV_VAR,
            PathUtils.jobCommandDirectoryPath(jobDirectory, jobSpec.getCommand().getId()).toString()
        );

        mapBuilder.put(
            JobConstants.GENIE_CLUSTER_DIR_ENV_VAR,
            PathUtils.jobClusterDirectoryPath(jobDirectory, jobSpec.getCluster().getId()).toString()
        );

        return mapBuilder.build();
    }

    private File createJobEnvironmentFile(
        final File jobDirectory,
        final List<File> setUpFiles,
        final Map<String, String> serverProvidedEnvironment,
        final Map<String, String> extraEnvironment
    ) throws SetUpJobException {
        final Path genieDirectory = PathUtils.jobGenieDirectoryPath(jobDirectory);
        final Path envScriptPath = PathUtils.composePath(
            genieDirectory,
            JobConstants.GENIE_AGENT_ENV_SCRIPT_RESOURCE
        );
        final Path envScriptLogPath = PathUtils.composePath(
            genieDirectory,
            JobConstants.LOGS_PATH_VAR,
            JobConstants.GENIE_AGENT_ENV_SCRIPT_LOG_FILE_NAME
        );
        final Path envScriptOutputPath = PathUtils.composePath(
            genieDirectory,
            JobConstants.GENIE_AGENT_ENV_SCRIPT_OUTPUT_FILE_NAME
        );

        // Copy env script from resources to genie directory
        try {
            Files.copy(
                new ClassPathResource(JobConstants.GENIE_AGENT_ENV_SCRIPT_RESOURCE).getInputStream(),
                envScriptPath,
                StandardCopyOption.REPLACE_EXISTING
            );
            // Make executable
            envScriptPath.toFile().setExecutable(true, true);
        } catch (final IOException e) {
            throw new SetUpJobException("Could not copy environment script resource: ", e);
        }

        // Set up process that executes the script
        final ProcessBuilder processBuilder = new ProcessBuilder()
            .directory(jobDirectory)
            .inheritIO();

        processBuilder.environment().putAll(serverProvidedEnvironment);
        processBuilder.environment().putAll(extraEnvironment);

        final List<String> commandArgs = Lists.newArrayList(
            envScriptPath.toString(),
            envScriptOutputPath.toString(),
            envScriptLogPath.toString()
        );

        setUpFiles.forEach(f -> commandArgs.add(f.getAbsolutePath()));

        processBuilder.command(commandArgs);

        // Run the setup script
        final int exitCode;
        try {
            exitCode = processBuilder.start().waitFor();
        } catch (final IOException e) {
            throw new SetUpJobException("Could not execute environment setup script", e);
        } catch (final InterruptedException e) {
            throw new SetUpJobException("Interrupted while waiting for environment setup script", e);
        }

        if (exitCode != 0) {
            throw new SetUpJobException("Non-zero exit code from environment setup script: " + exitCode);
        }

        // Check and return the output file
        final File envScriptOutputFile = envScriptOutputPath.toFile();

        if (!envScriptOutputFile.exists()) {
            throw new SetUpJobException("Expected output file does not exist: " + envScriptOutputPath.toString());
        }

        return envScriptOutputFile;
    }

    private Map<String, String> createJobEnvironmentMap(
        final File jobEnvironmentFile
    ) throws SetUpJobException {

        final Map<String, String> env;
        try {
            env = EnvUtils.parseEnvFile(jobEnvironmentFile);
        } catch (final IOException | EnvUtils.ParseException e) {
            throw new SetUpJobException(
                "Failed to parse environment from file: " + jobEnvironmentFile.getAbsolutePath(),
                e
            );
        }
        return Collections.unmodifiableMap(env);
    }

}
