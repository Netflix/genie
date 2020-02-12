/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.agent.execution.services.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.netflix.genie.agent.execution.CleanupStrategy;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.services.JobSetupService;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment;
import com.netflix.genie.common.external.dtos.v4.JobSpecification;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.util.RegexRuleSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
class JobSetupServiceImpl implements JobSetupService {

    private static final String NEWLINE = System.lineSeparator();
    private final DownloadService downloadService;

    JobSetupServiceImpl(
        final DownloadService downloadService
    ) {
        this.downloadService = downloadService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File createJobDirectory(final JobSpecification jobSpecification) throws SetUpJobException {
        final File jobDirectory = new File(
            jobSpecification.getJobDirectoryLocation(),
            jobSpecification.getJob().getId()
        );

        // Create parent folders separately
        try {
            Files.createDirectories(jobDirectory.getParentFile().toPath());
        } catch (final IOException e) {
            throw new SetUpJobException("Failed to create jobs directory", e);
        }

        // Create the job folder, this call throws if the directory already exists
        try {
            Files.createDirectory(jobDirectory.toPath());
        } catch (final IOException e) {
            throw new SetUpJobException("Failed to create job directory", e);
        }

        // Get DTOs
        final List<JobSpecification.ExecutionResource> applications = jobSpecification.getApplications();
        final JobSpecification.ExecutionResource cluster = jobSpecification.getCluster();
        final JobSpecification.ExecutionResource command = jobSpecification.getCommand();

        // Make a list of all entity dirs
        final List<Path> entityDirectories = Lists.newArrayList(
            PathUtils.jobClusterDirectoryPath(jobDirectory, cluster.getId()),
            PathUtils.jobCommandDirectoryPath(jobDirectory, command.getId())
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

        return jobDirectory;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    public Set<File> downloadJobResources(
        final JobSpecification jobSpecification,
        final File jobDirectory
    ) throws SetUpJobException {

        // Create download manifest for dependencies, configs, setup files for cluster, applications, command, job
        final DownloadService.Manifest jobDownloadsManifest =
            createDownloadManifest(jobDirectory, jobSpecification);

        // Download all files into place
        try {
            this.downloadService.download(jobDownloadsManifest);
        } catch (final DownloadException e) {
            throw new SetUpJobException("Failed to download job dependencies", e);
        }

        return jobDownloadsManifest.getTargetFiles();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public File createJobScript(
        final JobSpecification jobSpecification,
        final File jobDirectory
    ) throws SetUpJobException {

        final String scriptContent = new JobScriptComposer(jobSpecification, jobDirectory).composeScript();
        final Path scriptPath = PathUtils.jobScriptPath(jobDirectory);
        final File scriptFile = scriptPath.toFile();

        try {
            // Write the script
            try (
                Writer fileWriter = Files.newBufferedWriter(
                    scriptPath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC,
                    StandardOpenOption.DSYNC
                )
            ) {
                fileWriter.write(scriptContent);
                fileWriter.flush();
            }

            // Set script permissions
            final boolean permissionChangeSuccess = scriptFile.setExecutable(true);

            if (!permissionChangeSuccess) {
                throw new SetUpJobException("Could not set job script executable permission");
            }

            return scriptFile;

        } catch (SecurityException | IOException e) {
            throw new SetUpJobException("Failed to create job script: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cleanupJobDirectory(
        final Path jobDirectoryPath,
        final CleanupStrategy cleanupStrategy
    ) throws IOException {

        switch (cleanupStrategy) {
            case NO_CLEANUP:
                log.info("Skipping cleanup of job directory: {}", jobDirectoryPath);
                break;

            case FULL_CLEANUP:
                log.info("Wiping job directory: {}", jobDirectoryPath);
                FileSystemUtils.deleteRecursively(jobDirectoryPath);
                break;

            case DEPENDENCIES_CLEANUP:
                final RegexRuleSet cleanupWhitelist = RegexRuleSet.buildWhitelist(
                    Lists.newArrayList(
                        PathUtils.jobClusterDirectoryPath(jobDirectoryPath.toFile(), ".*"),
                        PathUtils.jobCommandDirectoryPath(jobDirectoryPath.toFile(), ".*"),
                        PathUtils.jobApplicationDirectoryPath(jobDirectoryPath.toFile(), ".*")
                    )
                        .stream()
                        .map(PathUtils::jobEntityDependenciesPath)
                        .map(Path::toString)
                        .map(pathString -> pathString + "/.*")
                        .map(Pattern::compile)
                        .toArray(Pattern[]::new)
                );
                Files.walk(jobDirectoryPath)
                    .filter(path -> cleanupWhitelist.accept(path.toAbsolutePath().toString()))
                    .forEach(path -> {
                        try {
                            log.debug("Deleting {}", path);
                            FileSystemUtils.deleteRecursively(path);
                        } catch (final IOException e) {
                            log.warn("Failed to delete: {}", path.toAbsolutePath().toString(), e);
                        }
                    });
                break;

            default:
                throw new RuntimeException("Unknown cleanup strategy: " + cleanupStrategy.name());
        }
    }

    private DownloadService.Manifest createDownloadManifest(
        final File jobDirectory,
        final JobSpecification jobSpec
    ) throws SetUpJobException {
        // Construct map of files to download and their expected locations in the job directory
        final DownloadService.Manifest.Builder downloadManifestBuilder = downloadService.newManifestBuilder();

        // Applications
        final List<JobSpecification.ExecutionResource> applications = jobSpec.getApplications();
        for (final JobSpecification.ExecutionResource application : applications) {
            final Path applicationDirectory =
                PathUtils.jobApplicationDirectoryPath(jobDirectory, application.getId());
            addEntitiesFilesToManifest(applicationDirectory, downloadManifestBuilder, application);
        }

        // Cluster
        final JobSpecification.ExecutionResource cluster = jobSpec.getCluster();
        final String clusterId = cluster.getId();
        final Path clusterDirectory =
            PathUtils.jobClusterDirectoryPath(jobDirectory, clusterId);
        addEntitiesFilesToManifest(clusterDirectory, downloadManifestBuilder, cluster);

        // Command
        final JobSpecification.ExecutionResource command = jobSpec.getCommand();
        final String commandId = command.getId();
        final Path commandDirectory =
            PathUtils.jobCommandDirectoryPath(jobDirectory, commandId);
        addEntitiesFilesToManifest(commandDirectory, downloadManifestBuilder, command);

        // Job (does not follow convention, downloads everything in the job root folder).
        try {
            final Path jobDirectoryPath = jobDirectory.toPath();
            final ExecutionEnvironment jobExecEnvironment = jobSpec.getJob().getExecutionEnvironment();

            if (jobExecEnvironment.getSetupFile().isPresent()) {
                final URI setupFileUri = new URI(jobExecEnvironment.getSetupFile().get());
                log.debug(
                    "Adding setup file to download manifest: {} -> {}",
                    setupFileUri,
                    jobDirectoryPath
                );
                downloadManifestBuilder.addFileWithTargetFile(
                    setupFileUri,
                    PathUtils.jobEntitySetupFilePath(jobDirectoryPath).toFile()
                );
            }

            for (final String dependencyUriString : jobExecEnvironment.getDependencies()) {
                log.debug(
                    "Adding dependency to download manifest: {} -> {}",
                    dependencyUriString,
                    jobDirectoryPath
                );
                downloadManifestBuilder.addFileWithTargetDirectory(
                    new URI(dependencyUriString), jobDirectory
                );
            }

            for (final String configUriString : jobExecEnvironment.getConfigs()) {
                log.debug(
                    "Adding config file to download manifest: {} -> {}",
                    configUriString,
                    jobDirectoryPath
                );
                downloadManifestBuilder.addFileWithTargetDirectory(
                    new URI(configUriString),
                    jobDirectory
                );
            }
        } catch (final URISyntaxException e) {
            throw new SetUpJobException("Failed to compose download manifest", e);
        }

        // Build manifest
        return downloadManifestBuilder.build();
    }

    private void addEntitiesFilesToManifest(
        final Path entityLocalDirectory,
        final DownloadService.Manifest.Builder downloadManifestBuilder,
        final JobSpecification.ExecutionResource executionResource
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
                downloadManifestBuilder.addFileWithTargetFile(
                    setupFileUri,
                    PathUtils.jobEntitySetupFilePath(entityLocalDirectory).toFile()
                );
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

    private static class JobScriptComposer {
        private static final String SETUP_LOG_ENV_VAR = "__GENIE_SETUP_LOG_FILE";
        private static final String ENVIRONMENT_LOG_ENV_VAR = "__GENIE_ENVIRONMENT_DUMP_FILE";

        private final String jobId;
        private final List<Pair<String, String>> setupFileReferences;
        private final List<Pair<String, String>> environmentVariables = Lists.newArrayList();
        private final String commandLine;

        JobScriptComposer(final JobSpecification jobSpecification, final File jobDirectory) {

            this.jobId = jobSpecification.getJob().getId();

            // Reference all files in the script in form: "${GENIE_JOB_DIR}/...".
            // This makes the script easier to relocate (easier to run, re-run on a different system/location).

            final Path jobDirectoryPath = jobDirectory.toPath().toAbsolutePath();

            final String jobSetupLogReference = getPathAsReference(
                PathUtils.jobSetupLogFilePath(jobDirectory),
                jobDirectoryPath
            );

            final String jobEnvironmentLogReference = getPathAsReference(
                PathUtils.jobEnvironmentLogFilePath(jobDirectory),
                jobDirectoryPath
            );

            final String applicationsDirReference = this.getPathAsReference(
                PathUtils.jobApplicationsDirectoryPath(jobDirectory),
                jobDirectoryPath
            );

            final JobSpecification.ExecutionResource cluster = jobSpecification.getCluster();
            final String clusterId = cluster.getId();
            final Path clusterDirPath = PathUtils.jobClusterDirectoryPath(jobDirectory, clusterId);
            final String clusterDirReference = this.getPathAsReference(clusterDirPath, jobDirectoryPath);

            final JobSpecification.ExecutionResource command = jobSpecification.getCommand();
            final String commandId = command.getId();
            final Path commandDirPath = PathUtils.jobCommandDirectoryPath(jobDirectory, commandId);
            final String commandDirReference = this.getPathAsReference(commandDirPath, jobDirectoryPath);

            // Set environment variables generated here
            this.addEnvVariable(JobConstants.GENIE_JOB_DIR_ENV_VAR, jobDirectoryPath.toString());
            this.addEnvVariable(JobConstants.GENIE_APPLICATION_DIR_ENV_VAR, applicationsDirReference);
            this.addEnvVariable(JobConstants.GENIE_COMMAND_DIR_ENV_VAR, commandDirReference);
            this.addEnvVariable(JobConstants.GENIE_CLUSTER_DIR_ENV_VAR, clusterDirReference);

            this.addEnvVariable(SETUP_LOG_ENV_VAR, jobSetupLogReference);
            this.addEnvVariable(ENVIRONMENT_LOG_ENV_VAR, jobEnvironmentLogReference);

            // And add the rest which come down from the server. (Sorted for determinism)
            ImmutableSortedMap.copyOf(jobSpecification.getEnvironmentVariables()).forEach(
                this::addEnvVariable
            );

            // Compose list of execution resources
            final List<Triple<String, JobSpecification.ExecutionResource, Path>> executionResources =
                Lists.newArrayList();

            jobSpecification.getApplications().forEach(
                application -> executionResources.add(
                    ImmutableTriple.of(
                        "application",
                        application,
                        PathUtils.jobApplicationDirectoryPath(jobDirectory, application.getId())
                    )
                )
            );

            executionResources.add(
                ImmutableTriple.of("cluster", cluster, PathUtils.jobClusterDirectoryPath(jobDirectory, clusterId))
            );
            executionResources.add(
                ImmutableTriple.of("command", command, PathUtils.jobCommandDirectoryPath(jobDirectory, commandId))
            );
            executionResources.add(
                ImmutableTriple.of("job", jobSpecification.getJob(), jobDirectoryPath)
            );

            // Transform execution resources list to description and file reference list
            this.setupFileReferences = executionResources.stream()
                .map(
                    triple -> {
                        final String resourceTypeString = triple.getLeft();
                        final JobSpecification.ExecutionResource resource = triple.getMiddle();

                        final String setupFileReference;
                        if (resource.getExecutionEnvironment().getSetupFile().isPresent()) {
                            final Path resourceDirectory = triple.getRight();
                            final Path setupFilePath = PathUtils.jobEntitySetupFilePath(resourceDirectory);
                            setupFileReference = getPathAsReference(setupFilePath, jobDirectoryPath);
                        } else {
                            setupFileReference = null;
                        }

                        final String resourceDescription = resourceTypeString + " " + resource.getId();
                        return ImmutablePair.of(resourceDescription, setupFileReference);
                    }
                )
                .collect(Collectors.toList());

            // Compose the final command-line

            this.commandLine = StringUtils.join(
                ImmutableList.builder()
                    .addAll(jobSpecification.getExecutableArgs())
                    .addAll(jobSpecification.getJobArgs())
                    .build(),
                " "
            );
        }

        // Transform an absolute path to a string that references $GENIE_JOB_DIR/...
        private String getPathAsReference(final Path path, final Path jobDirectoryPath) {
            final Path relativePath = jobDirectoryPath.relativize(path);
            return "${" + JobConstants.GENIE_JOB_DIR_ENV_VAR + "}/" + relativePath;
        }

        private void addEnvVariable(final String variableName, final String variableValue) {
            this.environmentVariables.add(ImmutablePair.of(variableName, variableValue));
        }

        String composeScript() {

            // Assemble the content of the script

            final StringBuilder sb = new StringBuilder();

            // Script Header Section
            sb
                .append("#!/usr/bin/env bash").append(NEWLINE)
                .append(NEWLINE);

            sb
                .append("#").append(NEWLINE)
                .append("# Generated by Genie for job: ").append(this.jobId).append(NEWLINE)
                .append("#").append(NEWLINE)
                .append(NEWLINE);

            sb
                .append("# Error out if any command fails").append(NEWLINE)
                .append("set -o errexit").append(NEWLINE)
                .append("# Error out if any command in a pipeline fails").append(NEWLINE)
                .append("set -o pipefail").append(NEWLINE)
                .append("# Error out if unknown variable is used").append(NEWLINE)
                .append("set -o nounset").append(NEWLINE)
                .append(NEWLINE);

            sb.append(NEWLINE);

            // Script Environment Section

            this.environmentVariables.forEach(
                envVar -> sb
                    .append("export ")
                    .append(envVar.getKey())
                    .append("=\"")
                    .append(envVar.getValue())
                    .append("\"")
                    .append(NEWLINE)
                    .append(NEWLINE)
            );

            sb.append(NEWLINE);

            // Script Setup Section

            final String setupFileRedirect = " >> ${" + SETUP_LOG_ENV_VAR + "}";

            sb
                .append("echo Setup begins: `date '+%Y-%m-%d %H:%M:%S'`")
                .append(setupFileRedirect)
                .append(NEWLINE);

            sb.append(NEWLINE);

            this.setupFileReferences.forEach(
                pair -> {
                    final String resourceDescription = pair.getLeft();
                    final String setupFileReference = pair.getRight();

                    if (setupFileReference != null) {
                        sb
                            .append("echo \"Sourcing setup script for ")
                            .append(resourceDescription)
                            .append("\"")
                            .append(setupFileRedirect)
                            .append(NEWLINE)
                            .append("source ")
                            .append(setupFileReference)
                            .append(" 2>&1")
                            .append(setupFileRedirect)
                            .append(NEWLINE);
                    } else {
                        sb
                            .append("echo \"No setup script for ")
                            .append(resourceDescription)
                            .append("\"")
                            .append(setupFileRedirect)
                            .append(NEWLINE);
                    }

                    sb.append(NEWLINE);
                }
            );

            sb
                .append("echo Setup end: `date '+%Y-%m-%d %H:%M:%S'`")
                .append(setupFileRedirect)
                .append(NEWLINE);

            sb.append(NEWLINE);

            // Dump environment for debugging
            sb
                .append("# Dump environment post-setup")
                .append(NEWLINE)
                .append("env | sort > ")
                .append("${").append(ENVIRONMENT_LOG_ENV_VAR).append("}")
                .append(NEWLINE);

            sb.append(NEWLINE);

            // Script Executable Section
            // N.B. Command *must* be last line of the script for the exit code to be propagated back correctly!
            sb
                .append("# Launch the command")
                .append(NEWLINE)
                .append(this.commandLine)
                .append(NEWLINE);

            return sb.toString();
        }
    }
}
