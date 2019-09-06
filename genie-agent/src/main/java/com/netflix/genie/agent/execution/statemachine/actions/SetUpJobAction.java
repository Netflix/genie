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
import com.netflix.genie.agent.cli.ArgumentDelegates;
import com.netflix.genie.agent.cli.UserConsole;
import com.netflix.genie.agent.execution.CleanupStrategy;
import com.netflix.genie.agent.execution.ExecutionContext;
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException;
import com.netflix.genie.agent.execution.exceptions.DownloadException;
import com.netflix.genie.agent.execution.exceptions.SetUpJobException;
import com.netflix.genie.agent.execution.services.AgentFileStreamService;
import com.netflix.genie.agent.execution.services.AgentHeartBeatService;
import com.netflix.genie.agent.execution.services.AgentJobKillService;
import com.netflix.genie.agent.execution.services.AgentJobService;
import com.netflix.genie.agent.execution.services.DownloadService;
import com.netflix.genie.agent.execution.statemachine.Events;
import com.netflix.genie.agent.utils.EnvUtils;
import com.netflix.genie.agent.utils.PathUtils;
import com.netflix.genie.common.dto.JobStatus;
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment;
import com.netflix.genie.common.internal.dto.v4.JobSpecification;
import com.netflix.genie.common.internal.jobs.JobConstants;
import com.netflix.genie.common.internal.util.RegexRuleSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Action performed when in state SETUP_JOB.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
class SetUpJobAction extends BaseStateAction implements StateAction.SetUpJob {

    private final AgentJobService agentJobService;
    private final AgentHeartBeatService heartbeatService;
    private final AgentJobKillService killService;
    private final AgentFileStreamService fileManifestService;
    private final ArgumentDelegates.CleanupArguments cleanupArguments;
    private DownloadService downloadService;

    SetUpJobAction(
        final ExecutionContext executionContext,
        final DownloadService downloadService,
        final AgentJobService agentJobService,
        final AgentHeartBeatService heartbeatService,
        final AgentJobKillService killService,
        final AgentFileStreamService fileStreamService,
        final ArgumentDelegates.CleanupArguments cleanupArguments
    ) {
        super(executionContext);
        this.downloadService = downloadService;
        this.agentJobService = agentJobService;
        this.heartbeatService = heartbeatService;
        this.killService = killService;
        this.fileManifestService = fileStreamService;
        this.cleanupArguments = cleanupArguments;
    }

    @Override
    protected void executePreActionValidation() {
        assertClaimedJobIdPresent();
        assertCurrentJobStatusEqual(JobStatus.CLAIMED);
        assertJobSpecificationPresent();
        assertJobDirectoryNotPresent();
        assertJobEnvironmentNotPresent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Events executeStateAction(
        final ExecutionContext executionContext
    ) {
        UserConsole.getLogger().info("Setting up job...");

        final String claimedJobId = executionContext.getClaimedJobId().get();
        final JobSpecification jobSpecification = executionContext.getJobSpecification().get();

        heartbeatService.start(claimedJobId);
        killService.start(claimedJobId);

        try {
            this.agentJobService.changeJobStatus(
                claimedJobId,
                JobStatus.CLAIMED,
                JobStatus.INIT,
                "Setting up job"
            );
            executionContext.setCurrentJobStatus(JobStatus.INIT);
        } catch (final ChangeJobStatusException e) {
            throw new RuntimeException("Failed to update job status", e);
        }

        try {
            // Create folder structure
            final File jobDirectory = setupJobDirectory(claimedJobId, jobSpecification);
            executionContext.setJobDirectory(jobDirectory);

            // Move the agent log file inside the job folder
            relocateAgentLogFile(jobDirectory);

            // Start manifest service, allowing server to browse and request files.
            this.fileManifestService.start(claimedJobId, jobDirectory.toPath());

            // Download dependencies, configurations, etc.
            final List<File> setupFiles = downloadResources(jobSpecification, jobDirectory);

            final Map<String, String> jobEnvironment = setupJobEnvironment(jobDirectory, jobSpecification, setupFiles);
            executionContext.setJobEnvironment(jobEnvironment);

        } catch (final SetUpJobException e) {
            throw new RuntimeException("Failed to set up job", e);
        }

        return Events.SETUP_JOB_COMPLETE;
    }

    @Override
    protected void executePostActionValidation() {
        assertCurrentJobStatusEqual(JobStatus.INIT);
        assertJobDirectoryPresent();
        assertJobEnvironmentPresent();
    }

    @Override
    protected void executeStateActionCleanup(final ExecutionContext executionContext) {
        final File jobDirectory = executionContext.getJobDirectory().get();

        try {
            cleanupJobDirectory(jobDirectory.toPath(), cleanupArguments.getCleanupStrategy());
        } catch (final IOException e) {
            log.warn("Exception while performing job directory cleanup", e);
        }

        // Stop services started during setup
        killService.stop();
        heartbeatService.stop();
        fileManifestService.stop();
    }

    private void relocateAgentLogFile(final File jobDirectory) {
        final Path destinationPath = PathUtils.jobAgentLogFilePath(jobDirectory);
        log.info("Relocating agent log file to: {}", destinationPath);
        try {
            UserConsole.relocateLogFile(destinationPath);
        } catch (IOException e) {
            log.error("Failed to relocate agent log file", e);
        }
    }

    private File setupJobDirectory(
        final String claimedJobId, final JobSpecification jobSpecification) throws SetUpJobException {

        // Create job directory
        final File jobDirectory = new File(
            jobSpecification.getJobDirectoryLocation(),
            claimedJobId
        );

        createJobDirectory(jobDirectory);
        // Create job directory structure
        createJobDirectoryStructure(jobSpecification, jobDirectory);

        return jobDirectory;
    }

    private List<File> downloadResources(
        final JobSpecification jobSpecification,
        final File jobDirectory
    ) throws SetUpJobException {

        final List<java.io.File> setupFiles = Lists.newArrayList();

        // Create download manifest for dependencies, configs, setup files for cluster, applications, command, job
        final DownloadService.Manifest jobDownloadsManifest =
            createDownloadManifest(jobDirectory, jobSpecification, setupFiles);

        // Download all files into place
        try {
            downloadService.download(jobDownloadsManifest);
        } catch (final DownloadException e) {
            throw new SetUpJobException("Failed to download job dependencies", e);
        }

        return setupFiles;
    }

    private Map<String, String> setupJobEnvironment(
        final File jobDirectory,
        final JobSpecification jobSpecification,
        final List<File> setupFiles
    ) throws SetUpJobException {

        // Create additional environment variables
        final Map<String, String> extraEnvironmentVariables =
            createAdditionalEnvironmentMap(jobDirectory, jobSpecification);

        // Source set up files and collect resulting environment variables into a file
        final File jobEnvironmentFile = createJobEnvironmentFile(
            jobDirectory,
            setupFiles,
            jobSpecification.getEnvironmentVariables(),
            extraEnvironmentVariables
        );

        // Collect environment variables into a map
        return createJobEnvironmentMap(jobEnvironmentFile);
    }

    private void createJobDirectory(
        final File jobDirectory
    ) throws SetUpJobException {
        final File parentDir = jobDirectory.getParentFile();

        try {
            Files.createDirectories(parentDir.toPath());
        } catch (final IOException e) {
            throw new SetUpJobException("Failed to create jobs directory", e);
        }

        try {
            Files.createDirectory(jobDirectory.toPath());
        } catch (final IOException e) {
            throw new SetUpJobException("Failed to create job directory", e);
        }
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
                downloadManifestBuilder.addFileWithTargetDirectory(
                    setupFileUri,
                    jobDirectory
                );
                setupFileUris.add(setupFileUri);
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

        // Variables in environment file are base64 encoded to avoid escaping, quoting.
        // Decode all values.
        env.keySet().forEach(key ->
            env.compute(key, (k, v) -> new String(Base64.decodeBase64(v), StandardCharsets.UTF_8))
        );

        return Collections.unmodifiableMap(env);
    }

    private void cleanupJobDirectory(
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
                    (Pattern[]) Lists.newArrayList(
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
}
