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
package com.netflix.genie.agent.execution.services.impl

import com.google.common.collect.Sets
import com.netflix.genie.agent.execution.CleanupStrategy
import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.agent.execution.exceptions.SetUpJobException
import com.netflix.genie.agent.execution.services.DownloadService
import com.netflix.genie.agent.execution.services.JobSetupService
import com.netflix.genie.agent.utils.EnvUtils
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.internal.jobs.JobConstants
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.stream.Collectors

class JobSetupServiceImplSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder

    JobSetupService service;
    DownloadService downloadService

    ExecutionContext executionContext

    Set<URI> manifestUris
    File dummyFile

    DownloadService.Manifest manifest
    DownloadService.Manifest.Builder manifestBuilder

    JobSpecification spec
    String jobId
    List<JobSpecification.ExecutionResource> apps
    JobSpecification.ExecutionResource app1
    JobSpecification.ExecutionResource app2
    JobSpecification.ExecutionResource cluster
    JobSpecification.ExecutionResource command
    JobSpecification.ExecutionResource job

    File app1Dir
    ExecutionEnvironment app1Env
    Optional<String> app1Setup
    Set<String> app1Configs
    Set<String> app1Deps

    File app2Dir
    ExecutionEnvironment app2Env
    Optional<String> app2Setup
    Set<String> app2Configs
    Set<String> app2Deps

    File clusterDir
    ExecutionEnvironment clusterEnv
    Optional<String> clusterSetup
    Set<String> clusterConfigs
    Set<String> clusterDeps

    File commandDir
    ExecutionEnvironment commandEnv
    Optional<String> commandSetup
    Set<String> commandConfigs
    Set<String> commandDeps

    File jobDir
    ExecutionEnvironment jobEnv
    Optional<String> jobSetup = Optional.empty()
    Set<String> jobConfigs = []
    Set<String> jobDeps = []

    Map<String, String> jobServerEnvMap

    void setup() {
        this.dummyFile = temporaryFolder.newFile()

        this.executionContext = Mock(ExecutionContext)

        this.manifestUris = Sets.newHashSet()

        this.manifest = Mock(DownloadService.Manifest) {
        }

        this.manifestBuilder = Mock(DownloadService.Manifest.Builder) {
            _ * build() >> manifest
        }
        this.downloadService = Mock(DownloadService) {
            _ * newManifestBuilder() >> manifestBuilder
        }

        this.jobId = UUID.randomUUID().toString()
        this.jobDir = new File(temporaryFolder.getRoot(), jobId)
        this.jobSetup = Optional.empty()
        this.jobConfigs = []
        this.jobDeps = []
        this.jobEnv = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> { return jobSetup }
            _ * getConfigs() >> { return jobConfigs }
            _ * getDependencies() >> { return jobDeps }
        }
        this.job = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> { return jobId }
            _ * getExecutionEnvironment() >> { return jobEnv }
        }

        def app1Id = UUID.randomUUID().toString()
        this.app1Setup = Optional.empty()
        this.app1Configs = []
        this.app1Deps = []
        this.app1Env = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> { return app1Setup }
            _ * getConfigs() >> { return app1Configs }
            _ * getDependencies() >> { return app1Deps }
        }
        this.app1 = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> { return app1Id }
            _ * getExecutionEnvironment() >> { return app1Env }
        }
        this.app1Dir = PathUtils.jobApplicationDirectoryPath(jobDir, app1.getId()).toFile()

        def app2Id = UUID.randomUUID().toString()
        this.app2Setup = Optional.empty()
        this.app2Configs = []
        this.app2Deps = []
        this.app2Env = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> { return app2Setup }
            _ * getConfigs() >> { return app2Configs }
            _ * getDependencies() >> { return app2Deps }
        }
        this.app2 = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> { return app2Id }
            _ * getExecutionEnvironment() >> { return app2Env }
        }
        this.app2Dir = PathUtils.jobApplicationDirectoryPath(jobDir, app2.getId()).toFile()

        this.apps = [
            app1,
            app2
        ]

        def clusterId = UUID.randomUUID().toString()
        this.clusterSetup = Optional.empty()
        this.clusterConfigs = []
        this.clusterDeps = []
        this.clusterEnv = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> { return clusterSetup }
            _ * getConfigs() >> { return clusterConfigs }
            _ * getDependencies() >> { return clusterDeps }
        }
        this.cluster = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> { return clusterId }
            _ * getExecutionEnvironment() >> { return clusterEnv }
        }
        this.clusterDir = PathUtils.jobClusterDirectoryPath(jobDir, cluster.getId()).toFile()

        def commandId = UUID.randomUUID().toString()
        this.commandSetup = Optional.empty()
        this.commandConfigs = []
        this.commandDeps = []
        this.commandEnv = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> { return commandSetup }
            _ * getConfigs() >> { return commandConfigs }
            _ * getDependencies() >> { return commandDeps }
        }
        this.command = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> { return commandId }
            _ * getExecutionEnvironment() >> { return commandEnv }
        }

        this.commandDir = PathUtils.jobCommandDirectoryPath(jobDir, command.getId()).toFile()

        this.jobServerEnvMap = [:]

        this.spec = Mock(JobSpecification) {
            _ * getApplications() >> apps
            _ * getCluster() >> cluster
            _ * getCommand() >> command
            _ * getJob() >> job
            _ * getJobDirectoryLocation() >> temporaryFolder.getRoot()
            _ * getEnvironmentVariables() >> jobServerEnvMap
        }

        this.downloadService = Mock(DownloadService)
        this.service = new JobSetupServiceImpl(downloadService)
    }

    void cleanup() {
        File file = new File("created-by-setup-file.txt")
        if (file.exists()) {
            file.delete()
        }
    }

    def "CreateJobDirectory"() {
        setup:
        def id1 = "job1"
        def id2 = "existing"
        def id3 = "invalid"
        String clusterId = "my-cluster"
        String commandId = "my-command"
        String applicationId1 = "my-app1"
        String applicationId2 = "my-app2"

        JobSpecification jobSpecification = Mock(JobSpecification)
        JobSpecification.ExecutionResource job = Mock(JobSpecification.ExecutionResource)
        JobSpecification.ExecutionResource cluster = Mock(JobSpecification.ExecutionResource)
        JobSpecification.ExecutionResource command = Mock(JobSpecification.ExecutionResource)
        JobSpecification.ExecutionResource app1 = Mock(JobSpecification.ExecutionResource)
        JobSpecification.ExecutionResource app2 = Mock(JobSpecification.ExecutionResource)

        def testDirectory = temporaryFolder.newFolder("createJobDirectory")

        when: "Create directory that already exists"
        new File(testDirectory, id2).createNewFile()
        service.createJobDirectory(jobSpecification)

        then:
        1 * jobSpecification.getJob() >> job
        1 * jobSpecification.getJobDirectoryLocation() >> testDirectory
        1 * job.getId() >> id2
        thrown(SetUpJobException)

        when: "Create directory in a forbidden location"
        new File(testDirectory, id2).createNewFile()
        service.createJobDirectory(jobSpecification)

        then:
        1 * jobSpecification.getJob() >> job
        1 * jobSpecification.getJobDirectoryLocation() >> new File("/", "foo")
        1 * job.getId() >> id3
        thrown(SetUpJobException)

        when: "Create successfully"
        def jobDirectory = service.createJobDirectory(jobSpecification)

        then:
        1 * jobSpecification.getJob() >> job
        1 * jobSpecification.getJobDirectoryLocation() >> testDirectory
        1 * jobSpecification.getApplications() >> [app1, app2]
        1 * jobSpecification.getCluster() >> cluster
        1 * jobSpecification.getCommand() >> command
        1 * job.getId() >> id1
        1 * cluster.getId() >> clusterId
        1 * command.getId() >> commandId
        1 * app1.getId() >> applicationId1
        1 * app2.getId() >> applicationId2

        expect:
        jobDirectory.exists()
        jobDirectory.getName() == id1
        Files.walk(jobDirectory.toPath())
            .filter({ f -> Files.isRegularFile(f) })
            .collect(Collectors.toList())
            .isEmpty()
        Files.walk(jobDirectory.toPath())
            .filter({ f -> !Files.isRegularFile(f) })
            .map({ f -> jobDirectory.toPath().relativize(f) })
            .map({ f -> f.toString() })
            .collect(Collectors.toSet())
            .containsAll([
                "genie/cluster/my-cluster/config",
                "genie/command/my-command/dependencies",
                "genie/applications/my-app1/dependencies",
                "genie/command",
                "genie/applications/my-app1/config",
                "genie/applications/my-app2/config",
                "genie/applications",
                "genie/logs",
                "genie/cluster",
                "genie/applications/my-app2",
                "genie/applications/my-app1",
                "genie/cluster/my-cluster/dependencies",
                "genie",
                "genie/cluster/my-cluster",
                "genie/applications/my-app2/dependencies",
                "genie/command/my-command",
                "genie/command/my-command/config"
            ])
    }

    def "Setup w/o dependencies and environment"() {

        when:
        File jobDirectory = service.createJobDirectory(spec)

        then:
        jobDir == jobDirectory

        when:
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)

        when:
        Map<String, String> jobEnvironment = service.setupJobEnvironment(jobDirectory, spec, setupFiles)

        then:
        jobEnvironment != null
        jobEnvironment.get(JobConstants.GENIE_JOB_DIR_ENV_VAR) == jobDir.toString()
        jobEnvironment.get(JobConstants.GENIE_APPLICATION_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.APPLICATION_PATH_VAR
        jobEnvironment.get(JobConstants.GENIE_COMMAND_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.COMMAND_PATH_VAR + "/" + command.getId()
        jobEnvironment.get(JobConstants.GENIE_CLUSTER_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.CLUSTER_PATH_VAR + "/" + cluster.getId()

        for (File entityDir : [app1Dir, app2Dir, clusterDir, commandDir]) {
            assert entityDir.exists()
            def confDir = new File(entityDir, JobConstants.CONFIG_FILE_PATH_PREFIX)
            assert confDir.exists()
            assert confDir.list().size() == 0
            def depsDir = new File(entityDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX)
            assert depsDir.exists()
            assert depsDir.list().size() == 0
        }
    }

    def "Execute w/ dependencies and environment"() {
        setup:
        jobServerEnvMap.put("SERVER_VARIABLE_KEY", "SERVER VARIABLE VALUE")
        def setupFileUri = URI.create("s3://my-bucket/my-org/my-job/setup.sh")
        jobSetup = Optional.of(setupFileUri.toString())
        app1Setup = Optional.of(setupFileUri.toString())
        app2Setup = Optional.of(setupFileUri.toString())
        commandSetup = Optional.of(setupFileUri.toString())
        clusterSetup = Optional.of(setupFileUri.toString())
        def dependencyUri = URI.create("s3://my-bucket/my-org/my-job/dependency.tar.gz")
        jobDeps.add(dependencyUri.toString())
        app1Deps.add(dependencyUri.toString())
        app2Deps.add(dependencyUri.toString())
        commandDeps.add(dependencyUri.toString())
        clusterDeps.add(dependencyUri.toString())
        def configUri = URI.create("s3://my-bucket/my-org/my-job/cfg.xml")
        jobConfigs.add(configUri.toString())
        app1Configs.add(configUri.toString())
        app2Configs.add(configUri.toString())
        commandConfigs.add(configUri.toString())
        clusterConfigs.add(configUri.toString())
        dummyFile.write(
            "echo Hello World\n"
                + "export SETUP_VARIABLE_KEY='SETUP VARIABLE VALUE'\n"
                + "touch created-by-setup-file.txt\n"
                + "export TRICKY_VARIABLE=\"'foo\nbar\ny'all'\"\n"
        )

        when:
        File jobDirectory = service.createJobDirectory(spec)
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)
        Map<String, String> jobEnvironment = service.setupJobEnvironment(jobDirectory, spec, setupFiles)

        then:
        jobDir == jobDirectory
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, app1Dir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, app2Dir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, commandDir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, clusterDir)
        5 * manifest.getTargetLocation(setupFileUri) >> dummyFile
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, jobDir)
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(app1Dir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(app2Dir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(commandDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(clusterDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        0 * manifest.getTargetLocation(dependencyUri)
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, jobDir)
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(app1Dir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(app2Dir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(commandDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(clusterDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        0 * manifest.getTargetLocation(configUri)

        expect:
        jobDir.exists()
        app1Dir.exists()
        app2Dir.exists()
        clusterDir.exists()
        commandDir.exists()
        jobEnvironment != null
        jobEnvironment.get(JobConstants.GENIE_JOB_DIR_ENV_VAR) == jobDir.toString()
        jobEnvironment.get(JobConstants.GENIE_APPLICATION_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.APPLICATION_PATH_VAR
        jobEnvironment.get(JobConstants.GENIE_COMMAND_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.COMMAND_PATH_VAR + "/" + command.getId()
        jobEnvironment.get(JobConstants.GENIE_CLUSTER_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.CLUSTER_PATH_VAR + "/" + cluster.getId()
        jobEnvironment.get("SERVER_VARIABLE_KEY") == "SERVER VARIABLE VALUE"
        jobEnvironment.get("SETUP_VARIABLE_KEY") == "SETUP VARIABLE VALUE"
        jobEnvironment.get("TRICKY_VARIABLE") == "'foo\nbar\ny'all'"
        new File("created-by-setup-file.txt").exists()
        PathUtils.composePath(
            PathUtils.jobGenieDirectoryPath(jobDir),
            JobConstants.LOGS_PATH_VAR,
            JobConstants.GENIE_AGENT_ENV_SCRIPT_LOG_FILE_NAME
        ).toFile().getText(StandardCharsets.UTF_8.toString()).count("Hello World") == 5
    }

    def "SetUp file lookup error"() {
        setup:
        def setupFileUri = URI.create("s3://my-bucket/my-org/my-job/setup.sh")
        jobSetup = Optional.of(setupFileUri.toString())

        when:
        File jobDirectory = service.createJobDirectory(spec)
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifest.getTargetLocation(setupFileUri) >> null
        thrown(SetUpJobException)
    }

    def "Malformed dependency URI"() {
        setup:
        jobSetup = Optional.of("://")

        when:
        File jobDirectory = service.createJobDirectory(spec)
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        def e = thrown(SetUpJobException)
        e.getCause().getClass() == URISyntaxException
    }

    def "Download exception"() {
        setup:

        when:
        File jobDirectory = service.createJobDirectory(spec)
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest) >> { throw new DownloadException("") }
        def e = thrown(SetUpJobException)
        e.getCause().getClass() == DownloadException
    }

    def "Setup script error"() {
        setup:
        def setupFileUri = URI.create("s3://my-bucket/my-org/my-job/setup.sh")
        jobSetup = Optional.of(setupFileUri.toString())
        dummyFile.write("exit 1\n")

        when:
        File jobDirectory = service.createJobDirectory(spec)
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)
        Map<String, String> jobEnvironment = service.setupJobEnvironment(jobDirectory, spec, setupFiles)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifest.getTargetLocation(setupFileUri) >> dummyFile
        thrown(SetUpJobException)
    }

    def "Environment file parse error"() {
        setup:
        def setupFileUri = URI.create("s3://my-bucket/my-org/my-job/setup.sh")
        jobSetup = Optional.of(setupFileUri.toString())

        def envFile = PathUtils.composePath(
            PathUtils.jobGenieDirectoryPath(jobDir),
            JobConstants.GENIE_AGENT_ENV_SCRIPT_OUTPUT_FILE_NAME
        ).toFile()

        dummyFile.write("echo \"syntax error!\" >> " + envFile.getAbsolutePath() + "\n")

        when:
        File jobDirectory = service.createJobDirectory(spec)
        List<File> setupFiles = service.downloadJobResources(spec, jobDirectory)
        Map<String, String> jobEnvironment = service.setupJobEnvironment(jobDirectory, spec, setupFiles)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifest.getTargetLocation(setupFileUri) >> dummyFile
        def e = thrown(SetUpJobException)
        e.getCause().getClass() == EnvUtils.ParseException
    }

    def "Skip cleanup"() {
        setup:
        File[] dependencies = [
            jobId + "/genie/command/presto0180/dependencies/presto-wrapper.py",
            jobId + "/genie/applications/presto0180/dependencies/presto.tar.gz",
            jobId + "/genie/cluster/presto-v005/dependencies/presto-v005.txt",
        ].collect { new File(temporaryFolder.getRoot(), it) }

        File[] otherFiles = [
            jobId + "/run",
            jobId + "/genie/logs/genie.log",
            jobId + "/genie/logs/env.log",
            jobId + "/genie/genie.done",
            jobId + "/stdout",
            jobId + "/stderr"
        ].collect { new File(temporaryFolder.getRoot(), it) }

        def allFiles = dependencies + otherFiles as File[]

        allFiles.each {
            file ->
                println "Creating dir " + file.getParentFile().getAbsolutePath()
                Files.createDirectories(file.getParentFile().toPath())
                println "Creating file " + file.getAbsolutePath()
                Files.createFile(file.toPath())
        }

        File jobDirectory = new File(temporaryFolder.getRoot(), jobId)

        when:
        service.cleanupJobDirectory(jobDirectory.toPath(), CleanupStrategy.NO_CLEANUP)

        then:
        allFiles.each {
                // Check all directories not deleted, even the empty dependencies one
            file -> assert file.exists()
        }
    }

    def "Full cleanup"() {
        setup:
        File[] dependencies = [
            jobId + "/genie/command/presto0180/dependencies/presto-wrapper.py",
            jobId + "/genie/applications/presto0180/dependencies/presto.tar.gz",
            jobId + "/genie/cluster/presto-v005/dependencies/presto-v005.txt",
        ].collect { new File(temporaryFolder.getRoot(), it) }

        File[] otherFiles = [
            jobId + "/run",
            jobId + "/genie/logs/genie.log",
            jobId + "/genie/logs/env.log",
            jobId + "/genie/genie.done",
            jobId + "/stdout",
            jobId + "/stderr"
        ].collect { new File(temporaryFolder.getRoot(), it) }

        def allFiles = dependencies + otherFiles as File[]

        allFiles.each {
            file ->
                println "Creating dir " + file.getParentFile().getAbsolutePath()
                Files.createDirectories(file.getParentFile().toPath())
                println "Creating file " + file.getAbsolutePath()
                Files.createFile(file.toPath())
        }

        File jobDirectory = new File(temporaryFolder.getRoot(), jobId)

        when:
        service.cleanupJobDirectory(jobDirectory.toPath(), CleanupStrategy.FULL_CLEANUP)

        then:
        !jobDirectory.exists()
    }

    def "Dependencies cleanup"() {
        setup:
        File[] dependencies = [
            jobId + "/genie/command/presto0180/dependencies/presto-wrapper.py",
            jobId + "/genie/applications/presto0180/dependencies/presto.tar.gz",
            jobId + "/genie/cluster/presto-v005/dependencies/presto-v005.txt",
        ].collect { new File(temporaryFolder.getRoot(), it) }

        File[] otherFiles = [
            jobId + "/run",
            jobId + "/genie/logs/genie.log",
            jobId + "/genie/logs/env.log",
            jobId + "/genie/applications/presto0180/config/presto.cfg",
            jobId + "/genie/applications/presto0180/setup.sh",
            jobId + "/genie/command/presto0180/config/presto-wrapper-config.py",
            jobId + "/genie/command/presto0180/setup.sh",
            jobId + "/genie/cluster/presto-v005/config/presto-v005.cfg",
            jobId + "/genie/genie.done",
            jobId + "/stdout",
            jobId + "/stderr",
            jobId + "/script.presto",
            "dependencies/foo.txt"
        ].collect { new File(temporaryFolder.getRoot(), it) }

        def allFiles = dependencies + otherFiles as File[]

        allFiles.each {
            file ->
                println "Creating dir " + file.getParentFile().getAbsolutePath()
                Files.createDirectories(file.getParentFile().toPath())
                println "Creating file " + file.getAbsolutePath()
                Files.createFile(file.toPath())
        }

        File jobDirectory = new File(temporaryFolder.getRoot(), jobId)

        when:
        service.cleanupJobDirectory(jobDirectory.toPath(), CleanupStrategy.DEPENDENCIES_CLEANUP)

        then:
        dependencies.each {
                // Check all dependencies deleted
            file -> assert !file.exists()
        }

        otherFiles.each {
                // Check all other files not deleted
            file -> assert file.exists()
        }

        allFiles.each {
                // Check all directories not deleted, even the empty dependencies one
            file -> assert file.getParentFile().exists()
        }
    }

}
