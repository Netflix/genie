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
import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.agent.execution.exceptions.SetUpJobException
import com.netflix.genie.agent.execution.services.DownloadService
import com.netflix.genie.agent.execution.services.JobSetupService
import com.netflix.genie.agent.execution.statemachine.ExecutionContext
import com.netflix.genie.agent.properties.AgentProperties
import com.netflix.genie.agent.properties.JobSetupServiceProperties
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.external.dtos.v4.ExecutionEnvironment
import com.netflix.genie.common.external.dtos.v4.JobSpecification
import com.netflix.genie.common.internal.jobs.JobConstants
import org.springframework.core.io.ClassPathResource
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

class JobSetupServiceImplSpec extends Specification {
    @TempDir
    Path temporaryFolder

    JobSetupService service
    DownloadService downloadService

    ExecutionContext executionContext

    Set<URI> manifestUris

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
    JobSetupServiceProperties jobSetupProperties
    AgentProperties agentProperties

    def setup() {
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
        this.jobDir = this.temporaryFolder.resolve(jobId).toFile()
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
            _ * getJobDirectoryLocation() >> temporaryFolder.toFile()
            _ * getEnvironmentVariables() >> jobServerEnvMap
        }

        this.downloadService = Mock(DownloadService)

        this.agentProperties = new AgentProperties()
        this.jobSetupProperties = new JobSetupServiceProperties()
        this.agentProperties.setJobSetupService(jobSetupProperties)

        this.service = new JobSetupServiceImpl(downloadService, agentProperties)
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

        def testDirectory = Files.createDirectory(this.temporaryFolder.resolve("createJobDirectory")).toFile()

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

    def "Setup w/ dependencies"() {
        setup:
        jobServerEnvMap.put("SERVER_ENVIRONMENT_Z", "VALUE_Z")
        jobServerEnvMap.put("SERVER_ENVIRONMENT_X", "VALUE_X")
        jobServerEnvMap.put("SERVER_ENVIRONMENT_Y", "VALUE_Y")

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

        when:
        File jobDirectory = service.createJobDirectory(spec)
        service.downloadJobResources(spec, jobDirectory)
        File jobScript = service.createJobScript(spec, jobDirectory)

        then:
        jobDir == jobDirectory
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.addFileWithTargetFile(setupFileUri, new File(app1Dir, JobConstants.GENIE_ENTITY_SETUP_SCRIPT_FILE_NAME))
        1 * manifestBuilder.addFileWithTargetFile(setupFileUri, new File(app2Dir, JobConstants.GENIE_ENTITY_SETUP_SCRIPT_FILE_NAME))
        1 * manifestBuilder.addFileWithTargetFile(setupFileUri, new File(commandDir, JobConstants.GENIE_ENTITY_SETUP_SCRIPT_FILE_NAME))
        1 * manifestBuilder.addFileWithTargetFile(setupFileUri, new File(clusterDir, JobConstants.GENIE_ENTITY_SETUP_SCRIPT_FILE_NAME))
        1 * manifestBuilder.addFileWithTargetFile(setupFileUri, new File(jobDir, JobConstants.GENIE_ENTITY_SETUP_SCRIPT_FILE_NAME))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, jobDir)
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(app1Dir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(app2Dir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(commandDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(clusterDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, jobDir)
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(app1Dir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(app2Dir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(commandDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(clusterDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)
        1 * manifest.getTargetFiles()
        1 * spec.getExecutableArgs() >> ["presto", "-v"]
        1 * spec.getJobArgs() >> ["--exec", "'select * from table limit 1'"]

        expect:
        jobDir.exists()
        app1Dir.exists()
        app2Dir.exists()
        clusterDir.exists()
        commandDir.exists()
        jobScript.exists()
        jobScript.canExecute()
        def expectedScript = new ClassPathResource("JobSetupServiceImplSpec_run1.test.sh").getInputStream().getText()
            .replaceAll("<JOB_ID_PLACEHOLDER>", jobId)
            .replaceAll("<JOB_DIR_PLACEHOLDER>", jobDirectory.toPath().toAbsolutePath().toString())
            .replaceAll("<COMMAND_ID_PLACEHOLDER>", command.getId())
            .replaceAll("<CLUSTER_ID_PLACEHOLDER>", cluster.getId())
            .replaceAll("<JOB_ID_PLACEHOLDER>", job.getId())
            .replaceAll("<APPLICATION_1_PLACEHOLDER>", app1.getId())
            .replaceAll("<APPLICATION_2_PLACEHOLDER>", app2.getId())
        def actualScript = jobScript.getText(StandardCharsets.UTF_8.name())
        actualScript == expectedScript
    }

    def "Setup w/o dependencies"() {
        setup:
        jobServerEnvMap.put("SERVER_ENVIRONMENT_Z", "VALUE_Z")
        jobServerEnvMap.put("SERVER_ENVIRONMENT_X", "VALUE_X")
        jobServerEnvMap.put("SERVER_ENVIRONMENT_Y", "VALUE_Y")

        when:
        File jobDirectory = service.createJobDirectory(spec)
        service.downloadJobResources(spec, jobDirectory)
        File jobScript = service.createJobScript(spec, jobDirectory)

        then:
        jobDir == jobDirectory
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        0 * manifestBuilder.addFileWithTargetFile(_, _)
        0 * manifestBuilder.addFileWithTargetDirectory(_, _)
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)
        1 * manifest.getTargetFiles()
        1 * spec.getExecutableArgs() >> ["presto", "-v"]
        1 * spec.getJobArgs() >> ["--exec", "'select * from table limit 10'"]

        expect:
        jobDir.exists()
        app1Dir.exists()
        app2Dir.exists()
        clusterDir.exists()
        commandDir.exists()
        jobScript.exists()
        jobScript.canExecute()
        def expectedScript = new ClassPathResource("JobSetupServiceImplSpec_run2.test.sh").getInputStream().getText()
            .replaceAll("<JOB_ID_PLACEHOLDER>", jobId)
            .replaceAll("<JOB_DIR_PLACEHOLDER>", jobDirectory.toPath().toAbsolutePath().toString())
            .replaceAll("<COMMAND_ID_PLACEHOLDER>", command.getId())
            .replaceAll("<CLUSTER_ID_PLACEHOLDER>", cluster.getId())
            .replaceAll("<JOB_ID_PLACEHOLDER>", job.getId())
            .replaceAll("<APPLICATION_1_PLACEHOLDER>", app1.getId())
            .replaceAll("<APPLICATION_2_PLACEHOLDER>", app2.getId())
        def actualScript = jobScript.getText(StandardCharsets.UTF_8.name())
        actualScript == expectedScript
    }

    @Unroll
    def "Setup w/ environment filter: #expectedFilter"() {
        setup:
        jobServerEnvMap.put("SERVER_ENVIRONMENT_Z", "VALUE_Z")
        jobServerEnvMap.put("SERVER_ENVIRONMENT_X", "VALUE_X")
        jobServerEnvMap.put("SERVER_ENVIRONMENT_Y", "VALUE_Y")

        if (expression != null) {
            this.jobSetupProperties.setEnvironmentDumpFilterExpression(expression)
        }

        if (inverted != null) {
            this.jobSetupProperties.setEnvironmentDumpFilterInverted((Boolean) inverted)
        }

        when:
        File jobDirectory = service.createJobDirectory(spec)
        service.downloadJobResources(spec, jobDirectory)
        File jobScript = service.createJobScript(spec, jobDirectory)

        then:
        jobDir == jobDirectory
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        0 * manifestBuilder.addFileWithTargetFile(_, _)
        0 * manifestBuilder.addFileWithTargetDirectory(_, _)
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest)
        1 * manifest.getTargetFiles()
        1 * spec.getExecutableArgs() >> ["presto", "-v"]
        1 * spec.getJobArgs() >> ["--exec", "'select * from table limit 10'"]

        expect:
        jobDir.exists()
        app1Dir.exists()
        app2Dir.exists()
        clusterDir.exists()
        commandDir.exists()
        jobScript.exists()
        jobScript.canExecute()
        def expectedScript = new ClassPathResource("JobSetupServiceImplSpec_run3.test.sh").getInputStream().getText()
            .replaceAll("<JOB_ID_PLACEHOLDER>", jobId)
            .replaceAll("<JOB_DIR_PLACEHOLDER>", jobDirectory.toPath().toAbsolutePath().toString())
            .replaceAll("<COMMAND_ID_PLACEHOLDER>", command.getId())
            .replaceAll("<CLUSTER_ID_PLACEHOLDER>", cluster.getId())
            .replaceAll("<JOB_ID_PLACEHOLDER>", job.getId())
            .replaceAll("<APPLICATION_1_PLACEHOLDER>", app1.getId())
            .replaceAll("<APPLICATION_2_PLACEHOLDER>", app2.getId())
            .replaceAll("<ENVIRONMENT_FILTER_PLACEHOLDER>", expectedFilter)
        def actualScript = jobScript.getText(StandardCharsets.UTF_8.name())
        actualScript == expectedScript

        where:
        expression             | inverted | expectedFilter
        null                   | null     | "grep -E --regex='.*'" // null -> use default properties value
        ".*"                   | false    | "grep -E --regex='.*'" // Dump everything
        ".*"                   | true     | "grep -E --invert-match --regex='.*'" // Dump nothing
        "(FOO|BAR).*"          | false    | "grep -E --regex='(FOO|BAR).*'"
        "[Ff]oo\\.[Bb]ar\\..*" | true     | "grep -E --invert-match --regex='[Ff]oo\\\\.[Bb]ar\\\\..*'"
    }

    def "Malformed dependency URI"() {
        setup:
        jobSetup = Optional.of("://")

        when:
        File jobDirectory = service.createJobDirectory(spec)
        service.downloadJobResources(spec, jobDirectory)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        def e = thrown(SetUpJobException)
        e.getCause().getClass() == URISyntaxException
    }

    def "Download exception"() {
        setup:

        when:
        File jobDirectory = service.createJobDirectory(spec)
        service.downloadJobResources(spec, jobDirectory)

        then:
        1 * downloadService.newManifestBuilder() >> manifestBuilder
        1 * manifestBuilder.build() >> manifest
        1 * downloadService.download(manifest) >> { throw new DownloadException("") }
        def e = thrown(SetUpJobException)
        e.getCause().getClass() == DownloadException
    }

    def "Skip cleanup"() {
        setup:
        File[] dependencies = [
            jobId + "/genie/command/presto0180/dependencies/presto-wrapper.py",
            jobId + "/genie/applications/presto0180/dependencies/presto.tar.gz",
            jobId + "/genie/cluster/presto-v005/dependencies/presto-v005.txt",
        ].collect { this.temporaryFolder.resolve(it).toFile() }

        File[] otherFiles = [
            jobId + "/run",
            jobId + "/genie/logs/genie.log",
            jobId + "/genie/logs/env.log",
            jobId + "/genie/genie.done",
            jobId + "/stdout",
            jobId + "/stderr"
        ].collect { this.temporaryFolder.resolve(it).toFile() }

        def allFiles = dependencies + otherFiles as File[]

        allFiles.each {
            file ->
                println "Creating dir " + file.getParentFile().getAbsolutePath()
                Files.createDirectories(file.getParentFile().toPath())
                println "Creating file " + file.getAbsolutePath()
                Files.createFile(file.toPath())
        }

        File jobDirectory = this.temporaryFolder.resolve(jobId).toFile()

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
        ].collect { this.temporaryFolder.resolve(it).toFile() }

        File[] otherFiles = [
            jobId + "/run",
            jobId + "/genie/logs/genie.log",
            jobId + "/genie/logs/env.log",
            jobId + "/genie/genie.done",
            jobId + "/stdout",
            jobId + "/stderr"
        ].collect { this.temporaryFolder.resolve(it).toFile() }

        def allFiles = dependencies + otherFiles as File[]

        allFiles.each {
            file ->
                println "Creating dir " + file.getParentFile().getAbsolutePath()
                Files.createDirectories(file.getParentFile().toPath())
                println "Creating file " + file.getAbsolutePath()
                Files.createFile(file.toPath())
        }

        File jobDirectory = this.temporaryFolder.resolve(jobId).toFile()

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
        ].collect { this.temporaryFolder.resolve(it).toFile() }

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
        ].collect { this.temporaryFolder.resolve(it).toFile() }

        def allFiles = dependencies + otherFiles as File[]

        allFiles.each {
            file ->
                println "Creating dir " + file.getParentFile().getAbsolutePath()
                Files.createDirectories(file.getParentFile().toPath())
                println "Creating file " + file.getAbsolutePath()
                Files.createFile(file.toPath())
        }

        File jobDirectory = this.temporaryFolder.resolve(jobId).toFile()

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
