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

package com.netflix.genie.agent.execution.statemachine.actions

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.ChangeJobStatusException
import com.netflix.genie.agent.execution.exceptions.DownloadException
import com.netflix.genie.agent.execution.exceptions.SetUpJobException
import com.netflix.genie.agent.execution.services.AgentJobService
import com.netflix.genie.agent.execution.services.DownloadService
import com.netflix.genie.agent.execution.statemachine.Events
import com.netflix.genie.agent.utils.EnvUtils
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.internal.dto.v4.ExecutionEnvironment
import com.netflix.genie.common.internal.dto.v4.JobSpecification
import com.netflix.genie.common.internal.jobs.JobConstants
import com.netflix.genie.test.categories.UnitTest
import org.assertj.core.util.Sets
import org.junit.Rule
import org.junit.experimental.categories.Category
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@Category(UnitTest.class)
class SetUpJobActionSpec extends Specification {
    @Rule
    TemporaryFolder temporaryFolder

    ExecutionContext executionContext

    Set<URI> manifestUris
    File dummyFile

    DownloadService.Manifest manifest
    DownloadService.Manifest.Builder manifestBuilder
    DownloadService downloadService
    AgentJobService agentJobService
    SetUpJobAction action

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

        this.temporaryFolder.create()
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

        this.agentJobService = Mock(AgentJobService)

        this.jobId = UUID.randomUUID().toString()
        this.jobDir = new File(temporaryFolder.getRoot(), jobId)
        this.jobSetup = Optional.empty()
        this.jobConfigs = []
        this.jobDeps = []
        this.jobEnv = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> {return jobSetup}
            _ * getConfigs() >> {return jobConfigs}
            _ * getDependencies() >> {return jobDeps}
        }
        this.job = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> {return jobId}
            _ * getExecutionEnvironment() >> {return jobEnv}
        }

        def app1Id = UUID.randomUUID().toString()
        this.app1Setup = Optional.empty()
        this.app1Configs = []
        this.app1Deps = []
        this.app1Env = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> {return app1Setup}
            _ * getConfigs() >> {return app1Configs}
            _ * getDependencies() >> {return app1Deps}
        }
        this.app1 = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> {return app1Id}
            _ * getExecutionEnvironment() >> {return app1Env}
        }
        this.app1Dir = PathUtils.jobApplicationDirectoryPath(jobDir, app1.getId()).toFile()

        def app2Id = UUID.randomUUID().toString()
        this.app2Setup = Optional.empty()
        this.app2Configs = []
        this.app2Deps = []
        this.app2Env = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> {return app2Setup}
            _ * getConfigs() >> {return app2Configs}
            _ * getDependencies() >> {return app2Deps}
        }
        this.app2 = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> {return app2Id}
            _ * getExecutionEnvironment() >> {return app2Env}
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
            _ * getSetupFile() >> {return clusterSetup}
            _ * getConfigs() >> {return clusterConfigs}
            _ * getDependencies() >> {return clusterDeps}
        }
        this.cluster = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> {return clusterId}
            _ * getExecutionEnvironment() >> {return clusterEnv}
        }
        this.clusterDir = PathUtils.jobClusterDirectoryPath(jobDir, cluster.getId()).toFile()

        def commandId = UUID.randomUUID().toString()
        this.commandSetup = Optional.empty()
        this.commandConfigs = []
        this.commandDeps = []
        this.commandEnv = Mock(ExecutionEnvironment) {
            _ * getSetupFile() >> {return commandSetup}
            _ * getConfigs() >> {return commandConfigs}
            _ * getDependencies() >> {return commandDeps}
        }
        this.command = Mock(JobSpecification.ExecutionResource) {
            _ * getId() >> {return commandId}
            _ * getExecutionEnvironment() >> {return commandEnv}
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

        this.action = new SetUpJobAction(executionContext, downloadService, agentJobService)
    }

    void cleanup() {
    }

    def "Execute w/o dependencies and environment"() {
        setup:
        Map<String, String> envMap

        when:
        def event = action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        manifestBuilder.build() >> manifest
        downloadService.download(manifest)
        executionContext.setJobEnvironment(_ as Map<String, String>) >> { args ->
            envMap = (Map<String, String>) args.getAt(0)
        }

        expect:
        event == Events.SETUP_JOB_COMPLETE
        envMap != null
        envMap.get(JobConstants.GENIE_JOB_DIR_ENV_VAR) == jobDir.toString()
        envMap.get(JobConstants.GENIE_APPLICATION_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.APPLICATION_PATH_VAR
        envMap.get(JobConstants.GENIE_COMMAND_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.COMMAND_PATH_VAR + "/" + command.getId()
        envMap.get(JobConstants.GENIE_CLUSTER_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.CLUSTER_PATH_VAR + "/" + cluster.getId()

        for (File entityDir : [jobDir, app1Dir, app2Dir, clusterDir, commandDir]) {
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
        Map<String, String> envMap
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
                        + "touch created-by-setup-file.txt"
        )

        when:
        def event = action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        manifestBuilder.build() >> manifest
        downloadService.download(manifest)
        executionContext.setJobEnvironment(_ as Map<String, String>) >> { args ->
            envMap = (Map<String, String>) args.getAt(0)
        }
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, app1Dir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, app2Dir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, commandDir)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, clusterDir)
        5 * manifest.getTargetLocation(setupFileUri) >> dummyFile
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(jobDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(app1Dir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(app2Dir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(commandDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(dependencyUri, new File(clusterDir, JobConstants.DEPENDENCY_FILE_PATH_PREFIX))
        0 * manifest.getTargetLocation(dependencyUri)
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(jobDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(app1Dir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(app2Dir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(commandDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        1 * manifestBuilder.addFileWithTargetDirectory(configUri, new File(clusterDir, JobConstants.CONFIG_FILE_PATH_PREFIX))
        0 * manifest.getTargetLocation(configUri)

        expect:
        event == Events.SETUP_JOB_COMPLETE
        jobDir.exists()
        app1Dir.exists()
        app2Dir.exists()
        clusterDir.exists()
        commandDir.exists()
        envMap != null
        envMap.get(JobConstants.GENIE_JOB_DIR_ENV_VAR) == jobDir.toString()
        envMap.get(JobConstants.GENIE_APPLICATION_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.APPLICATION_PATH_VAR
        envMap.get(JobConstants.GENIE_COMMAND_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.COMMAND_PATH_VAR + "/" + command.getId()
        envMap.get(JobConstants.GENIE_CLUSTER_DIR_ENV_VAR) == jobDir.toString() + "/" + JobConstants.GENIE_PATH_VAR + "/" + JobConstants.CLUSTER_PATH_VAR + "/" + cluster.getId()
        envMap.get("SERVER_VARIABLE_KEY") == "SERVER VARIABLE VALUE"
        envMap.get("SETUP_VARIABLE_KEY") == "SETUP VARIABLE VALUE"
        new File(jobDir, "created-by-setup-file.txt").exists()
        PathUtils.composePath(
                PathUtils.jobGenieDirectoryPath(jobDir),
                JobConstants.LOGS_PATH_VAR,
                JobConstants.GENIE_AGENT_ENV_SCRIPT_LOG_FILE_NAME
        ).toFile().getText(StandardCharsets.UTF_8.toString()).count("Hello World") == 5
    }

    def "Change job status exception"() {
        Exception exception = new ChangeJobStatusException("...")

        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getClaimedJobId() >> jobId
        1 * executionContext.getCurrentJobStatus() >> JobStatus.CLAIMED
        1 * agentJobService.changeJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, _ as String) >> { throw exception }
        def e = thrown(RuntimeException)
        e.getCause() == exception
    }

    def "Null job spec"() {
        when:
        action.executeStateAction(executionContext)

        then:
        1 * executionContext.getClaimedJobId() >> jobId
        1 * executionContext.getCurrentJobStatus() >> JobStatus.CLAIMED
        1 * agentJobService.changeJobStatus(jobId, JobStatus.CLAIMED, JobStatus.INIT, _ as String)
        1 * executionContext.getJobSpecification() >> null
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
    }

    def "Non-existent job directory parent"() {
        setup:

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        spec.getJobDirectoryLocation() >> new File(temporaryFolder.getRoot(), "nonexistent")
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
    }

    def "Invalid job directory parent"() {
        setup:

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        spec.getJobDirectoryLocation() >> temporaryFolder.newFile()
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
    }

    def "Relative job directory parent"() {
        setup:

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        spec.getJobDirectoryLocation() >> new File(".")
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
    }

    def "Existing job directory"() {
        setup:
        temporaryFolder.newFolder(jobId)

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
    }

    def "SetUp file lookup error"() {
        setup:
        def setupFileUri = URI.create("s3://my-bucket/my-org/my-job/setup.sh")
        jobSetup = Optional.of(setupFileUri.toString())

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        manifestBuilder.build() >> manifest
        downloadService.download(manifest)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifest.getTargetLocation(setupFileUri) >> null
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
    }

    def "Malformed dependency URI"() {
        setup:
        jobSetup = Optional.of("://")

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
        e.getCause().getCause().getClass() == URISyntaxException
    }

    def "Download exception"() {
        setup:

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        manifestBuilder.build() >> manifest
        downloadService.download(manifest) >> {throw new DownloadException("")}
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
        e.getCause().getCause().getClass() == DownloadException
    }

    def "Setup script error"() {
        setup:
        def setupFileUri = URI.create("s3://my-bucket/my-org/my-job/setup.sh")
        jobSetup = Optional.of(setupFileUri.toString())
        dummyFile.write("exit 1\n")

        when:
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        manifestBuilder.build() >> manifest
        downloadService.download(manifest)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifest.getTargetLocation(setupFileUri) >> dummyFile
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
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
        action.executeStateAction(executionContext)

        then:
        executionContext.getClaimedJobId() >> jobId
        executionContext.getJobSpecification() >> spec
        executionContext.setJobDirectory(jobDir)
        downloadService.newManifestBuilder() >> manifestBuilder
        manifestBuilder.build() >> manifest
        downloadService.download(manifest)
        1 * manifestBuilder.addFileWithTargetDirectory(setupFileUri, jobDir)
        1 * manifest.getTargetLocation(setupFileUri) >> dummyFile
        def e = thrown(RuntimeException)
        e.getCause().getClass() == SetUpJobException
        e.getCause().getCause().getClass() == EnvUtils.ParseException
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

        when:
        action.cleanup()

        then:
        1 * executionContext.getJobDirectory() >> new File(temporaryFolder.getRoot(), jobId)

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
