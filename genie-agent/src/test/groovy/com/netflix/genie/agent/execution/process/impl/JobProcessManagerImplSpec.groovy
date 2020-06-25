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
package com.netflix.genie.agent.execution.process.impl

import com.netflix.genie.agent.execution.exceptions.JobLaunchException
import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.process.JobProcessResult
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.external.dtos.v4.JobStatus
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.Unroll
import spock.util.environment.OperatingSystem

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.ScheduledFuture

@IgnoreIf({ OperatingSystem.getCurrent().isWindows() })
class JobProcessManagerImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    File stdOut
    File stdErr
    TaskScheduler scheduler
    JobProcessManager manager

    void setup() {
        this.stdOut = PathUtils.jobStdOutPath(temporaryFolder.getRoot()).toFile()
        this.stdErr = PathUtils.jobStdErrPath(temporaryFolder.getRoot()).toFile()
        Files.createDirectories(this.stdOut.getParentFile().toPath())
        Files.createDirectories(this.stdErr.getParentFile().toPath())
        this.scheduler = Mock(TaskScheduler)
        this.manager = new JobProcessManagerImpl(this.scheduler)
    }

    void cleanup() {
    }

    @Unroll
    def "LaunchProcess, (interactive: #interactive)"() {
        File expectedFile = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString())
        assert !expectedFile.exists()

        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.append("echo Hello stdout;\n");
        jobScript.append("echo Hello stderr 1>&2;\n");
        jobScript.append("touch " + expectedFile + ";\n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            interactive,
            null,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getExitCode() == 0
        expectedFile.exists()
        this.stdErr.exists() == !interactive
        this.stdOut.exists() == !interactive
        if (!interactive) {
            assert this.stdOut.getText(StandardCharsets.UTF_8.toString()).contains("Hello stdout")
            assert this.stdErr.getText(StandardCharsets.UTF_8.toString()).contains("Hello stderr")
        }

        where:
        interactive | _
        true        | _
        false       | _
    }

    def "LaunchProcess changing directory before launch"() {
        String expectedString = this.temporaryFolder.getRoot().getPath()

        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("pwd\n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            false,
            null,
            true
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getExitCode() == 0
        this.stdErr.exists()
        this.stdOut.exists()
        this.stdOut.getText(StandardCharsets.UTF_8.toString()).contains(expectedString)
    }

    def "LaunchProcess script error"() {
        File nonExistentFile = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString())

        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("cat " + nonExistentFile + "\n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            false,
            null,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.FAILED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FAILED
        result.getExitCode() == 1
        this.stdErr.exists()
        this.stdOut.exists()
        this.stdErr.getText(StandardCharsets.UTF_8.toString()).contains("No such file or directory")
    }

    def "LaunchProcess script setup error"() {
        File nonExistentFile = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString())

        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("source " + nonExistentFile + "\n");
        File setupFailedFile = PathUtils.jobSetupErrorMarkerFilePath(this.temporaryFolder.getRoot()).toFile()
        Files.createDirectories(setupFailedFile.getParentFile().toPath())
        setupFailedFile.write("See setup log for details")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            false,
            null,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.FAILED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_SETUP_FAILED
        result.getExitCode() != 0
        this.stdErr.exists()
        this.stdOut.exists()
        !this.stdErr.getText(StandardCharsets.UTF_8.toString()).isEmpty()
    }

    def "Job directory null"() {
        when:
        this.manager.launchProcess(
            null,
            Mock(File),
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job directory not a directory"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.newFile("foo"),
            Mock(File),
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job folder not existing"() {
        when:
        this.manager.launchProcess(
            new File(this.temporaryFolder.getRoot(), "foo"),
            Mock(File),
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Script is null"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            null,
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Script does not exist"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            null,
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Script is not not executable"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("echo hello world")

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            null,
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Kill running process"() {
        def future = Mock(ScheduledFuture)

        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("sleep 60 \n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            59,
            false
        )

        then:
        noExceptionThrown()
        1 * this.scheduler.schedule(_ as Runnable, _ as Instant) >> future

        when:
        this.manager.kill(KillService.KillSource.API_KILL_REQUEST)

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getExitCode() == 143
        !this.stdErr.exists()
        !this.stdOut.exists()
        1 * future.cancel(true)
    }

    @Unroll
    def "Kill running process via event (source: #killSource)"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("sleep 60 \n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            null,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.onApplicationEvent(new KillService.KillEvent(killSource))

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == expectedStatusMessage
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        result.getExitCode() == 143
        !this.stdErr.exists()
        !this.stdOut.exists()

        where:
        killSource                              | expectedStatusMessage
        KillService.KillSource.TIMEOUT          | JobStatusMessages.JOB_EXCEEDED_TIMEOUT
        KillService.KillSource.FILES_LIMIT      | JobStatusMessages.JOB_EXCEEDED_FILES_LIMIT
        KillService.KillSource.API_KILL_REQUEST | JobStatusMessages.JOB_KILLED_BY_USER
        KillService.KillSource.SYSTEM_SIGNAL    | JobStatusMessages.JOB_KILLED_BY_USER
    }

    // TODO: This test seems to verify incorrect behavior
    //       Why should a job that is completed successfully be marked as killed instead of successful? we should fix
    //       this case
    def "Kill completed process"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("echo\n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            null,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.kill(KillService.KillSource.SYSTEM_SIGNAL)

        then:
        noExceptionThrown()

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        // To document non-deterministic behavior that needs to be fixed
        result.getExitCode() == 143 || result.getExitCode() == 0
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    def "Skip process launch"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("echo\n");
        jobScript.setExecutable(true)

        when:
        this.manager.kill(KillService.KillSource.API_KILL_REQUEST)

        then:
        noExceptionThrown()

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            10,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        result.getExitCode() == 0
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    def "Double launch"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("echo\n");
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            null,
            false
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            null,
            false
        )

        then:
        thrown(IllegalStateException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job was never launched but someone calls wait for"() {
        when:
        this.manager.waitFor()

        then:
        thrown(IllegalStateException)
    }

    def "Job killed due to timeout"() {
        File jobScript = new File(this.temporaryFolder.getRoot(), "run")
        jobScript.write("sleep 60\n");
        jobScript.setExecutable(true)

        // This really are more of an integration test...
        def threadPoolScheduler = new ThreadPoolTaskScheduler()
        threadPoolScheduler.setPoolSize(1)
        threadPoolScheduler.setThreadNamePrefix("job-process-manager-impl-spec-")
        threadPoolScheduler.setWaitForTasksToCompleteOnShutdown(false)
        threadPoolScheduler.initialize()
        def realManager = new JobProcessManagerImpl(threadPoolScheduler)

        when:
        realManager.launchProcess(
            this.temporaryFolder.getRoot(),
            jobScript,
            true,
            1,
            false
        )

        then:
        noExceptionThrown()

        when:
        def result = realManager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_EXCEEDED_TIMEOUT
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        result.getExitCode() == 143
        !this.stdErr.exists()
        !this.stdOut.exists()

        cleanup:
        threadPoolScheduler.shutdown()
    }
}
