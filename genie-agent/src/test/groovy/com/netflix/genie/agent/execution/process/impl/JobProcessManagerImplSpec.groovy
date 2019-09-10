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
import com.netflix.genie.common.dto.JobStatus
import com.netflix.genie.common.dto.JobStatusMessages
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.util.environment.OperatingSystem

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.ScheduledFuture

@IgnoreIf({ OperatingSystem.getCurrent().isWindows() })
class JobProcessManagerImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    Map<String, String> envMap
    File stdOut
    File stdErr
    TaskScheduler scheduler
    JobProcessManager manager

    void setup() {
        this.envMap = [:]
        this.stdOut = PathUtils.jobStdOutPath(temporaryFolder.getRoot()).toFile()
        this.stdErr = PathUtils.jobStdErrPath(temporaryFolder.getRoot()).toFile()
        Files.createDirectories(this.stdOut.getParentFile().toPath())
        Files.createDirectories(this.stdErr.getParentFile().toPath())
        this.scheduler = Mock(TaskScheduler)
        this.manager = new JobProcessManagerImpl(this.scheduler)
    }

    void cleanup() {
    }

    def "LaunchProcess interactive"() {
        File expectedFile = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString())
        this.envMap.put("PATH", System.getenv("PATH") + ":/foo")

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["touch", expectedFile.getAbsolutePath()],
            true,
            null
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        expectedFile.exists()
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    def "LaunchProcess noninteractive with variable expansion"() {
        String helloWorld = "Hello World!"
        this.envMap.put("ECHO_COMMAND", "echo")

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["\${ECHO_COMMAND}", helloWorld],
            false,
            null
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        this.stdErr.exists()
        this.stdOut.exists()
        this.stdOut.getText(StandardCharsets.UTF_8.toString()).contains(helloWorld)
    }

    def "LaunchProcess noninteractive and check environment env"() {
        String uuid = UUID.randomUUID().toString()
        this.envMap.put("GENIE_UUID", uuid)
        String expectedString = "GENIE_UUID=" + uuid

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["env"],
            false,
            null
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        this.stdErr.exists()
        this.stdOut.exists()
        this.stdOut.getText(StandardCharsets.UTF_8.toString()).contains(expectedString)
    }

    def "LaunchProcess command error"() {
        File nonExistentFile = new File(this.temporaryFolder.getRoot(), UUID.randomUUID().toString())

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["rm", nonExistentFile.absolutePath],
            false,
            null
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.FAILED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FAILED
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        this.stdErr.exists()
        this.stdOut.exists()
        this.stdErr.getText(StandardCharsets.UTF_8.toString()).contains("No such file or directory")
    }

    def "LaunchProcess missing executable"() {
        String uuid = UUID.randomUUID().toString()

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            [uuid],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "LaunchProcess missing environment variable"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["\$COMMAND"],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job directory null"() {
        when:
        this.manager.launchProcess(
            null,
            this.envMap,
            ["echo"],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job directory not a directory"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.newFile("foo"),
            this.envMap,
            ["echo"],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job folder not existing"() {
        when:
        this.manager.launchProcess(
            new File(this.temporaryFolder.getRoot(), "foo"),
            this.envMap,
            ["echo"],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Environment null"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            null,
            ["echo"],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Args not set"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            null,
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Args empty"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            [],
            false,
            null
        )

        then:
        thrown(JobLaunchException)
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Kill running process"() {
        def future = Mock(ScheduledFuture)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["sleep", "60"],
            true,
            59
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
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        !this.stdErr.exists()
        !this.stdOut.exists()
        1 * future.cancel(true)
    }

    def "Kill running process via event"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["sleep", "60"],
            true,
            null
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.onApplicationEvent(new KillService.KillEvent(KillService.KillSource.API_KILL_REQUEST))

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    // TODO: This test seems to verify incorrect behavior
    //       Why should a job that is completed successfully be marked as killed instead of successful? we should fix
    //       this case
    def "Kill completed process"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["echo", "foo"],
            true,
            null
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
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    def "Skip process launch"() {
        when:
        this.manager.kill(KillService.KillSource.API_KILL_REQUEST)

        then:
        noExceptionThrown()

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["echo", "foo"],
            true,
            10
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
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    def "Double launch"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["echo", "foo"],
            true,
            null
        )

        then:
        noExceptionThrown()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["echo", "foo"],
            true,
            null
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
        // These really are more of an integration test...
        def threadPoolScheduler = new ThreadPoolTaskScheduler()
        threadPoolScheduler.setPoolSize(1)
        threadPoolScheduler.setThreadNamePrefix("job-process-manager-impl-spec-")
        threadPoolScheduler.setWaitForTasksToCompleteOnShutdown(false)
        threadPoolScheduler.initialize()
        def realManager = new JobProcessManagerImpl(threadPoolScheduler)

        when:
        realManager.launchProcess(
            this.temporaryFolder.getRoot(),
            this.envMap,
            ["sleep", "60"],
            true,
            1
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
        !this.stdErr.exists()
        !this.stdOut.exists()

        cleanup:
        threadPoolScheduler.shutdown()
    }
}
