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

import brave.Span
import brave.Tracer
import brave.propagation.TraceContext
import com.netflix.genie.agent.execution.exceptions.JobLaunchException
import com.netflix.genie.agent.execution.process.JobProcessManager
import com.netflix.genie.agent.execution.process.JobProcessResult
import com.netflix.genie.agent.execution.services.KillService
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.dto.JobStatusMessages
import com.netflix.genie.common.external.dtos.v4.JobStatus
import com.netflix.genie.common.internal.tracing.brave.BraveTagAdapter
import com.netflix.genie.common.internal.tracing.brave.BraveTracePropagator
import com.netflix.genie.common.internal.tracing.brave.BraveTracingCleanup
import com.netflix.genie.common.internal.tracing.brave.BraveTracingComponents
import org.springframework.core.io.ClassPathResource
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import spock.lang.IgnoreIf
import spock.lang.Specification
import spock.lang.TempDir
import spock.lang.Unroll
import spock.util.environment.OperatingSystem

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.concurrent.ScheduledFuture

@IgnoreIf({ OperatingSystem.getCurrent().isWindows() })
class JobProcessManagerImplSpec extends Specification {

    @TempDir
    Path temporaryFolder

    File stdOut
    File stdErr
    TaskScheduler scheduler
    JobProcessManager manager
    Tracer tracer
    BraveTracePropagator tracePropagator
    Span span
    TraceContext traceContext

    def setup() {
        this.stdOut = PathUtils.jobStdOutPath(temporaryFolder.toFile()).toFile()
        this.stdErr = PathUtils.jobStdErrPath(temporaryFolder.toFile()).toFile()
        Files.createDirectories(this.stdOut.getParentFile().toPath())
        Files.createDirectories(this.stdErr.getParentFile().toPath())
        this.scheduler = Mock(TaskScheduler)
        this.tracer = Mock(Tracer)
        this.tracePropagator = Mock(BraveTracePropagator)
        UUID uuid = UUID.randomUUID()
        this.traceContext = TraceContext.newBuilder()
            .traceId(uuid.getLeastSignificantBits())
            .traceIdHigh(uuid.getMostSignificantBits())
            .spanId(UUID.randomUUID().getLeastSignificantBits())
            .sampled(true)
            .build()
        this.span = Mock(Span)
        this.manager = new JobProcessManagerImpl(
            this.scheduler,
            new BraveTracingComponents(
                this.tracer,
                this.tracePropagator,
                Mock(BraveTracingCleanup),
                Mock(BraveTagAdapter)
            )
        )
    }

    def cleanup() {
    }

    @Unroll
    def "LaunchProcess, (interactive: #interactive)"() {
        File expectedFile = this.temporaryFolder.resolve(UUID.randomUUID().toString()).toFile()
        assert !expectedFile.exists()

        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.append("echo Hello stdout;\n")
        jobScript.append("echo Hello stderr 1>&2;\n")
        jobScript.append("touch " + expectedFile + ";\n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            interactive,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
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
        String expectedString = this.temporaryFolder

        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("pwd\n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            false,
            null,
            true
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> ["HI": "bye"]
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
        File nonExistentFile = this.temporaryFolder.resolve(UUID.randomUUID().toString()).toFile()

        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("cat " + nonExistentFile + "\n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            false,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> null
        0 * this.span.context()
        0 * this.tracePropagator.injectForJob(this.traceContext)
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
        File nonExistentFile = this.temporaryFolder.resolve(UUID.randomUUID().toString()).toFile()

        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("source " + nonExistentFile + "\n")
        File setupFailedFile = PathUtils.jobSetupErrorMarkerFilePath(this.temporaryFolder.toFile()).toFile()
        Files.createDirectories(setupFailedFile.getParentFile().toPath())
        setupFailedFile.write("See setup log for details")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            false,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
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
        0 * this.tracer.currentSpan()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job directory not a directory"() {
        when:
        this.manager.launchProcess(
            Files.createFile(this.temporaryFolder.resolve("foo")).toFile(),
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
            this.temporaryFolder.resolve("foo").toFile(),
            Mock(File),
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.tracer.currentSpan()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Script is null"() {
        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            null,
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.tracer.currentSpan()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Script does not exist"() {
        File jobScript = this.temporaryFolder.resolve("run").toFile()

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.tracer.currentSpan()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Script is not not executable"() {
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("echo hello world")

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            false,
            null,
            false
        )

        then:
        thrown(JobLaunchException)
        0 * this.tracer.currentSpan()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Kill running process"() {
        def future = Mock(ScheduledFuture)

        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("sleep 60 \n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            59,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
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
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("sleep 60 \n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.kill(killSource)

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
        killSource                                   | expectedStatusMessage
        KillService.KillSource.TIMEOUT               | JobStatusMessages.JOB_EXCEEDED_TIMEOUT
        KillService.KillSource.FILES_LIMIT           | JobStatusMessages.JOB_EXCEEDED_FILES_LIMIT
        KillService.KillSource.API_KILL_REQUEST      | JobStatusMessages.JOB_KILLED_BY_USER
        KillService.KillSource.REMOTE_STATUS_MONITOR | JobStatusMessages.JOB_MARKED_FAILED
    }

    @Unroll
    def "Kill running process with system signal (source: #interactive)"() {
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("sleep 60 \n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            interactive,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.kill(KillService.KillSource.SYSTEM_SIGNAL)

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.FAILED
        result.getFinalStatusMessage() == expectedStatusMessage
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        result.getExitCode() == 143

        where:
        interactive | expectedStatusMessage
        true        | JobStatusMessages.JOB_KILLED_BY_USER
        false       | JobStatusMessages.JOB_KILLED_BY_SYSTEM
    }

    def "Kill completed process"() {
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        Path touchedFile = temporaryFolder.resolve("touchz")
        jobScript.write("touch " + touchedFile)
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        // Wait until the process actually completes by checking the existence of the file
        while (Files.notExists(touchedFile)) {
            Thread.sleep(100)
        }

        // Kill after job completion
        when:
        this.manager.kill(KillService.KillSource.SYSTEM_SIGNAL)

        then:
        noExceptionThrown()

        when:
        def result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        // To document non-deterministic behavior that needs to be fixed
        result.getExitCode() == 143 || result.getExitCode() == 0
        !this.stdErr.exists()
        !this.stdOut.exists()
    }

    def "Skip process launch"() {
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("echo\n")
        jobScript.setExecutable(true)

        when:
        this.manager.kill(KillService.KillSource.API_KILL_REQUEST)

        then:
        noExceptionThrown()

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            10,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
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
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("echo\n")
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            null,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            null,
            false
        )

        then:
        thrown(IllegalStateException)
        0 * this.tracer.currentSpan()
        0 * this.scheduler.schedule(_ as Runnable, _ as Instant)
    }

    def "Job was never launched but someone calls wait for"() {
        when:
        this.manager.waitFor()

        then:
        thrown(IllegalStateException)
    }

    def "Job killed due to timeout"() {
        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write("sleep 60\n")
        jobScript.setExecutable(true)

        // This really are more of an integration test...
        def threadPoolScheduler = new ThreadPoolTaskScheduler()
        threadPoolScheduler.setPoolSize(1)
        threadPoolScheduler.setThreadNamePrefix("job-process-manager-impl-spec-")
        threadPoolScheduler.setWaitForTasksToCompleteOnShutdown(false)
        threadPoolScheduler.initialize()
        def realManager = new JobProcessManagerImpl(
            threadPoolScheduler,
            new BraveTracingComponents(
                this.tracer,
                this.tracePropagator,
                Mock(BraveTracingCleanup),
                Mock(BraveTagAdapter)
            )
        )

        when:
        realManager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            1,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()

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


    def "Force kill diehard process"() {
        def future = Mock(ScheduledFuture)

        Path touchedFile = temporaryFolder.resolve("touchz")

        String script = new ClassPathResource("slowlyDie.test.sh")
            .getInputStream()
            .getText()
            .replaceAll('\\$RUNFILE', touchedFile.toAbsolutePath().toString())

        File jobScript = this.temporaryFolder.resolve("run").toFile()
        jobScript.write(script)
        jobScript.setExecutable(true)

        when:
        this.manager.launchProcess(
            this.temporaryFolder.toFile(),
            jobScript,
            true,
            59,
            false
        )

        then:
        noExceptionThrown()
        1 * this.tracer.currentSpan() >> this.span
        1 * this.span.context() >> this.traceContext
        1 * this.tracePropagator.injectForJob(this.traceContext) >> new HashMap<>()
        1 * this.scheduler.schedule(_ as Runnable, _ as Instant) >> future

        // Wait until the process actually starts by checking the existence of the runfile
        while (Files.notExists(touchedFile)) {
            Thread.sleep(100)
        }

        when:
        this.manager.kill(KillService.KillSource.API_KILL_REQUEST)

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = this.manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        // Don't care what (internal) exit code is, as long as it's above 128 meaning terminated
        result.getExitCode() >= 128
        !this.stdErr.exists()
        !this.stdOut.exists()
        1 * future.cancel(true)
    }

}
