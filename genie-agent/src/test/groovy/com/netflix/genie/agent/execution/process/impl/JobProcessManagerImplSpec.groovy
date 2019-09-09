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
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class JobProcessManagerImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    Map<String, String> envMap
    File stdOut
    File stdErr

    void setup() {
        envMap = [:]
        stdOut = PathUtils.jobStdOutPath(temporaryFolder.getRoot()).toFile()
        stdErr = PathUtils.jobStdErrPath(temporaryFolder.getRoot()).toFile()
        Files.createDirectories(stdOut.getParentFile().toPath())
        Files.createDirectories(stdErr.getParentFile().toPath())
    }

    void cleanup() {
    }

    def "LaunchProcess interactive"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()
        File expectedFile = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString())
        envMap.put("PATH", System.getenv("PATH") + ":/foo")

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["touch", expectedFile.getAbsolutePath()],
            true
        )

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        expectedFile.exists()
        !stdErr.exists()
        !stdOut.exists()
    }

    def "LaunchProcess noninteractive with variable expansion"() {
        setup:
        String helloWorld = "Hello World!"
        JobProcessManager manager = new JobProcessManagerImpl()
        envMap.put("ECHO_COMMAND", "echo")

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["\${ECHO_COMMAND}", helloWorld],
            false
        )

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        stdErr.exists()
        stdOut.exists()
        stdOut.getText(StandardCharsets.UTF_8.toString()).contains(helloWorld)
    }

    def "LaunchProcess noninteractive and check environment env"() {
        setup:
        String uuid = UUID.randomUUID().toString()
        envMap.put("GENIE_UUID", uuid)
        String expectedString = "GENIE_UUID=" + uuid
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["env"],
            false
        )

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.SUCCEEDED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FINISHED_SUCCESSFULLY
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        stdErr.exists()
        stdOut.exists()
        stdOut.getText(StandardCharsets.UTF_8.toString()).contains(expectedString)
    }

    def "LaunchProcess command error"() {
        setup:
        File nonExistentFile = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString())
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["rm", nonExistentFile.absolutePath],
            false
        )

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.FAILED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_FAILED
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        stdErr.exists()
        stdOut.exists()
        stdErr.getText(StandardCharsets.UTF_8.toString()).contains("No such file or directory")
    }

    def "LaunchProcess missing executable"() {
        setup:
        String uuid = UUID.randomUUID().toString()
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            [uuid],
            false)

        then:
        thrown(JobLaunchException)
    }

    def "LaunchProcess missing environment variable"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["\$COMMAND"],
            false
        )

        then:
        thrown(JobLaunchException)
    }

    def "Job directory null"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            null,
            envMap,
            ["echo"],
            false
        )

        then:
        thrown(JobLaunchException)
    }

    def "Job directory not a directory"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.newFile("foo"),
            envMap,
            ["echo"],
            false
        )

        then:
        thrown(JobLaunchException)
    }

    def "Job folder not existing"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            new File(temporaryFolder.getRoot(), "foo"),
            envMap,
            ["echo"],
            false
        )

        then:
        thrown(JobLaunchException)
    }

    def "Environment null"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            null,
            ["echo"],
            false)

        then:
        thrown(JobLaunchException)
    }

    def "Args not set"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            null,
            false)

        then:
        thrown(JobLaunchException)
    }

    def "Args empty"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            [],
            false
        )

        then:
        thrown(JobLaunchException)
    }

    def "Kill running process"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["sleep", "60"],
            true
        )

        then:
        noExceptionThrown()

        when:
        manager.kill()

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        !stdErr.exists()
        !stdOut.exists()
    }

    def "Kill running process via event"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["sleep", "60"],
            true
        )

        then:
        noExceptionThrown()

        when:
        manager.onApplicationEvent(new KillService.KillEvent(KillService.KillSource.API_KILL_REQUEST))

        then:
        noExceptionThrown()

        when:
        JobProcessResult result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        !stdErr.exists()
        !stdOut.exists()
    }

    def "Kill completed process"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["echo", "foo"],
            true
        )

        then:
        noExceptionThrown()

        when:
        manager.kill()

        then:
        noExceptionThrown()

        when:
        def result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        !stdErr.exists()
        !stdOut.exists()
    }

    def "Skip process launch"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.kill()

        then:
        noExceptionThrown()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["echo", "foo"],
            true
        )

        then:
        noExceptionThrown()

        when:
        def result = manager.waitFor()

        then:
        result.getFinalStatus() == JobStatus.KILLED
        result.getFinalStatusMessage() == JobStatusMessages.JOB_KILLED_BY_USER
        result.getStdOutSize() == 0L
        result.getStdErrSize() == 0L
        !stdErr.exists()
        !stdOut.exists()
    }

    def "Double launch"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["echo", "foo"],
            true
        )

        then:
        noExceptionThrown()

        when:
        manager.launchProcess(
            temporaryFolder.getRoot(),
            envMap,
            ["echo", "foo"],
            true
        )

        then:
        thrown(IllegalStateException)
    }

    def "No launch"() {
        setup:
        JobProcessManager manager = new JobProcessManagerImpl()

        when:
        manager.waitFor()

        then:
        thrown(IllegalStateException)
    }
}
