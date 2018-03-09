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

package com.netflix.genie.agent.execution.services.impl

import com.netflix.genie.agent.execution.ExecutionContext
import com.netflix.genie.agent.execution.exceptions.JobLaunchException
import com.netflix.genie.agent.execution.services.LaunchJobService
import com.netflix.genie.agent.utils.PathUtils
import com.netflix.genie.common.dto.v4.JobSpecification
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

class LaunchJobServiceImplSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder

    ExecutionContext ctx
    JobSpecification spec
    Map<String, String> envMap
    File stdOut
    File stdErr

    void setup() {
        temporaryFolder.create()
        ctx = Mock()
        spec = Mock()
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
        LaunchJobService service = new LaunchJobServiceImpl(ctx)
        File expectedFile = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString())

        when:
        Process process = service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> ["touch", expectedFile.getAbsolutePath()]
        spec.isInteractive() >> true
        process != null

        when:
        boolean terminated = process.waitFor(3, TimeUnit.SECONDS)

        then:
        terminated
        0 == process.exitValue()
        expectedFile.exists()
        !stdErr.exists()
        !stdOut.exists()
    }

    def "LaunchProcess noninteractive"() {
        setup:
        String helloWorld = "Hello World!"
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        Process process = service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> ["echo", helloWorld]
        spec.isInteractive() >> false
        process != null

        when:
        boolean terminated = process.waitFor(3, TimeUnit.SECONDS)

        then:
        terminated
        0 == process.exitValue()
        stdErr.exists()
        stdOut.exists()
        stdOut.getText(StandardCharsets.UTF_8.toString()).contains(helloWorld)
    }

    def "LaunchProcess noninteractive and check environment env"() {
        setup:
        String uuid = UUID.randomUUID().toString()
        envMap.put("GENIE_UUID", uuid)
        String expectedString = "GENIE_UUID=" + uuid
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        Process process = service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> ["env"]
        spec.isInteractive() >> false
        process != null

        when:
        boolean terminated = process.waitFor(3, TimeUnit.SECONDS)

        then:
        terminated
        0 == process.exitValue()
        stdErr.exists()
        stdOut.exists()
        stdOut.getText(StandardCharsets.UTF_8.toString()).contains(expectedString)
    }

    def "LaunchProcess command error"() {
        setup:
        File nonExistentFile = new File(temporaryFolder.getRoot(), UUID.randomUUID().toString())
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        Process process = service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> ["rm", nonExistentFile.absolutePath]
        spec.isInteractive() >> false
        process != null

        when:
        boolean terminated = process.waitFor(3, TimeUnit.SECONDS)

        then:
        terminated
        0 != process.exitValue()
        stdErr.exists()
        stdOut.exists()
        stdErr.getText(StandardCharsets.UTF_8.toString()).contains("No such file or directory")
    }

    def "LaunchProcess missing executable"() {
        setup:
        String uuid = UUID.randomUUID().toString()
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        Process process = service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> [uuid]
        spec.isInteractive() >> false
        thrown(JobLaunchException)
    }

    def "Spec not set in context"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> null
        thrown(JobLaunchException)
    }

    def "Job directory not set in context"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> null
        thrown(JobLaunchException)
    }

    def "Job directory not a directory"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.newFile()
        thrown(JobLaunchException)
    }

    def "Job folder not existing"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> new File(temporaryFolder.getRoot(), "foo")
        thrown(JobLaunchException)
    }

    def "Environment not set in context"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> null
        thrown(JobLaunchException)
    }

    def "Args not set"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> null
        thrown(JobLaunchException)
    }

    def "Args empty"() {
        setup:
        LaunchJobService service = new LaunchJobServiceImpl(ctx)

        when:
        service.launchProcess()

        then:
        ctx.getJobSpecification() >> spec
        ctx.getJobDirectory() >> temporaryFolder.getRoot()
        ctx.getJobEnvironment() >> envMap
        spec.getCommandArgs() >> []
        thrown(JobLaunchException)
    }

}
