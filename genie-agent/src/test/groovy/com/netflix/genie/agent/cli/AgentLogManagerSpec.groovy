/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.cli

import com.netflix.genie.agent.cli.logging.AgentLogManagerLog4j2Impl
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.appender.FileAppender
import org.apache.logging.log4j.core.config.Configuration
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@Ignore("FileAppender is not easy to mock, so this test fails")
class AgentLogManagerSpec extends Specification {
    LoggerContext loggerContext
    Configuration configuration
    FileAppender appender
    Path temporaryLogFilePath
    Path newLogFilePath

    @TempDir
    Path temporaryFolder

    void setup() {
        this.loggerContext = Mock(LoggerContext)
        this.configuration = Mock(Configuration)
        this.temporaryLogFilePath = this.temporaryFolder.resolve("agent.log")
        this.newLogFilePath = Paths.get(temporaryFolder.getRoot().toString(), "agent-relocated.log")
        this.appender = GroovyMock(FileAppender)
        Files.createFile(this.temporaryLogFilePath)
    }

    def "Relocate successfully"() {
        Path returnedFilePath

        when:
        AgentLogManagerLog4j2Impl agentLogManager = new AgentLogManagerLog4j2Impl(loggerContext)

        then:
        1 * loggerContext.getConfiguration() >> configuration
        1 * configuration.getAppender(AgentLogManagerLog4j2Impl.AGENT_LOG_FILE_APPENDER_NAME) >> appender
        1 * appender.getFileName() >> temporaryLogFilePath.toString()

        when:
        returnedFilePath = agentLogManager.getLogFilePath()

        then:
        returnedFilePath == temporaryLogFilePath

        when:
        agentLogManager.relocateLogFile(newLogFilePath)

        then:
        Files.exists(newLogFilePath)
        !Files.exists(temporaryLogFilePath)

        when:
        returnedFilePath = agentLogManager.getLogFilePath()

        then:
        returnedFilePath == newLogFilePath
    }
}
