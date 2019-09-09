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
package com.netflix.genie.agent.cli

import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.slf4j.Logger
import org.springframework.core.env.Environment
import spock.lang.Specification

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

class UserConsoleSpec extends Specification {

    @Rule
    TemporaryFolder temporaryFolder = new TemporaryFolder()

    def "GetLogger"() {
        when:
        Logger logger = UserConsole.getLogger()
        String logPath = UserConsole.getLogFilePath()

        then:
        logger != null
        logPath != null
    }

    def "PrintBanner -- no banner set"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        UserConsole.printBanner(environment)

        then:
        1 * environment.getProperty(UserConsole.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> null
    }

    def "PrintBanner -- exception"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        UserConsole.printBanner(environment)

        then:
        1 * environment.getProperty(UserConsole.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> {
            throw new RuntimeException("error")
        }
        noExceptionThrown()
    }

    def "PrintBanner -- not existent"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        UserConsole.printBanner(environment)

        then:
        1 * environment.getProperty(UserConsole.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> "classpath:not-a-banner.txt"
    }

    def "PrintBanner"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        UserConsole.printBanner(environment)

        then:
        1 * environment.getProperty(UserConsole.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> "classpath:genie-agent-banner.txt"
        1 * environment.getProperty(
            UserConsole.BANNER_CHARSET_SPRING_PROPERTY_KEY,
            Charset.class,
            StandardCharsets.UTF_8
        ) >> StandardCharsets.UTF_8
    }

    def "relocateLogFile"() {

        when: "Destination exists"
        UserConsole.relocateLogFile(temporaryFolder.newFile().toPath())

        then:
        thrown(IOException)

        when:
        Path logFilePath = Paths.get(UserConsole.getLogFilePath())
        FileWriter writer = new FileWriter(logFilePath.toFile())
        writer.write("foo")
        writer.flush()
        Path newLogFilePath = Paths.get(temporaryFolder.getRoot().toPath().toString(), "agent.log")

        then:
        Files.exists(logFilePath)
        !Files.exists(newLogFilePath)

        when:
        UserConsole.relocateLogFile(newLogFilePath)

        then:
        !Files.exists(logFilePath)
        Files.exists(newLogFilePath)

        when:
        writer.write("bar\n")
        writer.flush()

        then:
        new FileReader(newLogFilePath.toFile()).readLine().contains("foobar")
    }
}
