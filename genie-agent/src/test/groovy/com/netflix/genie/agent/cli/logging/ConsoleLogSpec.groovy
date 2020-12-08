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
package com.netflix.genie.agent.cli.logging

import org.slf4j.Logger
import org.springframework.core.env.Environment
import spock.lang.Specification

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

class ConsoleLogSpec extends Specification {

    def "GetLogger"() {
        when:
        Logger logger = ConsoleLog.getLogger()

        then:
        logger != null
    }

    def "PrintBanner -- no banner set"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        ConsoleLog.printBanner(environment)

        then:
        1 * environment.getProperty(ConsoleLog.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> null
    }

    def "PrintBanner -- exception"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        ConsoleLog.printBanner(environment)

        then:
        1 * environment.getProperty(ConsoleLog.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> {
            throw new RuntimeException("error")
        }
        noExceptionThrown()
    }

    def "PrintBanner -- not existent"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        ConsoleLog.printBanner(environment)

        then:
        1 * environment.getProperty(ConsoleLog.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> "classpath:not-a-banner.txt"
    }

    def "PrintBanner"() {
        setup:
        Environment environment = Mock(Environment)

        when:
        ConsoleLog.printBanner(environment)

        then:
        1 * environment.getProperty(ConsoleLog.BANNER_LOCATION_SPRING_PROPERTY_KEY) >> "classpath:genie-agent-banner.txt"
        1 * environment.getProperty(
            ConsoleLog.BANNER_CHARSET_SPRING_PROPERTY_KEY,
            Charset.class,
            StandardCharsets.UTF_8
        ) >> StandardCharsets.UTF_8
    }
}
