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
package com.netflix.genie.web.scripts

import spock.lang.Specification


class ManagedScriptBasePropertiesSpec extends Specification {
    TestProperties testProperties

    void setup() {
        this.testProperties = new TestProperties()
    }

    def "Properties"() {
        expect:
        this.testProperties.getSource() == null
        this.testProperties.getTimeout() == 5_000L
        this.testProperties.isAutoLoadEnabled()

        when:
        this.testProperties.setSource(new URI("file:///foo.js"))
        this.testProperties.setTimeout(333L)
        this.testProperties.setAutoLoadEnabled(false)

        then:
        this.testProperties.getSource() != null
        this.testProperties.getTimeout() == 333L
        !this.testProperties.isAutoLoadEnabled()
    }

    private static class TestProperties extends ManagedScriptBaseProperties {
    }
}
