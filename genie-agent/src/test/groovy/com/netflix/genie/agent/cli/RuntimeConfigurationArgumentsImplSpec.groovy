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

import com.beust.jcommander.JCommander
import com.beust.jcommander.ParametersDelegate
import spock.lang.Specification

class RuntimeConfigurationArgumentsImplSpec extends Specification {

    TestOptions options
    JCommander jCommander

    void setup() {
        options = new TestOptions()
        jCommander = new JCommander(options)
    }

    void cleanup() {
    }

    def "Defaults"() {
        when:
        jCommander.parse()

        then:
        !CacheArgumentsImpl.DEFAULT_CACHE_PATH == options.runtimeConfigurationArguments.isLaunchInJobDirectory()
    }

    def "Parse"() {
        when:
        jCommander.parse(
            "--launchInJobDirectory"
        )

        then:
        options.runtimeConfigurationArguments.isLaunchInJobDirectory()
    }

    class TestOptions {
        @ParametersDelegate
        private ArgumentDelegates.RuntimeConfigurationArguments runtimeConfigurationArguments =
            new RuntimeConfigurationArgumentsImpl()
    }
}
