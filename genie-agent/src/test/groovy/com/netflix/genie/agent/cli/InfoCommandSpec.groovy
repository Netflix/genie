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

import com.netflix.genie.test.categories.UnitTest
import org.junit.experimental.categories.Category
import org.springframework.context.ConfigurableApplicationContext
import spock.lang.Specification

@Category(UnitTest.class)
class InfoCommandSpec extends Specification {
    InfoCommand.InfoCommandArguments args
    ConfigurableApplicationContext ctx


    void setup() {
        args = Mock()
        ctx = Mock()
    }

    void cleanup() {
    }

    def "Run"() {
        setup:
        def cmd = new InfoCommand(args, ctx)

        when:
        cmd.run()

        then:
        1 * args.getIncludeBeans() >> true
        1 * ctx.getBeanDefinitionCount() >> 0
        1 * ctx.getBeanDefinitionNames() >> new String[0]
        1 * args.getIncludeEnvironment() >> true
        1 * args.isIncludeProperties() >> true
    }

    def "Run skip all"() {
        setup:
        def cmd = new InfoCommand(args, ctx)

        when:
        cmd.run()

        then:
        1 * args.getIncludeBeans() >> false
        0 * ctx.getBeanDefinitionCount()
        0 * ctx.getBeanDefinitionNames()
        1 * args.getIncludeEnvironment() >> false
        1 * args.isIncludeProperties() >> false
    }
}
