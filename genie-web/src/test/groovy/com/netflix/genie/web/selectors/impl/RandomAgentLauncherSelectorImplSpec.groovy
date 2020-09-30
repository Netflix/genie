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
package com.netflix.genie.web.selectors.impl

import com.netflix.genie.web.agent.launchers.AgentLauncher
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext
import spock.lang.Specification

class RandomAgentLauncherSelectorImplSpec extends Specification {

    def "Select randomly"() {
        def launchers = [Mock(AgentLauncher), Mock(AgentLauncher)]
        def selector = new RandomAgentLauncherSelectorImpl(launchers)
        def context = Mock(AgentLauncherSelectionContext)

        when:
        def selectionResult = selector.select(context)

        then:
        1 * context.getResources() >> launchers
        selectionResult.getSelectedResource().isPresent()
        selectionResult.getSelectionRationale().isPresent()
        launchers.contains(selectionResult.getSelectedResource().get())

    }
}
