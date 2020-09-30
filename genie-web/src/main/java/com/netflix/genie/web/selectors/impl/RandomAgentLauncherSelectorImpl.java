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
package com.netflix.genie.web.selectors.impl;

import com.netflix.genie.web.agent.launchers.AgentLauncher;
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext;
import com.netflix.genie.web.selectors.AgentLauncherSelector;
import lombok.Getter;

import java.util.Collection;

/**
 * Basic implementation of a {@link AgentLauncherSelector} where a random {@link AgentLauncher} is selected from the
 * options presented.
 *
 * @author mprimi
 * @since 4.0.0
 */
public class RandomAgentLauncherSelectorImpl
    extends RandomResourceSelector<AgentLauncher, AgentLauncherSelectionContext>
    implements AgentLauncherSelector {

    @Getter
    private final Collection<AgentLauncher> agentLaunchers;

    /**
     * Constructor.
     *
     * @param agentLaunchers the list of available {@link AgentLauncher}.
     */
    public RandomAgentLauncherSelectorImpl(final Collection<AgentLauncher> agentLaunchers) {
        this.agentLaunchers = agentLaunchers;
    }
}
