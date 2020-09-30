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
package com.netflix.genie.web.selectors;

import com.netflix.genie.web.agent.launchers.AgentLauncher;
import org.springframework.validation.annotation.Validated;

import java.util.Collection;

/**
 * Interface for any classes which provide a way to select a {@link AgentLauncher} from a set of available candidates.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Validated
public interface AgentLauncherSelector extends ResourceSelector<AgentLauncher, AgentLauncherSelectionContext> {

    /**
     * Get the list of all available {@link AgentLauncher}.
     *
     * @return a collection of {@link AgentLauncher}
     */
    Collection<AgentLauncher> getAgentLaunchers();
}
