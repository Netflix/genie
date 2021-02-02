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
package com.netflix.genie.web.agent.launchers;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;
import org.springframework.boot.actuate.health.HealthIndicator;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * A interface which implementations will launch instances of an agent in some manner in order to run a job.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface AgentLauncher extends HealthIndicator {

    /**
     * Shared name of a timer that can be used by launchers to record how long it took them to perform their
     * respective launch.
     */
    String LAUNCH_TIMER = "genie.agents.launchers.launch.timer";

    /**
     * Shared key for a tag that can be added to the timer metric to add the implementation class in order to aid in
     * adding a convenient dimension.
     *
     * @see #LAUNCH_TIMER
     */
    String LAUNCHER_CLASS_KEY = "launcherClass";

    /**
     * Shared key representing the class that key for the Launcher Ext context the API can return.
     */
    String LAUNCHER_CLASS_EXT_FIELD = "launcherClass";

    /**
     * Shared key representing the hostname of the Genie server the Agent Launcher was executed on.
     */
    String SOURCE_HOST_EXT_FIELD = "sourceHostname";

    /**
     * Launch an agent to execute the given {@link ResolvedJob} information.
     *
     * @param resolvedJob          The {@link ResolvedJob} information for the agent to act on
     * @param requestedLauncherExt The launcher requested extension, or null
     * @return an optional {@link JsonNode} with the launcher context about the launched job
     * @throws AgentLaunchException For any error launching an Agent instance to run the job
     */
    Optional<JsonNode> launchAgent(
        ResolvedJob resolvedJob,
        @Nullable JsonNode requestedLauncherExt
    ) throws AgentLaunchException;
}
