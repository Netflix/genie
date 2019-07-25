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

import com.netflix.genie.web.dtos.ResolvedJob;
import com.netflix.genie.web.exceptions.checked.AgentLaunchException;

/**
 * A interface which implementations will launch instances of an agent in some manner in order to run a job.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface AgentLauncher {

    /**
     * Launch an agent to execute the given {@link ResolvedJob} information.
     *
     * @param resolvedJob The {@link ResolvedJob} information for the agent to act on
     * @throws AgentLaunchException For any error launching an Agent instance to run the job
     */
    void launchAgent(ResolvedJob resolvedJob) throws AgentLaunchException;
}
