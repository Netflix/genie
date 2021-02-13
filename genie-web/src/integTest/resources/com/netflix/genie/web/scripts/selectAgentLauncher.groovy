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
package com.netflix.genie.web.scripts

import com.fasterxml.jackson.databind.JsonNode
import com.netflix.genie.common.external.dtos.v4.JobRequest
import com.netflix.genie.web.agent.launchers.AgentLauncher
import com.netflix.genie.web.dtos.ResolvedJob
import com.netflix.genie.web.exceptions.checked.AgentLaunchException
import com.netflix.genie.web.selectors.AgentLauncherSelectionContext
import org.springframework.boot.actuate.health.Health

def binding = this.getBinding()

AgentLauncherSelectionContext context = GroovyScriptUtils.getAgentLauncherSelectionContext(binding)
String jobId = context.getJobId()
JobRequest jobRequest = context.getJobRequest()
Set<AgentLauncher> agentLaunchers = context.getAgentLaunchers()

AgentLauncher selectedAgentLauncher = null
String rationale = null

switch (jobId) {
    case "0":
        selectedAgentLauncher = agentLaunchers.find({ it -> (it.getClass().getSimpleName().endsWith("1")) })
        rationale = "selected 1"
        break
    case "1":
        rationale = "Couldn't find anything"
        break
    case "2":
        // Pretend for some reason the script returns something not in the original set
        selectedAgentLauncher = new AgentLauncher() {
            @Override
            Optional<JsonNode> launchAgent(final ResolvedJob resolvedJob, final JsonNode requestedLauncherExt) throws AgentLaunchException {
                return null
            }

            @Override
            Health health() {
                return null
            }
        }
        break
    case "3":
        break
    case "5":
        return jobRequest.getMetadata().getName()
    default:
        throw new Exception("uh oh")
}

return new ResourceSelectorScriptResult.Builder<AgentLauncher>()
    .withResource(selectedAgentLauncher)
    .withRationale(rationale)
    .build()
