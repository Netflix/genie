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

import com.netflix.genie.common.external.dtos.v4.Command
import com.netflix.genie.common.external.dtos.v4.CommandMetadata
import com.netflix.genie.common.external.dtos.v4.CommandStatus
import com.netflix.genie.common.external.dtos.v4.JobRequest

import java.time.Instant

def binding = this.getBinding()

String jobId = GroovyScriptUtils.getJobId(binding)
JobRequest jobRequest = GroovyScriptUtils.getJobRequest(binding)
Set<Command> commands = GroovyScriptUtils.getCommands(binding)

Command selectedCommand = null
String rationale = null

switch (jobId) {
    case "0":
        selectedCommand = commands.find({ it -> (it.getId() == "0") })
        rationale = "selected 0"
        break
    case "1":
        rationale = "Couldn't find anything"
        break
    case "2":
        // Pretend for some reason the script returns something not in the original set
        selectedCommand = new Command(
            UUID.randomUUID().toString(),
            Instant.now(),
            Instant.now(),
            null,
            new CommandMetadata.Builder(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                CommandStatus.DEPRECATED
            ).build(),
            [UUID.randomUUID().toString()],
            null,
            1234L,
            null
        )
        break
    case "3":
        break
    case "5":
        return jobRequest.getMetadata().getName()
    default:
        throw new Exception("uh oh")
}

return new ResourceSelectorScriptResult.Builder<Command>()
    .withResource(selectedCommand)
    .withRationale(rationale)
    .build()
