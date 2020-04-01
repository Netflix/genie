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
def commandsParameterName = "commandsParameter"
def jobIdParameterName = "jobIdParameter"
def jobRequestParameterName = "jobRequestParameter"

if (!binding.hasVariable(jobIdParameterName)
    || !(binding.getVariable(jobIdParameterName) instanceof String)) {
    throw new IllegalArgumentException("jobIdParameter argument not instance of " + String.class.getName())
}
final String jobId = (String) binding.getVariable(jobIdParameterName)

if (!binding.hasVariable(jobRequestParameterName)
    || !(binding.getVariable(jobRequestParameterName) instanceof JobRequest)) {
    throw new IllegalArgumentException("jobRequestParameter argument not instance of " + JobRequest.class.getName())
}
final JobRequest jobRequest = (JobRequest) binding.getVariable(jobRequestParameterName)

if (!binding.hasVariable(commandsParameterName) || !(binding.getVariable(commandsParameterName) instanceof Set)) {
    throw new IllegalArgumentException(
        "Expected commandsParameter to be instance of Set. Got "
            + binding.getVariable(commandsParameterName).getClass().getName()
    )
}
def commandsParameterSet = (Set) binding.getVariable(commandsParameterName)
if (commandsParameterSet.isEmpty() || !(commandsParameterSet.iterator().next() instanceof Command)) {
    throw new IllegalArgumentException("Expected commandsParameter to be non-empty set of " + Command.class.getName())
}
final Set<Command> commands = (Set<Command>) commandsParameterSet

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
