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
import com.netflix.genie.common.external.dtos.v4.JobRequest

if (!(jobRequestParameter instanceof JobRequest)) {
    throw new IllegalArgumentException("jobRequestParameter argument not instance of " + JobRequest.class.getName())
}
final JobRequest jobRequest = (JobRequest) jobRequestParameter

if (!(commandsParameter instanceof Set)) {
    throw new IllegalArgumentException(
        "Expected commandsParameter to be instance of Set. Got " + commandsParameter.getClass().getName()
    )
}
if (commandsParameter.isEmpty() || !(commandsParameter.iterator().next() instanceof Command)) {
    throw new IllegalArgumentException("Expected commandsParameter to be non-empty set of " + Command.class.getName())
}
final Set<Command> commands = (Set<Command>) commandsParameter

String commandId = null
String rationale = null

def requestedJobId = jobRequest.getRequestedId().orElse(UUID.randomUUID().toString())
switch (requestedJobId) {
    case "0":
        commandId = "0"
        rationale = "selected 0"
        break
    case "1":
        rationale = "Couldn't find anything"
        break
    case "2":
        commandId = UUID.randomUUID().toString()
        break
    case "3":
        break
    case "5":
        return "This is not the correct return type"
    default:
        throw new Exception("uh oh")
}

return new CommandSelectorManagedScript.ScriptResult(commandId, rationale)
