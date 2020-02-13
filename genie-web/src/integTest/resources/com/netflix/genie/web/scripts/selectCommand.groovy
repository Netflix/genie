/*
 *
 *  Copyright 2017 Netflix, Inc.
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
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

def jsonSlurper = new JsonSlurper()
def result = [:]

def cJson = jsonSlurper.parseText(commands)
def jJson = jsonSlurper.parseText(jobRequest)

def requestedJobId = jJson["requestedId"]
switch (requestedJobId) {
    case "0":
        result["commandId"] = "0"
        result["rationale"] = "selected 0"
        break
    case "1":
        result["rationale"] = "Couldn't find anything"
        break
    case "2":
        result["commandId"] = UUID.randomUUID().toString()
        break
    case "3":
        break
    default:
        throw new Exception("uh oh")
}

return JsonOutput.toJson(result)
