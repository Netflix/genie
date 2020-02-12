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
import groovy.json.JsonSlurper

Map<String, Object> parsedJobRequest = new JsonSlurper().parseText(jobRequest)

String jobDescription = parsedJobRequest.get("description") as String

if (jobDescription.endsWith("true")) {
    return true
} else if (jobDescription.endsWith("false")) {
    return false
} else if (jobDescription.endsWith("null")) {
    return null
}

throw new IllegalArgumentException("Unexpected test input: " + jobRequest)
