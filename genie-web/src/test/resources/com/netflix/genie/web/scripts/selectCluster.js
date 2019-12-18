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

/**
 * Example cluster selector script for JavaScript.
 * clusters and jobRequest variables passed in via script context.
 *
 * @author tgianos
 * @since 3.1.0
 */

var cJson = JSON.parse(clusters);
// var jJson = JSON.parse(jobRequest);

var index;
for (index = 0; index < cJson.length; index++) {
    var cluster = cJson[index];
    if (cluster.user === "h") {
        break;
    }
}

index < cJson.length ? cJson[index].id : null;
