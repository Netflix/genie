/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.jobs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

/**
 * Class that represents the structure of the kill-reason file created when a job is killed.
 *
 * @author mprimi
 * @since 3.0.7
 */
@Getter
public class JobKillReasonFile {

    private final String killReason;

    /**
     * Constructor, annotated for Jackson.
     *
     * @param killReason stores a string with the reason a given job was killed
     */
    @JsonCreator
    public JobKillReasonFile(@JsonProperty("killReason") final String killReason) {
        this.killReason = killReason;
    }
}
