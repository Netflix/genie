/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.common.internal.dto.v4;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;
import java.time.Instant;

/**
 * Fields common to every Genie v4 resource (job, cluster, etc).
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
@ToString(doNotUseGetters = true)
abstract class CommonResource {

    @NotEmpty(message = "An id is required")
    @Size(max = 255, message = "Max length for the ID is 255 characters")
    private final String id;
    private final Instant created;
    private final Instant updated;
    private final ExecutionEnvironment resources;

    CommonResource(
        final String id,
        final Instant created,
        final Instant updated,
        @Nullable final ExecutionEnvironment resources
    ) {
        this.id = id;
        this.created = created;
        this.updated = updated;
        this.resources = resources == null
            ? new ExecutionEnvironment(null, null, null)
            : resources;
    }
}
