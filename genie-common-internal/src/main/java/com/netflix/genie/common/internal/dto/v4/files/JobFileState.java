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
package com.netflix.genie.common.internal.dto.v4.files;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.util.Optional;

/**
 * Representation of the metadata for a job file on a given underlying storage system.
 *
 * @author tgianos
 * @since 4.0.0
 */
@Getter
@ToString(doNotUseGetters = true)
@EqualsAndHashCode(doNotUseGetters = true)
@SuppressWarnings("checkstyle:finalclass")
public class JobFileState {
    private final String path;
    @Min(value = 0L, message = "A file can't have a negative size")
    private final long size;
    private final String md5;

    /**
     * Constructor.
     *
     * @param path The relative path to the file from the root of the job directory
     * @param size The current size of the file within the storage system. Min 0
     * @param md5  The md5 hex of the file contents if it was calculated
     */
    public JobFileState(final String path, final long size, @Nullable final String md5) {
        this.path = path;
        this.size = size;
        this.md5 = md5;
    }

    /**
     * Get the MD5 hash of the file (as 32 hex characters) if it was calculated.
     *
     * @return The MD5 value or {@link Optional#empty()}
     */
    public Optional<String> getMd5() {
        return Optional.ofNullable(this.md5);
    }
}
