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
package com.netflix.genie.common.internal.services;

import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import org.springframework.core.io.WritableResource;

import java.net.URI;
import java.nio.file.Path;

/**
 * Implementations of this interface should be able to a write job files to a {@link WritableResource} root location.
 *
 * @author tgianos
 * @since 4.0.0
 */
public interface JobArchiver {

    /**
     * Attempt to archive a directory located at {@code directory} to the {@code target}. All existing data "under"
     * {@code target} should be assumed to be overwritten/replaced.
     *
     * @param directory The directory to archive
     * @param target    The root of a writable location to archive to.
     * @return {@code false} if this implementation doesn't support archiving to {@code target}. {@code true} if does
     * support archiving to {@code target} and the archival was successful
     * @throws JobArchiveException If an exception happened during archival
     */
    boolean archiveDirectory(Path directory, URI target) throws JobArchiveException;
}
