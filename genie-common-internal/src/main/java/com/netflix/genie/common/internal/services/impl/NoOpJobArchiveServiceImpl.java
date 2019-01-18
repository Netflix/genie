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
package com.netflix.genie.common.internal.services.impl;

import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiveService;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.file.Path;

/**
 * Implementation of JobArchiveService which does no archival.
 *
 * @author standon
 * @since 4.0.0
 */
@Slf4j
public class NoOpJobArchiveServiceImpl implements JobArchiveService {

    /**
     * No archival is done.
     *
     * @param path      path to the file/dir to archive
     * @param targetURI target uri for the archival location
     * @throws JobArchiveException On error
     */
    @Override
    public void archive(final Path path, final URI targetURI) throws JobArchiveException {
        log.warn("NoOpArchivalService called. No archival done.");
    }
}
