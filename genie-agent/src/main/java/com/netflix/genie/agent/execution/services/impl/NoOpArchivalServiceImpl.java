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

package com.netflix.genie.agent.execution.services.impl;

import com.netflix.genie.agent.execution.exceptions.ArchivalException;
import com.netflix.genie.agent.execution.services.ArchivalService;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.net.URI;

/**
 * Implementation of ArchivalService which does no archival.
 *
 * @author standon
 * @since 4.0.0
 */
@Slf4j
class NoOpArchivalServiceImpl implements ArchivalService {

    /**
     * No archival is done.
     * @param path      path to the file/dir to archive
     * @param targetURI target uri for the archival location
     * @throws ArchivalException
     */
    @Override
    public void archive(final Path path, final URI targetURI) throws ArchivalException {
        log.warn("NoOpArchivalService called. No archival done.");
    }
}
