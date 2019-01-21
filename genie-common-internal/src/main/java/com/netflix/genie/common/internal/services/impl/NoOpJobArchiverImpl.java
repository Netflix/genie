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
package com.netflix.genie.common.internal.services.impl;

import com.netflix.genie.common.internal.exceptions.JobArchiveException;
import com.netflix.genie.common.internal.services.JobArchiver;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.nio.file.Path;

/**
 * Implementation of JobArchiveService which does no archival.
 *
 * @author standon
 * @author tgianos
 * @since 4.0.0
 */
@Slf4j
public class NoOpJobArchiverImpl implements JobArchiver {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean archiveDirectory(final Path directory, final URI target) throws JobArchiveException {
        return true;
    }
}
