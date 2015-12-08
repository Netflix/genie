/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.core.services.impl;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.core.services.FileCopyService;
import org.springframework.stereotype.Service;

/**
 * A local unix file system implementation of the File copy service.
 *
 * @author amsharma
 */
@Service
public class LocalFileCopyServiceImpl implements FileCopyService {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(
            final String fileName
    ) throws GenieException {
        return true;
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public void copy(
            final String srcPath,
            final String destPath
    ) throws GenieException {

    };
}
