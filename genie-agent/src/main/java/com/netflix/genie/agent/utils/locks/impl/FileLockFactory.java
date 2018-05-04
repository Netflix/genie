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
package com.netflix.genie.agent.utils.locks.impl;

import com.netflix.genie.agent.execution.exceptions.LockException;
import com.netflix.genie.agent.utils.locks.CloseableLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import java.io.File;

/**
 * Factory for creating locks implementing {@link CloseableLock}.
 *
 * @author standon
 * @since 4.0.0
 */

@Slf4j
@Component
@Lazy
public class FileLockFactory {

    /**
     * Get a lock locking the provided File object.
     * @param file file to be locked
     * @return a lock locking the file
     * @throws LockException in case of a problem getting a lock for the file
     */
    public CloseableLock getLock(final File file) throws LockException {
        return new FileLock(file);
    }
}
