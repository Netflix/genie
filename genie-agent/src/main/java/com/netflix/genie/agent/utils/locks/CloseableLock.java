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
package com.netflix.genie.agent.utils.locks;

import com.netflix.genie.agent.execution.exceptions.LockException;
import java.io.Closeable;

/**
 * A simple interface representing a closeable lock.
 *
 * @author standon
 * @since 4.0.0
 */
public interface CloseableLock extends Closeable {

    /**
     * Acquire a lock.
     * @throws LockException in case of problem acquiring the lock
     */
    void lock() throws LockException;

}
