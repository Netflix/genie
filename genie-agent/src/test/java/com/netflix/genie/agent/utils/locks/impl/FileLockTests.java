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
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Tests for {@link FileLock}.
 *
 * @author standon
 * @since 4.0.0
 */
@Category(UnitTest.class)
public class FileLockTests {

    /**
     * Make sure close method for a FileLock gets called .
     *
     * @throws IOException   when the file is bad
     * @throws LockException when there is a problem getting lock on the file
     */
    @Test
    public void fileLockClosed() throws IOException, LockException {
        final FileLock mockLock = Mockito.mock(FileLock.class);
        try (final CloseableLock lock = mockLock) {
            lock.lock();
        }

        Mockito.verify(
            mockLock, Mockito.times(1)
        ).close();
    }

    /**
     * Make sure close method for a FileLock gets called on exception.
     *
     * @throws IOException   when the file is bad
     * @throws LockException when there is a problem getting lock on the file
     */
    @Test
    public void fileLockClosedOnException() throws IOException {
        final FileLock mockLock = Mockito.mock(FileLock.class);
        try (final CloseableLock lock = mockLock) {
            lock.lock();
            throw new LockException("dummy exception");
        } catch (LockException e) {
        }

        Mockito.verify(
            mockLock, Mockito.times(1)
        ).close();
    }
}
