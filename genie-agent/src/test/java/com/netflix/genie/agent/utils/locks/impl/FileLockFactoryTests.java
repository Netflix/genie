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
import com.netflix.genie.test.categories.UnitTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link com.netflix.genie.agent.utils.locks.impl.FileLockFactory}.
 *
 * @author standon
 * @since 4.0.0
 */
@Category(UnitTest.class)
public class FileLockFactoryTests {

    /**
     * Temporary folder.
     */
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private FileLockFactory fileLockFactory;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        this.fileLockFactory = new FileLockFactory();
    }

    /**
     * Make sure getLock returns a lock of the right type.
     *
     * @throws IOException   when the file is bad
     * @throws LockException when there is a problem getting lock on the file
     */
    @Test
    public void canGetTaskExecutor() throws IOException, LockException {
        Assert.assertTrue(
            fileLockFactory.getLock(temporaryFolder.newFile()) instanceof FileLock
        );
    }
}
