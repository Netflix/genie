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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Tests for {@link FileLockFactory}.
 *
 * @author standon
 * @since 4.0.0
 */
class FileLockFactoryTest {

    private FileLockFactory fileLockFactory;

    @BeforeEach
    void setup() {
        this.fileLockFactory = new FileLockFactory();
    }

    @Test
    void canGetTaskExecutor(@TempDir final Path tmpDir) throws IOException, LockException {
        Assertions
            .assertThat(
                this.fileLockFactory.getLock(Files.createFile(tmpDir.resolve(UUID.randomUUID().toString())).toFile())
            )
            .isInstanceOf(FileLock.class);
    }
}
