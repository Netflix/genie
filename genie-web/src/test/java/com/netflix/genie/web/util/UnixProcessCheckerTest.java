/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.web.util;

import com.netflix.genie.common.exceptions.GenieTimeoutException;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Unit tests for UnixProcessChecker.
 *
 * @author tgianos
 * @since 3.0.0
 */
class UnixProcessCheckerTest {

    private static final int PID = 18243;

    private Executor executor;
    private Instant tomorrow;

    /**
     * Setup for the tests.
     */
    @BeforeEach
    void setup() {
        Assumptions.assumeTrue(SystemUtils.IS_OS_UNIX);
        this.executor = Mockito.mock(Executor.class);
        this.tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        // For standard tests this will keep it from dying
    }

    /**
     * Make sure the correct process is invoked.
     *
     * @throws GenieTimeoutException on timeout
     * @throws IOException           on error
     */
    @Test
    void canCheckProcess() throws GenieTimeoutException, IOException {
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final UnixProcessChecker processChecker =
            new UnixProcessChecker(PID, this.executor, tomorrow, false);
        processChecker.checkProcess();
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().getExecutable()).isEqualTo("kill");
        Assertions.assertThat(argumentCaptor.getValue().getArguments().length).isEqualTo(2);
        Assertions.assertThat(argumentCaptor.getValue().getArguments()[0]).isEqualTo("-0");
        Assertions.assertThat(argumentCaptor.getValue().getArguments()[1]).isEqualTo(Integer.toString(PID));
    }

    /**
     * Make sure the correct process is invoked.
     *
     * @throws GenieTimeoutException on timeout
     * @throws IOException           on error
     */
    @Test
    void canCheckProcessWithSudo() throws GenieTimeoutException, IOException {
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final UnixProcessChecker processChecker =
            new UnixProcessChecker(PID, this.executor, tomorrow, true);
        processChecker.checkProcess();
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assertions.assertThat(argumentCaptor.getValue().getExecutable()).isEqualTo("sudo");
        Assertions.assertThat(argumentCaptor.getValue().getArguments().length).isEqualTo(3);
        Assertions.assertThat(argumentCaptor.getValue().getArguments()[0]).isEqualTo("kill");
        Assertions.assertThat(argumentCaptor.getValue().getArguments()[1]).isEqualTo("-0");
        Assertions.assertThat(argumentCaptor.getValue().getArguments()[2]).isEqualTo(Integer.toString(PID));
    }

    /**
     * Make sure if the timeout has been exceeded then an exception is thrown indicating the process should be killed.
     */
    @Test
    void canCheckProcessTimeout() {
        Assertions
            .assertThatExceptionOfType(GenieTimeoutException.class)
            .isThrownBy(
                () -> {
                    final Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
                    new UnixProcessChecker(PID, this.executor, yesterday, true).checkProcess();
                }
            );
    }
}
