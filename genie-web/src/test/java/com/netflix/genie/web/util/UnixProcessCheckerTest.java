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
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
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
public class UnixProcessCheckerTest {

    private static final int PID = 18243;

    private Executor executor;
    private UnixProcessChecker processChecker;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        this.executor = Mockito.mock(Executor.class);
        final Instant tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
        this.processChecker = new UnixProcessChecker(PID, this.executor, tomorrow);
    }

    /**
     * Make sure the correct process is invoked.
     *
     * @throws GenieTimeoutException on timeout
     * @throws IOException           on error
     */
    @Test
    public void canCheckProcess() throws GenieTimeoutException, IOException {
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        this.processChecker.checkProcess();
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertThat(argumentCaptor.getValue().getExecutable(), Matchers.is("ps"));
        Assert.assertThat(argumentCaptor.getValue().getArguments().length, Matchers.is(2));
        Assert.assertThat(argumentCaptor.getValue().getArguments()[0], Matchers.is("-p"));
        Assert.assertThat(
            argumentCaptor.getValue().getArguments()[1],
            Matchers.is(Integer.toString(PID))
        );
    }

    /**
     * Make sure if the timeout has been exceeded then an exception is thrown indicating the process should be killed.
     *
     * @throws GenieTimeoutException on timeout
     * @throws IOException           on any other error
     */
    @Test(expected = GenieTimeoutException.class)
    public void canCheckProcessTimeout() throws GenieTimeoutException, IOException {
        final Instant yesterday = Instant.now().minus(1, ChronoUnit.DAYS);
        this.processChecker = new UnixProcessChecker(PID, this.executor, yesterday);
        this.processChecker.checkProcess();
    }
}
