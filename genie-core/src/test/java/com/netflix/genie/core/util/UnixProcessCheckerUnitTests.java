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
package com.netflix.genie.core.util;

import com.netflix.genie.common.exceptions.GenieTimeoutException;
import com.netflix.genie.test.categories.UnitTest;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.SystemUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Calendar;

/**
 * Unit tests for UnixProcessChecker.
 *
 * @author tgianos
 * @since 3.0.0
 */
@Category(UnitTest.class)
public class UnixProcessCheckerUnitTests {

    private static final int PID = 18243;

    private Executor executor;
    private Calendar tomorrow;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        this.executor = Mockito.mock(Executor.class);
        this.tomorrow = Calendar.getInstance();
        // For standard tests this will keep it from dying
        tomorrow.add(Calendar.DAY_OF_YEAR, 1);
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
        final UnixProcessChecker processChecker =
            new UnixProcessChecker(PID, this.executor, tomorrow.getTime(), false);
        processChecker.checkProcess();
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertThat(argumentCaptor.getValue().getExecutable(), Matchers.is("kill"));
        Assert.assertThat(argumentCaptor.getValue().getArguments().length, Matchers.is(2));
        Assert.assertThat(argumentCaptor.getValue().getArguments()[0], Matchers.is("-0"));
        Assert.assertThat(
            argumentCaptor.getValue().getArguments()[1],
            Matchers.is(Integer.toString(PID))
        );
    }

    /**
     * Make sure the correct process is invoked.
     *
     * @throws GenieTimeoutException on timeout
     * @throws IOException           on error
     */
    @Test
    public void canCheckProcessWithSudo() throws GenieTimeoutException, IOException {
        final ArgumentCaptor<CommandLine> argumentCaptor = ArgumentCaptor.forClass(CommandLine.class);
        final UnixProcessChecker processChecker =
            new UnixProcessChecker(PID, this.executor, tomorrow.getTime(), true);
        processChecker.checkProcess();
        Mockito.verify(this.executor).execute(argumentCaptor.capture());
        Assert.assertThat(argumentCaptor.getValue().getExecutable(), Matchers.is("sudo"));
        Assert.assertThat(argumentCaptor.getValue().getArguments().length, Matchers.is(3));
        Assert.assertThat(argumentCaptor.getValue().getArguments()[0], Matchers.is("kill"));
        Assert.assertThat(argumentCaptor.getValue().getArguments()[1], Matchers.is("-0"));
        Assert.assertThat(
            argumentCaptor.getValue().getArguments()[2],
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
        final Calendar yesterday = Calendar.getInstance();
        yesterday.add(Calendar.DAY_OF_YEAR, -1);
        new UnixProcessChecker.Factory(this.executor, false).get(PID, yesterday.getTime()).checkProcess();
    }
}
