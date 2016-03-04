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
    private UnixProcessChecker processChecker;

    /**
     * Setup for the tests.
     */
    @Before
    public void setup() {
        Assume.assumeTrue(SystemUtils.IS_OS_UNIX);
        this.executor = Mockito.mock(Executor.class);
        this.processChecker = new UnixProcessChecker(PID, executor);
    }

    /**
     * Make sure the correct process is invoked.
     *
     * @throws IOException on error
     */
    @Test
    public void canCheckProcess() throws IOException {
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
        Assert.assertNotNull(argumentCaptor.getValue().getSubstitutionMap());
        Assert.assertThat(
            argumentCaptor.getValue().getSubstitutionMap().get(UnixProcessChecker.PID_KEY),
            Matchers.is(PID)
        );
    }
}
