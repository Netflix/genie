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
package com.netflix.genie.web.selectors.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.external.dtos.v4.Command;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

/**
 * Tests for {@link RandomCommandSelectorImpl}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class RandomCommandSelectorImplTest {

    private RandomCommandSelectorImpl selector;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.selector = new RandomCommandSelectorImpl();
    }

    /**
     * Test whether a command is returned from a set of candidates.
     *
     * @throws ResourceSelectionException on unexpected error
     */
    @Test
    void testValidCommandSet() throws ResourceSelectionException {
        final Command command1 = Mockito.mock(Command.class);
        final Command command2 = Mockito.mock(Command.class);
        final Command command3 = Mockito.mock(Command.class);
        final Set<Command> commands = Sets.newHashSet(command1, command2, command3);
        final JobRequest jobRequest = Mockito.mock(JobRequest.class);
        for (int i = 0; i < 5; i++) {
            final ResourceSelectionResult<Command> result = this.selector.selectCommand(commands, jobRequest);
            Assertions.assertThat(result).isNotNull();
            Assertions.assertThat(result.getSelectorClass()).isEqualTo(RandomCommandSelectorImpl.class);
            Assertions.assertThat(result.getSelectedResource()).isPresent().get().isIn(commands);
            Assertions.assertThat(result.getSelectionRationale()).isPresent();
        }
    }

    /**
     * Test whether a command is returned from a set of candidates.
     *
     * @throws ResourceSelectionException on unexpected error
     */
    @Test
    void testValidCommandSetOfOne() throws ResourceSelectionException {
        final Command command = Mockito.mock(Command.class);
        final ResourceSelectionResult<Command> result = this.selector.selectCommand(
            Sets.newHashSet(command),
            Mockito.mock(JobRequest.class)
        );
        Assertions
            .assertThat(result.getSelectedResource())
            .isPresent()
            .contains(command);
    }
}
