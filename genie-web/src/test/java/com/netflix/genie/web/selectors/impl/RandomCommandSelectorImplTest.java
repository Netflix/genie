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
import com.netflix.genie.web.selectors.CommandSelectionContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;
import java.util.UUID;

/**
 * Tests for {@link RandomCommandSelectorImpl}.
 *
 * @author tgianos
 * @since 4.0.0
 */
class RandomCommandSelectorImplTest {

    private RandomCommandSelectorImpl selector;

    @BeforeEach
    void setup() {
        this.selector = new RandomCommandSelectorImpl();
    }

    @Test
    void testValidCommandSet() throws ResourceSelectionException {
        final Command command1 = Mockito.mock(Command.class);
        final Command command2 = Mockito.mock(Command.class);
        final Command command3 = Mockito.mock(Command.class);
        final Set<Command> commands = Sets.newHashSet(command1, command2, command3);
        final JobRequest jobRequest = Mockito.mock(JobRequest.class);
        final String jobId = UUID.randomUUID().toString();
        final CommandSelectionContext context = Mockito.mock(CommandSelectionContext.class);
        Mockito.when(context.getResources()).thenReturn(commands);
        Mockito.when(context.getJobId()).thenReturn(jobId);
        Mockito.when(context.getJobRequest()).thenReturn(jobRequest);
        for (int i = 0; i < 5; i++) {
            final ResourceSelectionResult<Command> result = this.selector.select(context);
            Assertions.assertThat(result).isNotNull();
            Assertions.assertThat(result.getSelectorClass()).isEqualTo(RandomCommandSelectorImpl.class);
            Assertions.assertThat(result.getSelectedResource()).isPresent().get().isIn(commands);
            Assertions.assertThat(result.getSelectionRationale()).isPresent();
        }
    }

    @Test
    void testValidCommandSetOfOne() throws ResourceSelectionException {
        final Command command = Mockito.mock(Command.class);
        final CommandSelectionContext context = Mockito.mock(CommandSelectionContext.class);
        Mockito.when(context.getResources()).thenReturn(Sets.newHashSet(command));
        Mockito.when(context.getJobId()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(context.getJobRequest()).thenReturn(Mockito.mock(JobRequest.class));
        final ResourceSelectionResult<Command> result = this.selector.select(context);
        Assertions
            .assertThat(result.getSelectedResource())
            .isPresent()
            .contains(command);
    }
}
