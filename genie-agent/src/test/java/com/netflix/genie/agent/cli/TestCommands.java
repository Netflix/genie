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

package com.netflix.genie.agent.cli;

import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * Example AgentCommand and AgentCommandArguments used in tests.
 *
 * @author mprimi
 * @since 4.0.0
 */
final class TestCommands {

    private TestCommands() {
    }

    static class ExampleCommand1 implements AgentCommand {

        static final String NAME = "example1";
        static final String SHORT_NAME = "ex1";

        @Override
        public void run() {
        }
    }

    @Parameters(commandNames = {ExampleCommand1.NAME, ExampleCommand1.SHORT_NAME})
    static class ExampleCommandArgs1 implements AgentCommandArguments {

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return ExampleCommand1.class;
        }
    }

    static class ExampleCommand2 implements AgentCommand {

        static final String NAME = "example2";
        static final String SHORT_NAME = "ex2";

        @Override
        public void run() {
        }
    }

    @Parameters(commandNames = {ExampleCommand2.NAME, ExampleCommand2.SHORT_NAME})
    static class ExampleCommandArgs2 implements AgentCommandArguments {

        @Override
        public Class<? extends AgentCommand> getConsumerClass() {
            return ExampleCommand2.class;
        }
    }


    static List<AgentCommandArguments> allCommandArguments() {
        return ImmutableList.of(
            new ExampleCommandArgs1(),
            new ExampleCommandArgs2()
        );
    }

    static Set<String> allCommandNames() {
        return ImmutableSet.of(
            ExampleCommand1.NAME,
            ExampleCommand1.SHORT_NAME,
            ExampleCommand2.NAME,
            ExampleCommand2.SHORT_NAME
        );
    }
}
