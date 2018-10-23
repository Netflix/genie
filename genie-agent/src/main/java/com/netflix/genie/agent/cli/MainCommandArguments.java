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

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Container for operand arguments (arguments meant for the underlying command, that the agent should not try to
 * interpret.
 *
 * @author mprimi
 * @since 4.0.0
 */
class MainCommandArguments {

    private final List<String> arguments = Lists.newArrayList();
    private final List<String> argumentsReadonlyView = Collections.unmodifiableList(arguments);

    List<String> get() {
        return argumentsReadonlyView;
    }

    void set(final String[] operandArguments) {
        arguments.addAll(Arrays.asList(operandArguments));
    }
}
