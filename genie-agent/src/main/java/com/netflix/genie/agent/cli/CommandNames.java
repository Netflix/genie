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

import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Constants for command names.
 *
 * @author mprimi
 * @since 4.0.0
 */
final class CommandNames {

    static final String HELP = "help";

    static final String EXEC = "exec";

    static final String INFO = "info";

    static final String PING = "ping";

    static final String DOWNLOAD = "download";

    static final String RESOLVE = "resolve";

    private static final Set<Field> COMMAND_NAMES_FIELDS;

    static {
        final Field[] fields = CommandNames.class.getDeclaredFields();
        final List<Field> superFields = Arrays.asList(CommandNames.class.getSuperclass().getDeclaredFields());
        COMMAND_NAMES_FIELDS = Collections.unmodifiableSet(
            Arrays.stream(fields)
            .filter(f -> Modifier.isStatic(f.getModifiers()))
            .filter(f -> Modifier.isFinal(f.getModifiers()))
            .filter(f -> f.getType() == String.class)
            .filter(f -> !superFields.contains(f))
            .peek(f -> f.setAccessible(true))
            .collect(Collectors.toSet())
        );
    }

    private CommandNames() { }

    @VisibleForTesting
    static Set<Field> getCommandNamesFields() {
        return COMMAND_NAMES_FIELDS;
    }
}
