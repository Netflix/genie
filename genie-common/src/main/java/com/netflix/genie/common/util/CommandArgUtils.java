/*
 *
 *  Copyright 2019 Netflix, Inc.
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
package com.netflix.genie.common.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Utilities for working with command args.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class CommandArgUtils {

    /**
     * The maximum allowable size for a single command argument.
     */
    public static final int MAX_COMMAND_ARG_LENGTH = 10_000;

    private CommandArgUtils() {
    }

    /**
     * Given a string of command args for a job this method will split it (if necessary) into chunks
     * that will fit into requirements within the system.
     *
     * @param commandArgs The command args to split
     * @return The list of command args split into storable sizes
     */
    public static List<String> splitCommandArgs(final String commandArgs) {
        return Arrays.asList(StringUtils.splitPreserveAllTokens(commandArgs, StringUtils.SPACE));
    }

    /**
     * Utility method to standardize how we rebuild a single command arg strig from a list of command args.
     *
     * @param commandArgs the Command args to use as building blocks
     * @return A string representation of the command args
     */
    public static String rebuildCommandArgString(final List<String> commandArgs) {
        return StringUtils.join(commandArgs, StringUtils.SPACE);
    }
}
