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

import java.util.Arrays;

/**
 * CLI utility methods.
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class Util {

    /**
     * Bare double dash is a command-line option conventionally used to delimit options from arguments.
     * See POSIX POSIX.1-2017 - Utility Conventions - Gudeline #10
     * http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap12.html
     */
    private static final String BARE_DOUBLE_DASH = "--";
    private static final String BARE_DOUBLE_DASH_REPLACEMENT = "-//-";

    /**
     * Hidden constructor.
     */
    private Util() {
    }

    /**
     * Replace all "bare double dash" arguments in the input array.
     *
     * @param args the arguments array
     * @return a new array with bare double dashes replaced by a special marker
     */
    public static String[] mangleBareDoubleDash(final String[] args) {
        return replaceAll(args, BARE_DOUBLE_DASH, BARE_DOUBLE_DASH_REPLACEMENT);
    }

    /**
     * Restore "bare double dash" arguments in the input array.
     *
     * @param args the arguments array with where double dashes were replaced
     * @return a new array with bare double dashes restored in place of the special marker
     */
    public static String[] unmangleBareDoubleDash(final String[] args) {
        return replaceAll(args, BARE_DOUBLE_DASH_REPLACEMENT, BARE_DOUBLE_DASH);
    }

    private static String[] replaceAll(final String[] args, final String from, final String to) {
        return Arrays.stream(args).map(arg -> from.equals(arg) ? to : arg).toArray(String[]::new);
    }
}
