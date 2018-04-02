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

package com.netflix.genie.agent.utils;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for job environment variables.
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class EnvUtils {
    private static final Pattern SHELL_VARIABLE_REGEX = Pattern.compile(
        ".*\\$([a-zA-Z_][a-zA-Z0-9_]*).*"
    );

    private static final Pattern SHELL_VARIABLE_WITH_BRACES_REGEX = Pattern.compile(
        ".*\\$\\{([a-zA-Z_][a-zA-Z0-9_]*)}.*"
    );

    // Note: all regex expect values to be enclosed in single quotes because that's how the script (genie-env.sh)
    // outputs them.

    // Matches a line with a variable whose value is not multi-line.
    private static Pattern singleLineValuePattern = Pattern.compile("^(.+)='(.*)'$");
    // Matches a the first line of a variable whose value is multi-line.
    // I.e. variable name, equal sign, single-quote, followed by anything but another single quote.
    private static Pattern multiLineValueBeginPattern = Pattern.compile("^(.+)='([^']*)$");
    // Matches an intermediate line of a multi-line variable value.
    // I.e. anything but a single quote.
    private static Pattern multiLineValueMiddlePattern = Pattern.compile("^([^']*)$");
    // Matches the final line of a multi-line variable value.
    // I.e. anything followed by a single quote and end of line.
    private static Pattern multiLineValueEndPattern = Pattern.compile("^(.*)[']$");

    private EnvUtils() {
    }

    /**
     * Parse a file containing shell variable declarations.
     * Expects all values to be single-quoted. Lines not conforming are silently skipped.
     * Supports multi-line values.
     *
     * @param environmentFile file to parse
     * @return a map of the values parsed
     * @throws IOException if the file cannot be read
     * @throws ParseException if the file contents cannot be parsed
     */
    public static Map<String, String> parseEnvFile(
        final File environmentFile
    ) throws IOException, ParseException {
        return parseEnvStream(new FileInputStream(environmentFile));
    }

    /**
     * Parse a file containing shell variable declarations.
     * Expects all values to be single-quoted. Lines not conforming are silently skipped.
     * Supports multi-line values.
     *
     * @param inputStream stream to parse
     * @return a map of the values parsed
     * @throws ParseException if the file contents cannot be parsed
     */
    public static Map<String, String> parseEnvStream(
        final InputStream inputStream
    ) throws ParseException {

        final Map<String, String> parsedEnvMap = Maps.newHashMap();

        // Scan for single-line values
        try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.toString())) {

            while (scanner.hasNextLine()) {
                final String line = scanner.nextLine();

                // Skip empty lines
                if (StringUtils.isBlank(line)) {
                    continue;
                }

                final Matcher singleLineMatcher = singleLineValuePattern.matcher(line);
                if (singleLineMatcher.matches()) {
                    parsedEnvMap.put(
                        singleLineMatcher.group(1),
                        singleLineMatcher.group(2));
                    continue;
                }

                final Matcher multiLineBeginMatcher = multiLineValueBeginPattern.matcher(line);
                if (multiLineBeginMatcher.matches()) {
                    final StringBuilder stringBuilder = new StringBuilder();

                    stringBuilder
                        .append(multiLineBeginMatcher.group(2))
                        .append('\n');

                    boolean multiLineEndFound = false;
                    while (scanner.hasNextLine()) {
                        final String midValueLine = scanner.nextLine();

                        final Matcher multiLineEndMatcher = multiLineValueEndPattern.matcher(midValueLine);
                        if (multiLineEndMatcher.matches()) {
                            stringBuilder
                                .append(multiLineEndMatcher.group(1));

                            parsedEnvMap.put(
                                multiLineBeginMatcher.group(1),
                                stringBuilder.toString()
                            );

                            multiLineEndFound = true;
                            break;
                        }

                        final Matcher multiLineMiddleMatcher = multiLineValueMiddlePattern.matcher(midValueLine);
                        if (multiLineMiddleMatcher.matches()) {
                            stringBuilder
                                .append(multiLineMiddleMatcher.group(1))
                                .append('\n');
                            continue;
                        }

                        throw new ParseException(
                            "Unexpected line while parsing multi-line variable: \'"
                            + multiLineBeginMatcher.group(1)
                            + "\': \""
                            + midValueLine
                            + "\""
                        );
                    }

                    if (!multiLineEndFound) {
                        throw new ParseException(
                            "Reached end of stream without finding end of multi-line variable: "
                                + multiLineBeginMatcher.group(1)
                        );
                    }

                    continue;
                }

                throw new ParseException(
                    "Unexpected line: \""
                        + line
                        + "\""
                );

            }
        }

        return parsedEnvMap;
    }

    /**
     * Performs shell environment variables expansion on the given string.
     *
     * @param inputString a string, possibly containing shell variables
     * @param environmentVariablesMap a map of string environment variables
     * @return a new string with variables expanded
     * @throws VariableSubstitutionException if the value for a variable appearing in input is not found in the map
     */
    public static String expandShellVariables(
        final String inputString,
        final Map<String, String> environmentVariablesMap
    ) throws VariableSubstitutionException {
        String outputString = inputString;

        boolean variableSubstituted = true;
        while (variableSubstituted) {
            variableSubstituted = false;

            Matcher matcher;

            matcher = SHELL_VARIABLE_REGEX.matcher(outputString);
            if (matcher.matches()) {
                final String variableName = matcher.group(1);
                final String variableValue = environmentVariablesMap.get(variableName);
                if (variableValue == null) {
                    throw new VariableSubstitutionException(variableName, environmentVariablesMap);
                }
                outputString = outputString.replaceAll("\\$" + variableName, variableValue);
                variableSubstituted = true;
            }

            matcher = SHELL_VARIABLE_WITH_BRACES_REGEX.matcher(outputString);
            if (matcher.matches()) {
                final String variableName = matcher.group(1);
                final String variableValue = environmentVariablesMap.get(variableName);
                if (variableValue == null) {
                    throw new VariableSubstitutionException(variableName, environmentVariablesMap);
                }
                outputString = outputString.replaceAll("\\$\\{" + variableName + "}", variableValue);
                variableSubstituted = true;
            }
        }

        return outputString;
    }

    /**
     * Exception for parsing errors.
     */
    public static final class ParseException extends Exception {
        private ParseException(final String message) {
            super(message);
        }
    }

    /**
     * Exception for failed variable expansion due to missing value.
     */
    public static final class VariableSubstitutionException extends Exception {
        private VariableSubstitutionException(final String variableName, final Map<String, String> envMap) {
            super(
                "Failed to substitute variable: "
                    + variableName
                    + "(environment variables: "
                    + Arrays.toString(envMap.keySet().toArray())
                    + ")"
            );
        }
    }
}
