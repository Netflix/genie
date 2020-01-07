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
package com.netflix.genie.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.common.external.util.GenieObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringTokenizer;
import org.apache.commons.text.matcher.StringMatcherFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility methods for interacting with JSON.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class JsonUtils {
    /**
     * Protected constructor for a utility class.
     */
    protected JsonUtils() {
    }

    /**
     * Convert a Java object to a JSON string.
     *
     * @param value The Java object to marshall
     * @return The JSON string
     * @throws GenieException For any marshalling exception
     */
    public static String marshall(final Object value) throws GenieException {
        try {
            return GenieObjectMapper.getMapper().writeValueAsString(value);
        } catch (final JsonProcessingException jpe) {
            throw new GenieServerException("Failed to marshall object", jpe);
        }
    }

    /**
     * Convert a JSON string of a collection back to a Java object.
     *
     * @param source        The JSON string
     * @param typeReference The type reference of the collection to unmarshall to
     * @param <T>           The type of the collection ie Set of String
     * @return The Java object
     * @throws GenieException For any exception during unmarshalling
     */
    public static <T extends Collection> T unmarshall(
        final String source,
        final TypeReference<T> typeReference
    ) throws GenieException {
        try {
            if (StringUtils.isNotBlank(source)) {
                return GenieObjectMapper.getMapper().readValue(source, typeReference);
            } else {
                return GenieObjectMapper.getMapper().readValue("[]", typeReference);
            }
        } catch (final IOException ioe) {
            throw new GenieServerException("Failed to read JSON value", ioe);
        }
    }

    /**
     * Given a flat string of command line arguments this method will attempt to tokenize the string and split it for
     * use in DTOs. The split will occur on whitespace (tab, space, new line, carriage return) while respecting
     * single quotes to keep those elements together.
     * <p>
     * Example:
     * {@code "/bin/bash -xc 'echo "hello" world!'"} results in {@code ["/bin/bash", "-xc", "echo "hello" world!"]}
     *
     * @param commandArgs The original string representation of the command arguments
     * @return An ordered list of arguments
     */
    @Nonnull
    public static List<String> splitArguments(final String commandArgs) {
        final StringTokenizer tokenizer = new StringTokenizer(
            commandArgs,
            StringMatcherFactory.INSTANCE.splitMatcher(),
            StringMatcherFactory.INSTANCE.quoteMatcher()
        );

        return tokenizer.getTokenList();
    }

    /**
     * Given an ordered list of command line arguments join them back together as a space delimited String where
     * each argument is wrapped in {@literal '}.
     *
     * @param commandArgs The command arguments to join back together
     * @return The command arguments joined together or {@literal null} if there weren't any arguments
     */
    @Nullable
    public static String joinArguments(final List<String> commandArgs) {
        if (commandArgs.isEmpty()) {
            return null;
        } else {
            return commandArgs
                .stream()
                .map(argument -> '\'' + argument + '\'')
                .collect(Collectors.joining(StringUtils.SPACE));
        }
    }
}
