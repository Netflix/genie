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
package com.netflix.genie.core.util;

import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GenieServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class to parse string (for command-line arguments, versions, etc).
 *
 * @author skrishnan
 * @author tgianos
 */
@Slf4j
public final class StringUtil {

    /**
     * The argument delimiter, which is set to white space.
     */
    private static final String ARG_DELIMITER = "\\s";

    /**
     * Should never be called.
     */
    protected StringUtil() {
    }

    /**
     * Mimics bash command-line parsing as close as possible.<br>
     * Caveat - only supports double quotes, not single quotes.
     *
     * @param input command-line arguments as a string
     * @return argument array that is split using (as to close to) bash rules as
     * possible
     * @throws GenieException If there is any error
     */
    public static String[] splitCmdLine(final String input) throws GenieException {
        log.debug("Command line: {}", input);
        if (StringUtils.isBlank(input)) {
            return new String[0];
        }

        final String[] output;
        try {
            // ignore delimiter if it is within quotes
            output = input.trim().split("[" + ARG_DELIMITER
                + "]+(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        } catch (final Exception e) {
            final String msg = "Invalid argument: " + input;
            log.error(msg, e);
            throw new GenieServerException(msg, e);
        }

        // "cleanse" inputs - get rid of enclosing quotes
        for (int i = 0; i < output.length; i++) {
            // double quotes
            if (output[i].startsWith("\"") && output[i].endsWith("\"")) {
                output[i] = output[i].replaceAll("(^\")|(\"$)", "");
            }
            log.debug("{}: {}", i, output[i]);
        }
        return output;
    }

    /**
     * Returns a canonical version number with at most 3 digits of the form
     * X.Y.Z.<br>
     * 0.8.1.4 -&gt; 0.8.1, 0.8.2 -&gt; 0.8.2, 0.8 -&gt; 0.8.
     *
     * @param fullVersion input version number
     * @return trimmed version number as documented
     */
    public static String trimVersion(final String fullVersion) {
            log.debug("Returning canonical version for {}", fullVersion);
        if (fullVersion == null) {
            return null;
        }

        final String[] splits = fullVersion.split("\\.");
        final StringBuilder trimmedVersion = new StringBuilder();
        int i = 0;
        while (true) {
            trimmedVersion.append(splits[i]);
            if (i < splits.length - 1 && i < 2) {
                trimmedVersion.append(".");
                i++;
            } else {
                break;
            }
        }
        final String finalVersion = trimmedVersion.toString();
        log.debug("Canonical version for {} is {}", fullVersion, finalVersion);

        return trimmedVersion.toString();
    }
}
