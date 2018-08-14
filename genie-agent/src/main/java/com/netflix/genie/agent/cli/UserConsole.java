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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for interacting with the user terminal/console.
 *
 * @author mprimi
 * @since 4.0.0
 */
public final class UserConsole {

    /**
     * The name of this logger must match the one explicitly whitelisted in the underlying logger configuration
     * consumed by Spring.
     */
    private static final String CONSOLE_LOGGER_NAME = "genie-agent";
    /**
     * This string path must match the one present in the log appender configuration consumed by Spring.
     */
    private static final String LOG_FILE_PATH = "/tmp/genie-agent-%s.log";
    /**
     * This system property is set by Spring.
     */
    private static final String PID_SYSTEM_PROPERTY_NAME = "PID";
    private static final Logger LOGGER = LoggerFactory.getLogger(CONSOLE_LOGGER_NAME);

    private UserConsole() {
    }

    /**
     * Get the LOGGER visible to user on the console.
     * All other LOGGER messages are logged on file only to avoid interfering with the job console output.
     *
     * @return a special Logger whose messages are visible on the user terminal.
     */
    public static Logger getLogger() {
        return LOGGER;
    }

    static String getLogFilePath() {
        return String.format(
            LOG_FILE_PATH,
            System.getProperty(PID_SYSTEM_PROPERTY_NAME, "???")
        );
    }
}
