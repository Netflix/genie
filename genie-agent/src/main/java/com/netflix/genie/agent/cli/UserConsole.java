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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.Environment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

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
     * This system property is set by Spring.
     */
    private static final String PID_SYSTEM_PROPERTY_NAME = "PID";
    /**
     * System property or environment variable to override the full path of the logfile.
     */
    private static final String LOG_FILE_PATH_OVERRIDE_PROPERTY_NAME = "GENIE_AGENT_TEMPORARY_LOG_FILE";
    /**
     * System property or environment variable to override the directory of the logfile.
     */
    private static final String LOG_DIRECTORY_OVERRIDE_PROPERTY_NAME = "GENIE_AGENT_TEMPORARY_LOG_DIRECTORY";

    /**
     * Fallback/Default value for log directory, if no override is present.
     */
    private static final String DEFAULT_LOG_DIRECTORY = "/tmp";
    /**
     * Fallback/Default value for log filename, if no override is present.
     */
    private static final String DEFAULT_LOG_FILENAME = String.format(
        "genie-agent-%s.log",
        System.getProperty(PID_SYSTEM_PROPERTY_NAME, "unknown-pid")
    );

    /**
     * Log directory evaluated considering overrides.
     */
    private static final String ACTUAL_LOG_DIRECTORY = System.getProperty(
        LOG_DIRECTORY_OVERRIDE_PROPERTY_NAME,
        System.getenv().getOrDefault(
            LOG_DIRECTORY_OVERRIDE_PROPERTY_NAME,
            DEFAULT_LOG_DIRECTORY
        )
    );

    /**
     * Expected log file path, used unless full path override is provided.
     */
    private static final String DEFAULT_LOG_FILE_PATH = ACTUAL_LOG_DIRECTORY + "/" + DEFAULT_LOG_FILENAME;
    /**
     * Stores the current location of the log file.
     * The logfile starts in a temporary location but may move inside the job folder during execution.
     * <p>
     * Note that this value does not set the location of the log, it just tries to guess it.
     * The file location is determined by the logback configuration (which is expected to follow the same logic).
     */
    private static final AtomicReference<Path> CURRENT_LOG_FILE_PATH = new AtomicReference<>(
        Paths.get(
            // Use property value if set
            System.getProperty(
                LOG_FILE_PATH_OVERRIDE_PROPERTY_NAME,
                // Otherwise use environment variable if set
                System.getenv().getOrDefault(
                    LOG_FILE_PATH_OVERRIDE_PROPERTY_NAME,
                    // Otherwise fallback to default
                    DEFAULT_LOG_FILE_PATH
                )
            )
        )
    );

    private static final Logger LOGGER = LoggerFactory.getLogger(CONSOLE_LOGGER_NAME);

    /**
     * Because the banner is printed in the log file and not visible to the user, manually re-print it
     * in {@code UserConsole}. Use the existing Spring configuration to control this behavior.
     */
    private static final String BANNER_LOCATION_SPRING_PROPERTY_KEY = SpringApplication.BANNER_LOCATION_PROPERTY;
    private static final String BANNER_CHARSET_SPRING_PROPERTY_KEY = "spring.banner.charset";

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
        return CURRENT_LOG_FILE_PATH.get().toString();
    }

    /**
     * Load and print the Spring banner (if one is configured) to UserConsole.
     *
     * @param environment the Spring environment
     */
    static void printBanner(final Environment environment) {
        try {
            final String bannerLocation = environment.getProperty(BANNER_LOCATION_SPRING_PROPERTY_KEY);
            if (StringUtils.isNotBlank(bannerLocation)) {
                final ResourceLoader resourceLoader = new DefaultResourceLoader();
                final Resource resource = resourceLoader.getResource(bannerLocation);
                if (resource.exists()) {
                    final String banner = StreamUtils.copyToString(
                        resource.getInputStream(),
                        environment.getProperty(
                            BANNER_CHARSET_SPRING_PROPERTY_KEY,
                            Charset.class,
                            StandardCharsets.UTF_8
                        )
                    );
                    UserConsole.getLogger().info(banner);
                }
            }
        } catch (final Throwable t) {
            System.err.println("Failed to print banner: " + t.getMessage());
            t.printStackTrace(System.err);
        }
    }

    /**
     * Move the log file from the current position to the given destination.
     * Refuses to move across filesystems. Moving within the same filesystem should not invalidate any open descriptors.
     *
     * @param destinationPath destination path
     * @throws IOException if source and destination are on different filesystem devices, if destination exists, or if
     *                     the move fails.
     */
    public static synchronized void relocateLogFile(final Path destinationPath) throws IOException {

        final Path sourcePath = CURRENT_LOG_FILE_PATH.get();
        final Path destinationAbsolutePath = destinationPath.toAbsolutePath();

        if (!Files.exists(sourcePath)) {
            throw new IOException("Log file does not exists: " + sourcePath.toString());
        } else if (Files.exists(destinationAbsolutePath)) {
            throw new IOException("Destination already exists: " + destinationAbsolutePath.toString());
        } else if (!sourcePath.getFileSystem().provider().equals(destinationAbsolutePath.getFileSystem().provider())) {
            throw new IOException("Source and destination are not in the same filesystem");
        }

        Files.move(sourcePath, destinationAbsolutePath);
        CURRENT_LOG_FILE_PATH.set(destinationAbsolutePath);

        getLogger().info("Agent log file relocated to: " + getLogFilePath());
    }
}
