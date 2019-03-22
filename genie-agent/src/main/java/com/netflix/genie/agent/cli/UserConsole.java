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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
        return String.format(
            LOG_FILE_PATH,
            System.getProperty(PID_SYSTEM_PROPERTY_NAME, "???")
        );
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
}
