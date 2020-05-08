/*
 *
 *  Copyright 2020 Netflix, Inc.
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
package com.netflix.genie.agent.cli.logging;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.FileAppender;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class that locates and relocates the agent log file.
 * This implementation is based on log4j2.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public final class AgentLogManagerLog4j2Impl implements AgentLogManager {

    /**
     * This name must match with the appender declaration in log4j2 configuration file.
     */
    private static final String AGENT_LOG_FILE_APPENDER_NAME = "AgentLogFile";
    private final AtomicReference<Path> logFilePath = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param context the log4j2 logger context
     */
    public AgentLogManagerLog4j2Impl(final LoggerContext context) {
        final FileAppender logFileAppender = context.getConfiguration().getAppender(AGENT_LOG_FILE_APPENDER_NAME);
        final String filename = logFileAppender.getFileName();
        if (StringUtils.isBlank(filename)) {
            throw new IllegalStateException("Could not determine location of agent log file");
        }
        this.logFilePath.set(Paths.get(filename));
        ConsoleLog.getLogger().info("Agent (temporarily) logging to: {}", filename);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path getLogFilePath() {
        return logFilePath.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void relocateLogFile(final Path destinationPath) throws IOException {

        final Path sourcePath = this.logFilePath.get();
        final Path destinationAbsolutePath = destinationPath.toAbsolutePath();

        log.info("Relocating agent log file from: {} to: {}", sourcePath, destinationAbsolutePath);

        if (!Files.exists(sourcePath)) {
            throw new IOException("Log file does not exists: " + sourcePath.toString());
        } else if (Files.exists(destinationAbsolutePath)) {
            throw new IOException("Destination already exists: " + destinationAbsolutePath.toString());
        } else if (!sourcePath.getFileSystem().provider().equals(destinationAbsolutePath.getFileSystem().provider())) {
            throw new IOException("Source and destination are not in the same filesystem");
        }

        Files.move(sourcePath, destinationAbsolutePath);

        this.logFilePath.set(destinationAbsolutePath);

        ConsoleLog.getLogger().info("Agent log file relocated to: " + destinationAbsolutePath);
    }
}
