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

import java.io.IOException;
import java.nio.file.Path;

/**
 * Utility to locate and relocate agent log file.
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface AgentLogManager {

    /**
     * Get the current location of the agent log file.
     *
     * @return a path string
     */
    Path getLogFilePath();

    /**
     * Attempt to relocate the agent log file inside the job directory.
     *
     * @param destinationPath the destination path
     * @throws IOException if relocation fails
     */
    void relocateLogFile(Path destinationPath) throws IOException;

}
