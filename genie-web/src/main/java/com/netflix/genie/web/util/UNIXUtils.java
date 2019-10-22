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
package com.netflix.genie.web.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.Executor;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Utility class for UNIX user and file permissions.
 * <p>
 * N.B. These are some old routines moved out from JobKickoffTask for reuse as part of V3 to V4 migration.
 * This class is expected to be deleted once execution is delegated to Agent.
 *
 * @author mprimi
 * @since 4.0.0
 */
@Slf4j
public final class UNIXUtils {

    private static final String SUDO = "sudo";

    /**
     * Private constructor.
     */
    private UNIXUtils() {
    }

    /**
     * Create user on the system.
     *
     * @param user     user id
     * @param group    group id
     * @param executor the command executor
     * @throws IOException If the user or group could not be created.
     */
    public static synchronized void createUser(
        final String user,
        @Nullable final String group,
        final Executor executor
    ) throws IOException {

        // First check if user already exists
        final CommandLine idCheckCommandLine = new CommandLine("id")
            .addArgument("-u")
            .addArgument(user);

        try {
            executor.execute(idCheckCommandLine);
            log.debug("User already exists");
        } catch (final IOException ioe) {
            log.debug("User does not exist. Creating it now.");

            // Determine if the group is valid by checking that its not null and not same as user.
            final boolean isGroupValid = StringUtils.isNotBlank(group) && !group.equals(user);

            // Create the group for the user if its not the same as the user.
            if (isGroupValid) {
                log.debug("Group and User are different so creating group now.");
                final CommandLine groupCreateCommandLine = new CommandLine(SUDO).addArgument("groupadd")
                    .addArgument(group);

                // We create the group and ignore the error as it will fail if group already exists.
                // If the failure is due to some other reason, then user creation will fail and we catch that.
                try {
                    log.debug("Running command to create group:  [{}]", groupCreateCommandLine);
                    executor.execute(groupCreateCommandLine);
                } catch (IOException ioexception) {
                    log.debug("Group creation threw an error as it might already exist", ioexception);
                }
            }

            final CommandLine userCreateCommandLine = new CommandLine(SUDO)
                .addArgument("useradd")
                .addArgument(user);
            if (isGroupValid) {
                userCreateCommandLine
                    .addArgument("-G")
                    .addArgument(group);
            }
            userCreateCommandLine
                .addArgument("-M");

            log.debug("Running command to create user: [{}]", userCreateCommandLine);
            executor.execute(userCreateCommandLine);
        }
    }

    /**
     * Change the ownership of a directory (recursively).
     *
     * @param dir      The directory to change the ownership of.
     * @param user     Userid of the user.
     * @param executor the command executor
     * @throws IOException if the operation fails
     */
    public static void changeOwnershipOfDirectory(
        final String dir,
        final String user,
        final Executor executor
    ) throws IOException {

        final CommandLine commandLine = new CommandLine(SUDO)
            .addArgument("chown")
            .addArgument("-R")
            .addArgument(user)
            .addArgument(dir);
        executor.execute(commandLine);
    }

    /**
     * Give write permission to the group owning a given file or directory.
     *
     * @param path     the path
     * @param executor the command executor
     * @throws IOException if the operation fails
     */
    public static void makeDirGroupWritable(final String path, final Executor executor) throws IOException {
        log.debug("Adding write permissions for the directory {} for the group.", path);
        final CommandLine commandLIne = new CommandLine(SUDO).addArgument("chmod").addArgument("g+w")
            .addArgument(path);
        executor.execute(commandLIne);
    }
}
