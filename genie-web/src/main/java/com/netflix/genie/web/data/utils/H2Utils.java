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
package com.netflix.genie.web.data.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Utilities for working with H2 database. In particular can write stored functions here that are pre-compiled.
 *
 * @author tgianos
 * @since 4.0.0
 */
public final class H2Utils {

    static final String V3_COMMAND_EXECUTABLE_QUERY = "SELECT `id`, `executable` FROM `commands`;";
    static final String V4_COMMAND_ARGUMENT_SQL = "INSERT INTO `command_executable_arguments` VALUES(?,?,?);";
    static final int V3_COMMAND_ID_INDEX = 1;
    static final int V3_COMMAND_EXECUTABLE_INDEX = 2;
    static final int V4_COMMAND_ID_INDEX = 1;
    static final int V4_COMMAND_ARGUMENT_INDEX = 2;
    static final int V4_COMMAND_ARGUMENT_ORDER_INDEX = 3;

    private H2Utils() {
    }

    /**
     * Split the existing command executable on any whitespace characters and insert them into the
     * {@code command_executable_arguments} table in order.
     * <p>
     * See: {@code src/main/resources/db/migration/h2/V4_0_0__Genie_4.sql} for usage
     *
     * @param con The database connection to use
     * @throws Exception On Error
     */
    public static void splitV3CommandExecutableForV4(final Connection con) throws Exception {
        try (
            PreparedStatement commandsQuery = con.prepareStatement(V3_COMMAND_EXECUTABLE_QUERY);
            PreparedStatement insertCommandArgument = con.prepareStatement(V4_COMMAND_ARGUMENT_SQL);
            ResultSet rs = commandsQuery.executeQuery()
        ) {
            while (rs.next()) {
                final long commandId = rs.getLong(V3_COMMAND_ID_INDEX);
                final String executable = rs.getString(V3_COMMAND_EXECUTABLE_INDEX);
                final String[] arguments = StringUtils.splitByWholeSeparator(executable, null);
                if (arguments.length > 0) {
                    insertCommandArgument.setLong(V4_COMMAND_ID_INDEX, commandId);
                    for (int i = 0; i < arguments.length; i++) {
                        insertCommandArgument.setString(V4_COMMAND_ARGUMENT_INDEX, arguments[i]);
                        insertCommandArgument.setInt(V4_COMMAND_ARGUMENT_ORDER_INDEX, i);
                        insertCommandArgument.executeUpdate();
                    }
                }
            }
        }
    }
}
