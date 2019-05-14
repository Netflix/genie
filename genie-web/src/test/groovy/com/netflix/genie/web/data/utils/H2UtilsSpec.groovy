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
package com.netflix.genie.web.data.utils

import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Specifications for {@link H2Utils}
 *
 * @author tgianos
 */
class H2UtilsSpec extends Specification {

    def "Can perform V3 -> V4 command argument migration"() {
        def connection = Mock(Connection)
        def query = Mock(PreparedStatement)
        def insert = Mock(PreparedStatement)
        def resultSet = Mock(ResultSet)
        def commandId = 245L

        when:
        H2Utils.splitV3CommandExecutableForV4(connection)

        then:
        1 * connection.prepareStatement(H2Utils.V3_COMMAND_EXECUTABLE_QUERY) >> query
        1 * connection.prepareStatement(H2Utils.V4_COMMAND_ARGUMENT_SQL) >> insert
        1 * query.executeQuery() >> resultSet
        2 * resultSet.next() >>> [true, false]
        1 * resultSet.getLong(H2Utils.V3_COMMAND_ID_INDEX) >> commandId
        1 * resultSet.getString(H2Utils.V3_COMMAND_EXECUTABLE_INDEX) >> "hello world!\tH2\nprocedure   test"
        1 * insert.setLong(H2Utils.V4_COMMAND_ID_INDEX, commandId)
        1 * insert.setString(H2Utils.V4_COMMAND_ARGUMENT_INDEX, "hello")
        1 * insert.setString(H2Utils.V4_COMMAND_ARGUMENT_INDEX, "world!")
        1 * insert.setString(H2Utils.V4_COMMAND_ARGUMENT_INDEX, "H2")
        1 * insert.setString(H2Utils.V4_COMMAND_ARGUMENT_INDEX, "procedure")
        1 * insert.setString(H2Utils.V4_COMMAND_ARGUMENT_INDEX, "test")
        1 * insert.setInt(H2Utils.V4_COMMAND_ARGUMENT_ORDER_INDEX, 0)
        1 * insert.setInt(H2Utils.V4_COMMAND_ARGUMENT_ORDER_INDEX, 1)
        1 * insert.setInt(H2Utils.V4_COMMAND_ARGUMENT_ORDER_INDEX, 2)
        1 * insert.setInt(H2Utils.V4_COMMAND_ARGUMENT_ORDER_INDEX, 3)
        1 * insert.setInt(H2Utils.V4_COMMAND_ARGUMENT_ORDER_INDEX, 4)
        5 * insert.executeUpdate()
    }
}
