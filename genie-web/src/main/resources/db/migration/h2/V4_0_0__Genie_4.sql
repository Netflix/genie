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

CREATE TABLE `command_executable_arguments` (
  `command_id`     BIGINT(20)    NOT NULL,
  `argument`       VARCHAR(1024) NOT NULL,
  `argument_order` INT(11)       NOT NULL,
  PRIMARY KEY (`command_id`, `argument_order`),
  CONSTRAINT `COMMAND_EXECUTABLE_ARGUMENTS_COMMAND_ID_FK` FOREIGN KEY (`command_id`) REFERENCES `commands` (`id`)
  ON DELETE CASCADE
);

CREATE INDEX `COMMAND_EXECUTABLE_ARGUMENTS_COMMAND_ID_INDEX`
  ON `command_executable_arguments` (`command_id`);

DROP ALIAS IF EXISTS SPLIT_COMMAND_EXECUTABLE;

CREATE ALIAS SPLIT_COMMAND_EXECUTABLE AS $$
import java.sql.Connection;
import java.sql.ResultSet;
@CODE
void splitCommandExecutable(final Connection con) throws Exception {
	final ResultSet rs = con.createStatement().executeQuery("SELECT `id`, `executable` FROM `commands`;");

	while (rs.next()) {
	    final long commandId = rs.getLong(1);
	    final String executable = rs.getString(2);
      final String[] arguments = executable.split("\\s");
      for (int i = 0; i < arguments.length; i++) {
          con
              .createStatement()
              .executeUpdate("INSERT INTO `command_executable_arguments` VALUES (" + commandId + ", '" + arguments[i] + "', " + i + ");");
      }
	}
}
$$;

CALL SPLIT_COMMAND_EXECUTABLE();

DROP ALIAS IF EXISTS SPLIT_COMMAND_EXECUTABLE;

ALTER TABLE `commands`
  DROP COLUMN `executable`;
