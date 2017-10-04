/*
 *
 *  Copyright 2016 Netflix, Inc.
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
package com.netflix.genie.core.jobs;

import com.netflix.genie.common.dto.JobExecution;
import org.apache.commons.lang3.SystemUtils;
import java.util.TimeZone;

/**
 * A class holding some constants to be used everywhere.
 *
 * @author amsharma
 * @author tgianos
 * @since 3.0.0
 */
public final class JobConstants {

    /**
     * The header to use to mark a forwarded from another Genie node.
     */
    public static final String GENIE_FORWARDED_FROM_HEADER = "Genie-Forwarded-From";

    /**
     * Key used for look up of Job Execution environment object in a Context Map for workflows.
     **/
    public static final String JOB_EXECUTION_ENV_KEY = "jee";

    /**
     * Key used for look up of File Transfer object in a Context Map for workflows.
     **/
    public static final String FILE_TRANSFER_SERVICE_KEY = "fts";

    /**
     * Key used for look up of Job Execution DTO in a Context Map for workflows.
     **/
    public static final String JOB_EXECUTION_DTO_KEY = "jexecdto";

    /**
     * The launcher script name that genie creates to setup a job for running.
     **/
    public static final String GENIE_JOB_LAUNCHER_SCRIPT = "run";

    /**
     * Delimiter to be used while creating file paths.
     **/
    public static final String FILE_PATH_DELIMITER = "/";

    /**
     * Unix Pkill command.
     */
    public static final String UNIX_PKILL_COMMAND = "pkill";
    /**
     * Bash export command.
     **/
    public static final String EXPORT = "export ";

    /**
     * Equals symbol.
     **/
    public static final String EQUALS_SYMBOL = "=";

    /**
     * Double Quote symbol.
     **/
    public static final String DOUBLE_QUOTE_SYMBOL = "\"";

    /**
     * Bash source command.
     **/
    public static final String SOURCE = "source ";

    /**
     * Semicolon symbol.
     **/
    public static final String SEMICOLON_SYMBOL = ";";

    /**
     * String containing a whitespace.
     **/
    public static final String WHITE_SPACE = " ";

    /**
     * unix stdout symbol.
     **/
    public static final String STDOUT_REDIRECT = " > ";

    /**
     * unix stderr symbol.
     **/
    public static final String STDERR_REDIRECT = " 2> ";

    /**
     * File Path prefix to be used while creating paths for dependency files downloaded by Genie to local dir.
     **/
    public static final String DEPENDENCY_FILE_PATH_PREFIX = "dependencies";

    /**
     * File Path prefix to be used while creating paths for config files downloaded by Genie to local dir.
     **/
    public static final String CONFIG_FILE_PATH_PREFIX = "config";

    /**
     * Stderr Filename generated by Genie after running a job.
     **/
    public static final String STDERR_LOG_FILE_NAME = "stderr";

    /**
     * Stdout Filename generated by Genie after running a job.
     **/
    public static final String STDOUT_LOG_FILE_NAME = "stdout";

    /**
     * Done filename generated by Genie after running a job.
     **/
    public static final String GENIE_DONE_FILE_NAME = "genie/genie.done";

    /**
     * Temporary done filename generated by Genie after running a job as not to conflict/overwrite the
     * one that 'trap' handler might have created.
     **/
    public static final String GENIE_TEMPORARY_DONE_FILE_NAME = "genie/genie.done.temp";

    /**
     * "Kill reason" filename generated by Genie after killing a job.
     **/
    public static final String GENIE_KILL_REASON_FILE_NAME = "genie/kill-reason";

    /**
     * File created by Genie with details and trace for a job that failed to initialize.
     **/
    public static final String GENIE_INIT_FAILURE_MESSAGE_FILE_NAME = "initFailureDetails.txt";

    /**
     * Genie log file path.
     **/
    public static final String GENIE_LOG_PATH = "/genie/logs/genie.log";

    /**
     * Genie env file path.
     **/
    public static final String GENIE_ENV_PATH = "/genie/logs/env.log";

    /**
     * File Path prefix to be used while creating directories for application files to local dir.
     **/
    public static final String APPLICATION_PATH_VAR = "applications";

    /**
     * File Path prefix to be used while creating directories for command files to local dir.
     **/
    public static final String COMMAND_PATH_VAR = "command";

    /**
     * File Path prefix to be used while creating directories for cluster files to local dir.
     **/
    public static final String CLUSTER_PATH_VAR = "cluster";

    /**
     * File Path prefix to be used while creating working directory for jobs.
     **/
    public static final String GENIE_PATH_VAR = "genie";

    /**
     * File Path prefix to be used while creating working directory for jobs.
     **/
    public static final String LOGS_PATH_VAR = "logs";

    /**
     * Environment variable for Genie job working directory.
     **/
    public static final String GENIE_JOB_DIR_ENV_VAR = "GENIE_JOB_DIR";

    /**
     * Environment variable for Genie cluster directory.
     **/
    public static final String GENIE_CLUSTER_DIR_ENV_VAR = "GENIE_CLUSTER_DIR";

    /**
     * Environment variable for Genie cluster id.
     */
    public static final String GENIE_CLUSTER_ID_ENV_VAR = "GENIE_CLUSTER_ID";

    /**
     * Environment variable for the Genie cluster name.
     */
    public static final String GENIE_CLUSTER_NAME_ENV_VAR = "GENIE_CLUSTER_NAME";

    /**
     * Environment variable for the Genie cluster tags.
     */
    public static final String GENIE_CLUSTER_TAGS_ENV_VAR = "GENIE_CLUSTER_TAGS";

    /**
     * Environment variable for Genie command directory.
     **/
    public static final String GENIE_COMMAND_DIR_ENV_VAR = "GENIE_COMMAND_DIR";

    /**
     * Environment variable for Genie command id.
     */
    public static final String GENIE_COMMAND_ID_ENV_VAR = "GENIE_COMMAND_ID";

    /**
     * Environment variable for the Genie command name.
     */
    public static final String GENIE_COMMAND_NAME_ENV_VAR = "GENIE_COMMAND_NAME";

    /**
     * Environment variable for the Genie command tags.
     */
    public static final String GENIE_COMMAND_TAGS_ENV_VAR = "GENIE_COMMAND_TAGS";

    /**
     * Environment variable for Genie application directory.
     **/
    public static final String GENIE_APPLICATION_DIR_ENV_VAR = "GENIE_APPLICATION_DIR";

    /**
     * Environment variable for Genie Job ID.
     */
    public static final String GENIE_JOB_ID_ENV_VAR = "GENIE_JOB_ID";

    /**
     * Environment variable for Genie Job Name.
     */
    public static final String GENIE_JOB_NAME_ENV_VAR = "GENIE_JOB_NAME";

    /**
     * Environment variable for Genie Job Memory.
     */
    public static final String GENIE_JOB_MEMORY_ENV_VAR = "GENIE_JOB_MEMORY";

    /**
     * Environment variable for the Genie command tags in the job request.
     */
    public static final String GENIE_REQUESTED_COMMAND_TAGS_ENV_VAR = "GENIE_REQUESTED_COMMAND_TAGS";

    /**
     * Environment variable for the Genie cluster criteria tags in the job request.
     */
    public static final String GENIE_REQUESTED_CLUSTER_TAGS_ENV_VAR = "GENIE_REQUESTED_CLUSTER_TAGS";

    /**
     * Process ID.
     **/
    public static final String PID = "pid";

    /**
     * Genie Done file contents prefix.
     **/
    public static final String GENIE_DONE_FILE_CONTENT_PREFIX = "printf '{\"exitCode\": \"%s\"}\\n' \"$?\" > ";

    /**
     * Flag to send with the pkill command while killing jobs using the process group ids.
     */
    public static final String KILL_PROCESS_GROUP_FLAG = "-g";

    /**
     * Flag to send with the pkill command using the parent pid.
     */
    public static final String KILL_PARENT_PID_FLAG = "-P";

    /**
     * Key used to look up the writer object.
     */
    public static final String WRITER_KEY = "writer";

    /**
     * UTC timezone.
     */
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    /**
     * Environment variable run script children pid.
     */
    public static final String CHILDREN_PID_ENV_VAR = "CHILDREN_PID";

    /**
     * An object the encapsulates the kill handling logic to be added to the for each job.
     */
    public static final String JOB_FAILURE_AND_KILL_HANDLER_LOGIC = new StringBuilder()
        .append("#!/usr/bin/env bash\n\n")
        .append("set -o nounset -o pipefail\n\n")
        .append("# Set function in case any of the exports or source commands cause an error\n")
        .append("trap \"handle_failure\" ERR EXIT\n\n")
        .append("function handle_failure {\n")
        .append("    ERROR_CODE=$?\n")
        .append("    echo \"Handling exit signal (code: ${ERROR_CODE})\"\n")
        .append("\n")
        .append("    # Good exit\n")
        .append("    if [[ ${ERROR_CODE} -eq 0 ]]; then\n")
        .append("        exit 0\n")
        .append("    fi\n")
        .append("    # Bad exit\n")
        .append("    printf '{\"exitCode\": \"%s\"}\\n' \"${ERROR_CODE}\" > "
            + "${GENIE_JOB_DIR}/").append(GENIE_DONE_FILE_NAME).append("\n")
        .append("    exit \"${ERROR_CODE}\"\n")
        .append("}\n")
        .append("\n")
        .append("# Set function for handling kill signal from the job kill service\n")
        .append("trap \"handle_kill_request\" SIGTERM\n")
        .append("\n")
        .append("function handle_kill_request {\n")
        .append("\n")
        .append("    # Disable traps\n")
        .append("    trap \"\" SIGTERM ERR EXIT\n")
        .append("\n")
        .append("    echo \"Kill signal received\"\n")
        .append("\n")
        .append("    KILL_EXIT_CODE=").append(JobExecution.KILLED_EXIT_CODE).append("\n")
        .append("\n")
        .append("    ### Write the kill exit code to genie.done file as exit code before doing anything else\n")
        .append("    echo \"Generate done file with exit code ${KILL_EXIT_CODE}\"\n")
        .append("    printf '{\"exitCode\": \"%s\"}\\n' \"${KILL_EXIT_CODE}\" > "
            + "${GENIE_JOB_DIR}/").append(GENIE_DONE_FILE_NAME).append("\n")
        .append("\n")
        .append("    ### Send a kill signal the entire process group\n")
        .append("    echo \"Sending a kill signal to the process group\"\n")
        .append("    pkill -P $$\n")
        .append("\n")
        .append("    for ((iteration=1; iteration < 30; iteration++))\n")
        .append("    {\n")
        .append("        if kill -0 ${" + CHILDREN_PID_ENV_VAR + "} &> /dev/null;\n")
        .append("        then\n")
        .append("            echo \"Waiting for children of ${SELF_PID} to terminate\"\n")
        .append("            pgrep -P ${SELF_PID}\n")
        .append("            sleep 1\n")
        .append("        else\n")
        .append("            echo \"Children process no longer running, exiting\"\n")
        .append("            exit 0\n")
        .append("        fi\n")
        .append("    }\n")
        .append("\n")
        .append("    ### Reaching at this point means the children did not die. ")
        .append("If so send kill -9 to the entire process group\n")
        .append("    # this is a hard kill and will this process itself as well\n")
        .append("    echo \"Sending a kill -9 to children\"\n")
        .append("\n")
        .append("    pkill -9 -P $$\n")
        .append("    echo \"Done\"\n")
        .append("}\n")
        .append("\n")
        .append("SELF_PID=$$\n\n")
        .append("echo Start: `date '+%Y-%m-%d %H:%M:%S'`\n")
        .toString();

    /**
     * Protected constructor for utility class.
     */
    protected JobConstants() {
    }

    /**
     * Returns the appropriate flag to append to kill command based on the OS.
     *
     * @return The flag to use for kill command.
     */
    public static String getKillFlag() {
        if (SystemUtils.IS_OS_LINUX) {
            return KILL_PROCESS_GROUP_FLAG;
        } else {
            return KILL_PARENT_PID_FLAG;
        }
    }
}
