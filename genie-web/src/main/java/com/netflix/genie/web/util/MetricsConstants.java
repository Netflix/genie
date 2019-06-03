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
package com.netflix.genie.web.util;


/**
 * Used to store constants related to metric names.
 *
 * @author tgianos
 * @since 3.0.0
 */
public final class MetricsConstants {

    /**
     * Utility class private constructor.
     */
    private MetricsConstants() {
    }

    /**
     * Inner class for constants used as key to tag metrics.
     */
    public static final class TagKeys {

        /**
         * Key to tag metrics with exception class.
         */
        public static final String EXCEPTION_CLASS = "exceptionClass";

        /**
         * Key to tag metrics with application ID.
         */
        public static final String APPLICATION_ID = "applicationId";

        /**
         * Key to tag metrics with application name.
         */
        public static final String APPLICATION_NAME = "applicationName";

        /**
         * Key to tag metrics with cluster ID.
         */
        public static final String CLUSTER_ID = "clusterId";

        /**
         * Key to tag metrics with cluster name.
         */
        public static final String CLUSTER_NAME = "clusterName";

        /**
         * Key to tag metrics with command ID.
         */
        public static final String COMMAND_ID = "commandId";

        /**
         * Key to tag metrics with command name.
         */
        public static final String COMMAND_NAME = "commandName";

        /**
         * Key to tag a class name.
         */
        public static final String CLASS_NAME = "class";

        /**
         * Key to tag the status of a request or operation.
         */
        public static final String STATUS = "status";

        /**
         * Key to tag a username.
         */
        public static final String USER = "user";

        /**
         * Key to tag a hostname.
         */
        public static final String HOST = "host";

        /**
         * Key to tag a health indicator name.
         */
        public static final String HEALTH_INDICATOR = "healthIndicator";

        /**
         * Key to tag a health indicator status.
         */
        public static final String HEALTH_STATUS = "healthStatus";

        /**
         * Key to tag the user concurrent job limit.
         */
        public static final String JOBS_USER_LIMIT = "jobsUserLimit";

        /**
         * Utility class private constructor.
         */
        private TagKeys() {
        }
    }

    /**
     * Constants used as metrics tags values by various classes.
     */
    public static final class TagValues {
        /**
         * Tag value to denote success (used with TagKeys.STATUS).
         */
        public static final String SUCCESS = "success";

        /**
         * Tag value to denote failure (used with TagKeys.STATUS).
         */
        public static final String FAILURE = "failure";

        /**
         * Utility class private constructor.
         */
        private TagValues() {
        }
    }
}
