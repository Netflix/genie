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
package com.netflix.genie.core.util;

import com.google.common.collect.Maps;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.core.jobs.JobExecutionEnvironment;

import java.util.Map;

/**
 * Utility methods for metrics.
 *
 * @author mprimi
 * @since 3.1.0
 */
public final class MetricsUtils {

    /**
     * Utility class private constructor.
     */
    private MetricsUtils() {
    }

    /**
     * Convenience method to add failure status and exception cause to an existing map of tags.
     *
     * @param tagsMap   the map of tags
     * @param throwable the exception to be tagged
     * @return the updated map of tags
     */
    public static Map<String, String> addFailureTagsWithException(
        final Map<String, String> tagsMap,
        final Throwable throwable
    ) {
        tagsMap.put(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.FAILURE);
        tagsMap.put(MetricsConstants.TagKeys.EXCEPTION_CLASS, throwable.getClass().getCanonicalName());
        return tagsMap;
    }

    /**
     * Convenience method to add success tag to an existing map of tags.
     *
     * @param tagsMap the map of tags
     * @return the updated map of tags
     */
    public static Map<String, String> addSuccessTags(final Map<String, String> tagsMap) {
        tagsMap.put(MetricsConstants.TagKeys.STATUS, MetricsConstants.TagValues.SUCCESS);
        return tagsMap;
    }

    /**
     * Convenience method that creates a tag map pre-populated with success status.
     *
     * @return a new map containing success tags
     */
    public static Map<String, String> newSuccessTagsMap() {
        return addSuccessTags(Maps.newHashMap());
    }

    /**
     * Convenience method that creates a tag map pre-populated with failure status and exception details.
     *
     * @param t the exception
     * @return a new map containing failure tags
     */
    public static Map<String, String> newFailureTagsMapForException(final Throwable t) {
        return addFailureTagsWithException(Maps.newHashMap(), t);
    }

    /**
     * Convenience method to add common tags from job execution environment, for
     * slicing and dicing metrics, to the map of tags passed in.
     * Populates defaultIdValue in case ids are not present in the job execution environment
     * E.g defaultIdValue will be added to the tags maps for the cluster_id key in case cluster_id
     * does not exist in jobExecEnv
     *
     * @param tags           The map of tags
     * @param jobExecEnv     Genie job execution env
     * @param defaultIdValue Default value for missing id in jobExecEnv
     */
    public static void addCommonJobWorkflowMetricTags(final JobExecutionEnvironment jobExecEnv,
                                                      final Map<String, String> tags,
                                                      final String defaultIdValue) {
        addCommonJobWorkflowMetricTags(jobExecEnv.getCluster(), jobExecEnv.getCommand(), tags, defaultIdValue);
    }

    /**
     * Convenience method to add common tags related to Cluster and Command, for
     * slicing and dicing metrics, to the map of tags passed in.
     * Populates defaultIdValue in case ids are not present in the objects passed in
     * E.g defaultIdValue will be added to the tags maps for the cluster_id key in case cluster_id
     * does not exist in Cluster object
     *
     * @param tags           The map of tags
     * @param cluster        Cluster chosen for running the command
     * @param command        Command chosen to be executed
     * @param defaultIdValue Default value for missing id in jobExecEnv
     */
    public static void addCommonJobWorkflowMetricTags(final Cluster cluster,
                                                      final Command command,
                                                      final Map<String, String> tags,
                                                      final String defaultIdValue) {
        /* Add cluster tags */
        tags.put(MetricsConstants.TagKeys.CLUSTER_NAME, cluster.getName());
        tags.put(MetricsConstants.TagKeys.CLUSTER_ID, cluster.getId().orElse(defaultIdValue));

        /* Add command tags  */
        tags.put(MetricsConstants.TagKeys.COMMAND_NAME, command.getName());
        tags.put(MetricsConstants.TagKeys.COMMAND_ID, command.getId().orElse(defaultIdValue));

    }
}
