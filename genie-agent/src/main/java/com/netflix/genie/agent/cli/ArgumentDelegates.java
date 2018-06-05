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

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.genie.common.internal.dto.v4.Criterion;

import java.io.File;
import java.util.List;
import java.util.Set;

/**
 * Interfaces for command-line delegates (groups of options shared by multiple commands).
 *
 * @author mprimi
 * @since 4.0.0
 */
public interface ArgumentDelegates {

    /**
     * Delegate for server connection options.
     */
    interface ServerArguments {

        String getServerHost();

        int getServerPort();

        long getRpcTimeout();
    }

    /**
     * Delegate for agent dependencies cache.
     */
    interface CacheArguments {

        File getCacheDirectory();

    }

    /**
     * Delegate for agent job request parameters.
     */
    interface JobRequestArguments {

        List<String> getCommandArguments();

        File getJobDirectoryLocation();

        boolean isInteractive();

        boolean isArchivalDisabled();

        Integer getTimeout();

        String getJobId();

        List<Criterion> getClusterCriteria();

        Criterion getCommandCriterion();

        List<String> getApplicationIds();

        String getJobName();

        String getUser();

        String getEmail();

        String getGrouping();

        String getGroupingInstance();

        String getJobDescription();

        Set<String> getJobTags();

        String getJobVersion();

        JsonNode getJobMetadata();

        boolean isJobRequestedViaAPI();
    }

}
