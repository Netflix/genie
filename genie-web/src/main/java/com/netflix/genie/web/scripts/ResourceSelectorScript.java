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
package com.netflix.genie.web.scripts;

import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;

import java.util.Set;

/**
 * Interface for defining the contract between the selection of a resource from a set of resources for a given
 * job request.
 *
 * @param <R> The type of resource this script is selecting from
 * @author tgianos
 * @since 4.0.0
 */
public interface ResourceSelectorScript<R> {

    /**
     * Given the {@link JobRequest} and an associated set of {@literal resources} which matched the request criteria
     * invoke the configured script to see if a preferred resource is selected based on the current logic.
     *
     * @param resources  The set of resources of type {@literal R} which should be selected from
     * @param jobRequest The {@link JobRequest} that the resource will be running
     * @return A {@link ResourceSelectorScriptResult} instance
     * @throws ResourceSelectionException If an unexpected error occurs during selection
     */
    ResourceSelectorScriptResult<R> selectResource(
        Set<R> resources,
        JobRequest jobRequest
    ) throws ResourceSelectionException;
}
