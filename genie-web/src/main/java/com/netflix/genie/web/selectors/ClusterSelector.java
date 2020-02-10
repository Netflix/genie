/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.genie.web.selectors;

import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.Set;

/**
 * Interface for the cluster selector, which returns the "best" cluster to
 * run job on from an array of candidates.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Validated
public interface ClusterSelector {

    /**
     * Return best cluster to run job on.
     *
     * @param clusters   An immutable, non-empty list of available clusters to choose from
     * @param jobRequest The job request these clusters are being selected for
     * @return A {@link ResourceSelectionResult} which contains details about the outcome of the invocation
     * @throws ResourceSelectionException When the underlying implementation can't successfully come to a selection
     *                                    decision
     */
    ResourceSelectionResult<Cluster> selectCluster(
        @NotEmpty Set<@Valid Cluster> clusters,
        @Valid JobRequest jobRequest
    ) throws ResourceSelectionException;
}
