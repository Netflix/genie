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
package com.netflix.genie.web.selectors.impl;

import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.common.external.dtos.v4.JobRequest;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import com.netflix.genie.web.selectors.ClusterSelector;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.Set;

/**
 * Basic implementation of a selector where a cluster is picked at random.
 *
 * @author tgianos
 */
@Slf4j
public class RandomClusterSelectorImpl extends RandomResourceSelectorBase<Cluster> implements ClusterSelector {

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceSelectionResult<Cluster> selectCluster(
        @NotEmpty final Set<@Valid Cluster> clusters,
        @Valid final JobRequest jobRequest
    ) throws ResourceSelectionException {
        log.debug("called");
        final ResourceSelectionResult.Builder<Cluster> builder = new ResourceSelectionResult.Builder<>(this.getClass());

        try {
            final Cluster selectedCluster = this.randomlySelect(clusters);
            return builder.withSelectionRationale(SELECTION_RATIONALE).withSelectedResource(selectedCluster).build();
        } catch (final Exception e) {
            throw new ResourceSelectionException(e);
        }
    }
}
