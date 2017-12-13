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
package com.netflix.genie.core.services;

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import lombok.NonNull;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.core.Ordered;
import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * Interface for the cluster load-balancer, which returns the "best" cluster to
 * run job on from an array of candidates.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Validated
public interface ClusterLoadBalancer extends Ordered {

    /**
     * The default order to apply to any implementation that doesn't explicitly set one.
     *
     * @see org.springframework.core.Ordered
     * @see org.springframework.core.OrderComparator
     */
    int DEFAULT_ORDER = Ordered.LOWEST_PRECEDENCE - 1;

    /**
     * Return best cluster to run job on.
     *
     * @param clusters   An immutable, non-empty list of available clusters to choose from
     * @param jobRequest The job request these clusters are being load balanced for
     * @return the "best" cluster to run job on or null if no cluster selected
     * @throws GenieException if there is any error
     */
    @Nullable
    Cluster selectCluster(
        @Nonnull @NonNull @NotEmpty final Set<Cluster> clusters,
        @Nonnull @NonNull final JobRequest jobRequest
    ) throws GenieException;

    /**
     * {@inheritDoc}
     */
    @Override
    default int getOrder() {
        return DEFAULT_ORDER;
    }
}
