/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.services.loadbalancers.script;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dto.v4.Cluster;
import com.netflix.genie.common.internal.exceptions.unchecked.GenieClusterNotFoundException;
import com.netflix.genie.web.exceptions.checked.ScriptExecutionException;
import com.netflix.genie.web.exceptions.checked.ScriptNotConfiguredException;
import com.netflix.genie.web.scripts.ClusterLoadBalancerScript;
import com.netflix.genie.web.services.ClusterLoadBalancer;
import com.netflix.genie.web.util.MetricsConstants;
import com.netflix.genie.web.util.MetricsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotEmpty;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the {@link ClusterLoadBalancer} interface which uses user-provided script to make decisions
 * based on the list of clusters and the job request supplied.
 * <p>
 * Note: this LoadBalancer implementation intentionally returns 'null' (a.k.a. 'no preference') in case of error,
 * rather throwing an exception. For example if the script cannot be loaded, or if an invalid cluster is returned.
 * TODO: this logic of falling back to 'no preference' in case of error should be moved out of this implementation
 * and into the service using this interface.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Slf4j
public class ScriptLoadBalancer implements ClusterLoadBalancer {

    static final String SELECT_TIMER_NAME = "genie.jobs.clusters.loadBalancers.script.select.timer";
    private static final Cluster NO_PREFERENCE = null;

    private final MeterRegistry registry;
    private final ClusterLoadBalancerScript clusterLoadBalancerScript;

    /**
     * Constructor.
     *
     * @param clusterLoadBalancerScript the cluster load balancer script
     * @param registry the metrics registry
     */
    public ScriptLoadBalancer(
        final ClusterLoadBalancerScript clusterLoadBalancerScript,
        final MeterRegistry registry
    ) {
        this.clusterLoadBalancerScript = clusterLoadBalancerScript;
        this.registry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Cluster selectCluster(
        @Nonnull @NonNull @NotEmpty final Set<Cluster> clusters,
        @Nonnull @NonNull final JobRequest jobRequest
    ) throws GenieException {
        final long selectStart = System.nanoTime();
        log.debug("Called");
        final Set<Tag> tags = Sets.newHashSet();

        try {
            final Cluster selectedCluster = clusterLoadBalancerScript.selectCluster(jobRequest, clusters);
            MetricsUtils.addSuccessTags(tags);

            if (selectedCluster == null) {
                log.debug("Script returned null, no preference");
                tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, "null"));
                return NO_PREFERENCE;
            }

            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_ID, selectedCluster.getId()));
            tags.add(Tag.of(MetricsConstants.TagKeys.CLUSTER_NAME, selectedCluster.getMetadata().getName()));

            return selectedCluster;

        } catch (ScriptNotConfiguredException | ScriptExecutionException | GenieClusterNotFoundException e) {
            log.error("Cluster selection error: " + e.getMessage(), e);
            MetricsUtils.addFailureTagsWithException(tags, e);
            return NO_PREFERENCE;
        } finally {
            this.registry
                .timer(SELECT_TIMER_NAME, tags)
                .record(System.nanoTime() - selectStart, TimeUnit.NANOSECONDS);
        }
    }
}
