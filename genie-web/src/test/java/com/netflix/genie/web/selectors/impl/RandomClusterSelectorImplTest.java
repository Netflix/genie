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

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.external.dtos.v4.Cluster;
import com.netflix.genie.web.dtos.ResourceSelectionResult;
import com.netflix.genie.web.exceptions.checked.ResourceSelectionException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

/**
 * Test for {@link RandomClusterSelectorImpl}.
 *
 * @author tgianos
 */
class RandomClusterSelectorImplTest {

    private RandomClusterSelectorImpl clb;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.clb = new RandomClusterSelectorImpl();
    }

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws ResourceSelectionException For any problem if anything went wrong with the test.
     */
    @Test
    void testValidClusterSet() throws ResourceSelectionException {
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        final Cluster cluster2 = Mockito.mock(Cluster.class);
        final Cluster cluster3 = Mockito.mock(Cluster.class);
        final Set<Cluster> clusters = Sets.newHashSet(cluster1, cluster2, cluster3);
        final JobRequest jobRequest = Mockito.mock(JobRequest.class);
        for (int i = 0; i < 5; i++) {
            final ResourceSelectionResult<Cluster> result = this.clb.selectCluster(clusters, jobRequest);
            Assertions.assertThat(result).isNotNull();
            Assertions.assertThat(result.getSelectorClass()).isEqualTo(RandomClusterSelectorImpl.class);
            Assertions.assertThat(result.getSelectedResource()).isPresent().get().isIn(clusters);
            Assertions.assertThat(result.getSelectionRationale()).isPresent();
        }
    }

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws ResourceSelectionException For any problem if anything went wrong with the test.
     */
    @Test
    void testValidClusterSetOfOne() throws ResourceSelectionException {
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        final ResourceSelectionResult<Cluster> result = this.clb.selectCluster(
            Sets.newHashSet(cluster1),
            Mockito.mock(JobRequest.class)
        );
        Assertions
            .assertThat(result.getSelectedResource())
            .isPresent()
            .contains(cluster1);
    }
}
