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
package com.netflix.genie.web.services.impl;

import com.google.common.collect.Sets;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.internal.dtos.v4.Cluster;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test for the cluster selector.
 *
 * @author tgianos
 */
class RandomizedClusterSelectorImplTest {

    private RandomizedClusterSelectorImpl clb;

    /**
     * Setup the tests.
     */
    @BeforeEach
    void setup() {
        this.clb = new RandomizedClusterSelectorImpl();
    }

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws GenieException For any problem if anything went wrong with the test.
     */
    @Test
    void testValidClusterList() throws GenieException {
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        final Cluster cluster2 = Mockito.mock(Cluster.class);
        final Cluster cluster3 = Mockito.mock(Cluster.class);
        Assertions.assertThat(
            this.clb.selectCluster(
                Sets.newHashSet(cluster1, cluster2, cluster3),
                Mockito.mock(JobRequest.class)
            )
        ).isNotNull();
    }

    /**
     * Test whether a cluster is returned from a set of candidates.
     *
     * @throws GenieException For any problem if anything went wrong with the test.
     */
    @Test
    void testValidClusterListOfOne() throws GenieException {
        final Cluster cluster1 = Mockito.mock(Cluster.class);
        Assertions
            .assertThat(
                this.clb.selectCluster(
                    Sets.newHashSet(cluster1),
                    Mockito.mock(JobRequest.class)
                )
            )
            .isEqualTo(cluster1);
    }
}
