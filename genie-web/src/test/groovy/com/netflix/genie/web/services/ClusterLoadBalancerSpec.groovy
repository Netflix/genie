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
package com.netflix.genie.web.services

import com.netflix.genie.common.dto.Cluster
import com.netflix.genie.common.dto.JobRequest
import com.netflix.genie.common.exceptions.GenieException
import com.netflix.genie.test.categories.UnitTest
import lombok.NonNull
import org.junit.experimental.categories.Category
import org.springframework.core.Ordered
import spock.lang.Specification

import javax.annotation.Nonnull
import javax.validation.constraints.NotEmpty

/**
 * Specifications for the ClusterLoadBalancer interface.
 *
 * @author tgianos
 * @since 3.1.0
 */
@Category(UnitTest.class)
class ClusterLoadBalancerSpec extends Specification {

    def "The default order variable is the expected value"() {
        expect:
        assert ClusterLoadBalancer.DEFAULT_ORDER == Ordered.LOWEST_PRECEDENCE - 1
    }

    def "The default interface implementation returns the default order value"() {
        def loadBalancer = new ClusterLoadBalancer() {
            @Override
            Cluster selectCluster(
                    @Nonnull @NonNull @NotEmpty Set<Cluster> clusters,
                    @Nonnull @NonNull JobRequest jobRequest) throws GenieException {
                return null
            }
        }

        when:
        def order = loadBalancer.getOrder()

        then:
        order == ClusterLoadBalancer.DEFAULT_ORDER
    }
}
