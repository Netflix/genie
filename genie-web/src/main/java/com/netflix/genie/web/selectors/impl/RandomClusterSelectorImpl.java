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
import com.netflix.genie.web.selectors.ClusterSelectionContext;
import com.netflix.genie.web.selectors.ClusterSelector;

/**
 * Basic implementation of a {@link ClusterSelector} where a random {@link Cluster} is selected from the options
 * presented.
 *
 * @author tgianos
 * @since 2.0.0
 */
public class RandomClusterSelectorImpl
    extends RandomResourceSelector<Cluster, ClusterSelectionContext>
    implements ClusterSelector {
}
