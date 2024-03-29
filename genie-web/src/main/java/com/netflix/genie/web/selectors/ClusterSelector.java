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

import com.netflix.genie.common.internal.dtos.Cluster;
import com.netflix.genie.common.internal.dtos.JobRequest;
import org.springframework.validation.annotation.Validated;

/**
 * Interface for any classes which provide a way to select a {@link Cluster} from a set of clusters
 * which matched criterion provided by a user in a {@link JobRequest} and combined with the criteria for the command
 * selected for a given job.
 *
 * @author tgianos
 * @since 2.0.0
 */
@Validated
public interface ClusterSelector extends ResourceSelector<Cluster, ClusterSelectionContext> {
}
