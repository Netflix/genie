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
package com.netflix.genie.common.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotEmpty;
import java.io.Serializable;
import java.util.Set;

/**
 * Cluster Criteria.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Getter
@EqualsAndHashCode(doNotUseGetters = true)
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;

    @NotEmpty(message = "No valid (e.g. non-blank) tags present")
    private Set<String> tags;

    /**
     * Create a cluster criteria object with the included tags.
     *
     * @param tags The tags to add. Not null or empty and must have at least one non-empty tag.
     */
    @JsonCreator
    public ClusterCriteria(
        @JsonProperty("tags") final Set<String> tags
    ) {
        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        tags.forEach(
            tag -> {
                if (StringUtils.isNotBlank(tag)) {
                    builder.add(tag);
                }
            }
        );
        this.tags = builder.build();
    }
}
