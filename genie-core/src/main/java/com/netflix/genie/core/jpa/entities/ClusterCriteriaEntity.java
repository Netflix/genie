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
package com.netflix.genie.core.jpa.entities;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.Set;

/**
 * Entity for a given cluster criteria.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Getter
@Setter
@ToString(callSuper = true)
@Entity
@Table(name = "cluster_criterias")
public class ClusterCriteriaEntity extends BaseEntity {
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "cluster_criterias_tags",
        joinColumns = {
            @JoinColumn(name = "cluster_criteria_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    private Set<TagEntity> tags = new HashSet<>();

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "job_id", nullable = false, updatable = false)
    private JobEntity job;
}
