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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nullable;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import java.util.HashSet;
import java.util.Set;

/**
 * Entity for criteria records.
 *
 * @author tgianos
 * @since 3.3.0
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(of = "tags", callSuper = false)
@ToString(callSuper = true)
@Entity
@Table(name = "criteria")
public class CriterionEntity extends IdEntity {
    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
        name = "criteria_tags",
        joinColumns = {
            @JoinColumn(name = "criterion_id", referencedColumnName = "id", nullable = false, updatable = false)
        },
        inverseJoinColumns = {
            @JoinColumn(name = "tag_id", referencedColumnName = "id", nullable = false, updatable = false)
        }
    )
    @NotEmpty(message = "Must have at least one tag associated with a criterion")
    private Set<TagEntity> tags = new HashSet<>();

    /**
     * Constructor.
     *
     * @param tags The tags to associate with this criterion
     */
    public CriterionEntity(@Nullable final Set<TagEntity> tags) {
        super();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }

    /**
     * Set all the tags associated to this criterion.
     *
     * @param tags The criterion tags to set
     */
    public void setTags(@Nullable final Set<TagEntity> tags) {
        this.tags.clear();
        if (tags != null) {
            this.tags.addAll(tags);
        }
    }
}
