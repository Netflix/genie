/*
 *
 *  Copyright 2018 Netflix, Inc.
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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.Hibernate;

import jakarta.persistence.Basic;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.Objects;
import java.util.UUID;

/**
 * An extendable entity class for tables which have a UniqueId field.
 *
 * @author tgianos
 * @since 4.0.0
 */
@MappedSuperclass
@Getter
@Setter
@ToString(
    callSuper = true,
    doNotUseGetters = true
)
public class UniqueIdEntity extends AuditEntity {

    @Basic(optional = false)
    @Column(name = "unique_id", nullable = false, unique = true, updatable = false)
    @NotBlank(message = "A unique identifier is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String uniqueId = UUID.randomUUID().toString();

    @Basic(optional = false)
    @Column(name = "requested_id", nullable = false, updatable = false)
    private boolean requestedId;

    /**
     * {@inheritDoc}
     */
    @SuppressFBWarnings({"BC_EQUALS_METHOD_SHOULD_WORK_FOR_ALL_OBJECTS"})
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || Hibernate.getClass(this) != Hibernate.getClass(o)) {
            return false;
        }
        final UniqueIdEntity that = (UniqueIdEntity) o;
        return Objects.equals(this.uniqueId, that.uniqueId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return this.uniqueId.hashCode();
    }
}
