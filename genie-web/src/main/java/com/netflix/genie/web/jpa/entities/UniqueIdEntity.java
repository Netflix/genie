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
package com.netflix.genie.web.jpa.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
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
@EqualsAndHashCode(of = "uniqueId", callSuper = false)
@ToString(of = "uniqueId", callSuper = true)
public class UniqueIdEntity extends AuditEntity {

    @Basic(optional = false)
    @Column(name = "unique_id", nullable = false, unique = true, updatable = false)
    @NotBlank(message = "A unique identifier is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String uniqueId = UUID.randomUUID().toString();

    @Basic(optional = false)
    @Column(name = "requested_id", nullable = false, updatable = false)
    private boolean requestedId;
}
