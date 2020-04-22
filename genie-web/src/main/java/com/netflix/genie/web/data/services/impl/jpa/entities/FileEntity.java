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
package com.netflix.genie.web.data.services.impl.jpa.entities;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;

/**
 * File Entity.
 *
 * @author tgianos
 * @since 3.3.0
 */
@NoArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(
    callSuper = false,
    doNotUseGetters = true
)
@ToString(
    callSuper = true,
    doNotUseGetters = true
)
@Entity
@Table(name = "files")
public class FileEntity extends AuditEntity {

    @Basic(optional = false)
    @Column(name = "file", nullable = false, unique = true, updatable = false)
    @NotBlank(message = "Must have a file location associated with this entity")
    @Size(max = 1024, message = "Max length of a file is 1024 characters")
    private String file;

    /**
     * Constructor.
     *
     * @param file The file to reference
     */
    public FileEntity(final String file) {
        super();
        this.file = file;
    }
}
