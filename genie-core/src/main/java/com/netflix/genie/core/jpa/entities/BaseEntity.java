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
package com.netflix.genie.core.jpa.entities;

import com.netflix.genie.core.jpa.entities.projections.BaseProjection;
import com.netflix.genie.core.jpa.entities.projections.SetupFileProjection;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.hibernate.annotations.Type;
import org.hibernate.validator.constraints.NotBlank;

import javax.annotation.Nullable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.validation.constraints.Size;
import java.util.Optional;
import java.util.UUID;

/**
 * The base for all Genie top level entities. e.g. Applications, Jobs, etc.
 *
 * @author amsharma
 * @author tgianos
 * @since 2.0.0
 */
@Getter
@Setter
@EqualsAndHashCode(of = "uniqueId", callSuper = false)
@ToString(callSuper = true, exclude = {"description", "setupFile"})
@MappedSuperclass
public class BaseEntity extends AuditEntity implements BaseProjection, SetupFileProjection {

    private static final long serialVersionUID = -5040659007494311180L;

    @Basic(optional = false)
    @Column(name = "unique_id", nullable = false, unique = true, updatable = false)
    @NotBlank(message = "A unique identifier is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String uniqueId = UUID.randomUUID().toString();

    @Basic(optional = false)
    @Column(name = "version", nullable = false)
    @NotBlank(message = "Version is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String version;

    @Basic(optional = false)
    @Column(name = "genie_user", nullable = false)
    @NotBlank(message = "User name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String user;

    @Basic(optional = false)
    @Column(name = "name", nullable = false)
    @NotBlank(message = "Name is missing and is required.")
    @Size(max = 255, message = "Max length in database is 255 characters")
    private String name;

    @Lob
    @Basic(fetch = FetchType.LAZY)
    @Column(name = "description")
    @Type(type = "org.hibernate.type.TextType")
    private String description;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "setup_file")
    private FileEntity setupFile;

    /**
     * Default constructor.
     */
    BaseEntity() {
        super();
    }

    /**
     * Gets the description of this entity.
     *
     * @return description
     */
    public Optional<String> getDescription() {
        return Optional.ofNullable(this.description);
    }

    /**
     * Set the description.
     *
     * @param description The description. Null wipes database field
     */
    public void setDescription(@Nullable final String description) {
        this.description = description;
    }

    /**
     * Get the setup file for this entity.
     *
     * @return The setup file as an Optional in case it's null
     */
    public Optional<FileEntity> getSetupFile() {
        return Optional.ofNullable(this.setupFile);
    }

    /**
     * Set the setup file for this entity.
     *
     * @param setupFile The setup file. Null clears reference in the database
     */
    public void setSetupFile(@Nullable final FileEntity setupFile) {
        this.setupFile = setupFile;
    }
}
