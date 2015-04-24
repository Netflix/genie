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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import java.util.Set;

/**
 * The common entity fields for all Genie entities.
 *
 * @author amsharma
 * @author tgianos
 */
@MappedSuperclass
public class CommonEntityFields extends Auditable {

    private static final Logger LOG = LoggerFactory.getLogger(CommonEntityFields.class);

    /**
     * The namespace to use for genie specific tags.
     */
    protected static final String GENIE_TAG_NAMESPACE = "genie.";

    /**
     * The namespace to use for the id.
     */
    protected static final String GENIE_ID_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "id:";

    /**
     * The namespace to use for the name.
     */
    protected static final String GENIE_NAME_TAG_NAMESPACE = GENIE_TAG_NAMESPACE + "name:";

    private static final int MAX_ID_TAG_NAMESPACE = 1;
    private static final int MAX_NAME_TAG_NAMESPACE = 1;
    private static final int MAX_TAG_GENIE_NAMESPACE = 2;

    /**
     * Version of this entity.
     */
    @Basic(optional = false)
    @Column(name = "version")
    @ApiModelProperty(
            value = "The version number",
            required = true
    )
    private String version;

    /**
     * User who created this entity.
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "User who created/owns this object",
            required = true
    )
    private String user;

    /**
     * Name of this entity.
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "The name to use",
            required = true
    )
    private String name;

    /**
     * Default constructor.
     */
    public CommonEntityFields() {
        super();
    }

    /**
     * Construct a new CommonEntity Object with all required parameters.
     *
     * @param name    The name of the entity. Not null/empty/blank.
     * @param user    The user who created the entity. Not null/empty/blank.
     * @param version The version of this entity. Not null/empty/blank.
     */
    public CommonEntityFields(
            final String name,
            final String user,
            final String version) {
        super();
        this.name = name;
        this.user = user;
        this.version = version;
    }

    /**
     * Before modifying database make sure everything is ok.
     *
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    @PrePersist
    @PreUpdate
    protected void onCreateOrUpdateCommonEntityFields() throws GeniePreconditionException {
        this.validate(this.name, this.user, this.version, null);
    }

    /**
     * Gets the version of this entity.
     *
     * @return version
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Sets the version for this entity.
     *
     * @param version version number for this entity
     */
    public void setVersion(final String version) {
        this.version = version;
    }

    /**
     * Gets the user that created this entity.
     *
     * @return user
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the user who created this entity.
     *
     * @param user user who created this entity. Not null/empty/blank.
     */
    public void setUser(final String user) {
        this.user = user;
    }


    /**
     * Gets the name for this entity.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this entity.
     *
     * @param name the new name of this entity. Not null/empty/blank
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate() throws GeniePreconditionException {
        String error = null;
        try {
            super.validate();
        } catch (final GeniePreconditionException ge) {
            error = ge.getMessage();
        }
        this.validate(this.name, this.user, this.version, error);
    }

    /**
     * Reusable method for adding the system tags to the set of tags.
     *
     * @param tags The tags to add the system tags to.
     * @throws GeniePreconditionException If a precondition is violated.
     */
    protected void addAndValidateSystemTags(final Set<String> tags) throws GeniePreconditionException {
        if (tags == null) {
            throw new GeniePreconditionException("No tags entered. Unable to continue.");
        }
        // Add Genie tags to the app and make sure if old ones existed they're removed
        tags.add(GENIE_ID_TAG_NAMESPACE + this.getId());
        String oldNameTag = null;
        for (final String tag : tags) {
            if (tag.startsWith(GENIE_NAME_TAG_NAMESPACE)
                    && !tag.equalsIgnoreCase(GENIE_NAME_TAG_NAMESPACE + this.name)) {
                oldNameTag = tag;
                break;
            }
        }
        if (oldNameTag != null) {
            tags.remove(oldNameTag);
        }
        tags.add(GENIE_NAME_TAG_NAMESPACE + this.name);


        int genieNameSpaceCount = 0;
        int genieIdTagCount = 0;
        int genieNameTagCount = 0;
        for (final String tag : tags) {
            if (tag.contains(GENIE_TAG_NAMESPACE)) {
                genieNameSpaceCount++;
                if (tag.contains(GENIE_ID_TAG_NAMESPACE)) {
                    genieIdTagCount++;
                } else if (tag.contains(GENIE_NAME_TAG_NAMESPACE)) {
                    genieNameTagCount++;
                }
            }
        }
        if (genieIdTagCount > MAX_ID_TAG_NAMESPACE) {
            throw new GeniePreconditionException(
                    "More Genie id namespace tags encountered ("
                            + genieIdTagCount
                            + ") than expected ("
                            + MAX_ID_TAG_NAMESPACE
                            + ")."
            );
        }
        if (genieNameTagCount > MAX_NAME_TAG_NAMESPACE) {
            throw new GeniePreconditionException(
                    "More Genie name namespace tags encountered ("
                            + genieNameTagCount
                            + ") than expected ("
                            + MAX_NAME_TAG_NAMESPACE
                            + ")."
            );
        }
        if (genieNameSpaceCount > MAX_TAG_GENIE_NAMESPACE) {
            throw new GeniePreconditionException(
                    "More Genie namespace tags encountered ("
                            + genieNameSpaceCount
                            + ") than expected ("
                            + MAX_TAG_GENIE_NAMESPACE
                            + ")."
            );
        }
    }

    /**
     * Helper method for checking the validity of required parameters.
     *
     * @param name The name of the application
     * @param user The user who created the application
     * @throws GeniePreconditionException
     */
    private void validate(
            final String name,
            final String user,
            final String version,
            final String error) throws GeniePreconditionException {
        final StringBuilder builder = new StringBuilder();
        if (StringUtils.isNotBlank(error)) {
            builder.append(error);
        }
        if (StringUtils.isBlank(user)) {
            builder.append("User name is missing and is required.\n");
        }
        if (StringUtils.isBlank(name)) {
            builder.append("Name is missing and is required.\n");
        }
        if (StringUtils.isBlank(version)) {
            builder.append("Version is missing and is required.\n");
        }
        if (builder.length() > 0) {
            builder.insert(0, "CommonEntityFields configuration errors:\n");
            final String msg = builder.toString();
            LOG.error(msg);
            throw new GeniePreconditionException(msg);
        }
    }
}
