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
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster Criteria.
 *
 * @author amsharma
 * @author tgianos
 */
@ApiModel(description = "A set of cluster criteria for a job.")
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;
    private static final Logger LOG = LoggerFactory.getLogger(ClusterCriteria.class);

    @ApiModelProperty(
            value = "The tags which are ANDed together to select a viable cluster for the job",
            required = true
    )
    @NotEmpty(message = "No tags passed in to set and are required")
    private Set<String> tags = new HashSet<>();

    /**
     * Default Constructor.
     */
    public ClusterCriteria() {
    }

    /**
     * Create a cluster criteria object with the included tags.
     *
     * @param tags The tags to add. Not null or empty.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public ClusterCriteria(final Set<String> tags) throws GeniePreconditionException {
        this.checkTags(tags);
        this.tags = tags;
    }

    /**
     * Get the tags for this cluster criteria.
     *
     * @return The tags for this criteria as unmodifiable list
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Set the tags for the cluster criteria.
     *
     * @param tags The tags to set. Not null or empty.
     * @throws GeniePreconditionException If any precondition isn't met.
     */
    public void setTags(final Set<String> tags) throws GeniePreconditionException {
        this.checkTags(tags);
        this.tags = tags;
    }

    /**
     * Helper method for checking the tags.
     *
     * @param tags The tags to check
     * @throws GeniePreconditionException If the tags are null or empty.
     */
    private void checkTags(final Set<String> tags) throws GeniePreconditionException {
        if (tags == null || tags.isEmpty()) {
            final String msg = "No tags passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new GeniePreconditionException(msg);
        }
    }
}
