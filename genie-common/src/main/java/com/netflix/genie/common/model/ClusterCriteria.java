/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.genie.common.exceptions.CloudServiceException;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.Set;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster Criteria.
 *
 * @author amsharma
 * @author tgianos
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;
    private static final Logger LOG = LoggerFactory.getLogger(ClusterCriteria.class);

    private Set<String> tags = new HashSet<String>();

    /**
     * Default Constructor.
     */
    public ClusterCriteria() {
    }

    /**
     * Create a cluster criteria object with the included tags.
     *
     * @param tags The tags to add
     * @throws CloudServiceException
     */
    public ClusterCriteria(final Set<String> tags) throws CloudServiceException {
        if (tags == null || tags.isEmpty()) {
            final String msg = "No tags passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
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
     * @param tags The tags to set. Not null.
     * @throws CloudServiceException
     */
    public void setTags(final Set<String> tags) throws CloudServiceException {
        if (tags == null || tags.isEmpty()) {
            final String msg = "No tags passed in to set. Unable to continue.";
            LOG.error(msg);
            throw new CloudServiceException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
        this.tags = tags;
    }
}
