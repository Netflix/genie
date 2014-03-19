/*
 *
 *  Copyright 2013 Netflix, Inc.
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

package com.netflix.genie.common.messages;

import java.util.Arrays;


import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import com.netflix.genie.common.model.ClusterElement;

import com.netflix.genie.common.exceptions.CloudServiceException;

/**
 * Represents response from the Cluster REST resource.
 *
 * @author skrishnan
 */
@XmlRootElement(name = "response")
public class ClusterResponse extends BaseResponse {

    private static final long serialVersionUID = -1L;

    private String message;

    private ClusterElement[] clusters;

    /**
     * Constructor to be used if there is an error.
     *
     * @param error
     *            CloudServiceException to be returned
     */
    public ClusterResponse(CloudServiceException error) {
        super(error);
    }

    /**
     * Default constructor.
     */
    public ClusterResponse() {
    }

    /**
     * Human-readable message for client.
     *
     * @return message from the server
     */
    @XmlElement(name = "message")
    public String getMessage() {
        return message;
    }

    /**
     * Sets the human readable message to return to clients.
     *
     * @param message
     *            from the server
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Get a list of cluster configs for this response.
     *
     * @return array of cluster config elements
     */
    @XmlElementWrapper(name = "clusters")
    @XmlElement(name = "cluster")
    public ClusterElement[] getClusters() {
        if (clusters == null) {
            return null;
        } else {
            return Arrays.copyOf(clusters, clusters.length);
        }
    }

    /**
     * Set a list of cluster configs for this response.
     *
     * @param inClusterConfigs
     *            array of cluster config elements for this response
     */
    public void setClusters(ClusterElement[] inClusterConfigs) {
        if (inClusterConfigs == null) {
            this.clusters = null;
        } else {
            this.clusters = Arrays.copyOf(inClusterConfigs,
                    inClusterConfigs.length);
        }
    }
}
