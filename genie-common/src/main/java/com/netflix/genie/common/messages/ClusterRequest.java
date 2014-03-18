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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.netflix.genie.common.model.ClusterElement;

/**
 * Represents request to the Cluster REST resource.
 *
 * @author skrishnan
 */
@XmlRootElement(name = "request")
public class ClusterRequest extends BaseRequest {

    private static final long serialVersionUID = -1L;

    private ClusterElement cluster;

    /**
     * Constructor.
     */
    public ClusterRequest() {
    }

    /**
     * Gets the cluster config that is part of this request.
     *
     * @return cluster config element
     */
    @XmlElement(name = "cluster")
    public ClusterElement getClusterConfig() {
        return cluster;
    }

    /**
     * Sets the cluster config for this request.
     *
     * @param cluster
     *            cluster config element to set
     */
    public void setClusterConfig(ClusterElement cluster) {
        this.cluster = cluster;
    }
}
