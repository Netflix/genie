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

import com.netflix.genie.common.model.ClusterConfigElementOld;

/**
 * Represents request to the ClusterConfig REST resource.
 *
 * @author skrishnan
 */
@XmlRootElement(name = "request")
public class ClusterConfigRequestOld extends BaseRequest {

    private static final long serialVersionUID = -1L;

    private ClusterConfigElementOld clusterConfig;

    /**
     * Constructor.
     */
    public ClusterConfigRequestOld() {
    }

    /**
     * Gets the cluster config that is part of this request.
     *
     * @return cluster config element
     */
    @XmlElement(name = "clusterConfig")
    public ClusterConfigElementOld getClusterConfig() {
        return clusterConfig;
    }

    /**
     * Sets the cluster config for this request.
     *
     * @param clusterConfig
     *            cluster config element to set
     */
    public void setClusterConfig(ClusterConfigElementOld clusterConfig) {
        this.clusterConfig = clusterConfig;
    }
}
