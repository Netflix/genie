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

import com.netflix.genie.common.model.Types.ClusterStatus;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

/**
 * JPA Meta Model for the Cluster entity.
 *
 * @author tgianos
 * @see Cluster
 */
@StaticMetamodel(value = Cluster.class)
public class Cluster_ extends Auditable_ {

    public static volatile SingularAttribute<Cluster, String> jobManager;
    public static volatile SingularAttribute<Cluster, String> name;
    public static volatile SingularAttribute<Cluster, ClusterStatus> status;
    public static volatile SingularAttribute<Cluster, String> user;
    public static volatile SingularAttribute<Cluster, String> version;
    public static volatile SetAttribute<Cluster, String> tags;
    public static volatile SetAttribute<Cluster, Command> commands;
    public static volatile SetAttribute<Cluster, String> configs;
}
