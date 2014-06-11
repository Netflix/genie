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

import com.netflix.genie.common.model.Types.CommandStatus;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

/**
 * JPA Meta Model for the Command entity.
 *
 * @author tgianos
 * @see Command
 */
@StaticMetamodel(value = Command.class)
public class Command_ extends Auditable_ {

    public static volatile SingularAttribute<Command, String> envPropFile;
    public static volatile SingularAttribute<Command, String> executable;
    public static volatile SingularAttribute<Command, String> jobType;
    public static volatile SingularAttribute<Command, String> name;
    public static volatile SingularAttribute<Command, CommandStatus> status;
    public static volatile SingularAttribute<Command, String> user;
    public static volatile SingularAttribute<Command, String> version;
    public static volatile SetAttribute<Command, Application> applications;
    public static volatile SetAttribute<Command, Cluster> clusters;
    public static volatile SetAttribute<Command, String> configs;
}
