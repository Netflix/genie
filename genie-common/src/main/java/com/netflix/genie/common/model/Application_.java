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

import com.netflix.genie.common.model.Types.ApplicationStatus;
import javax.persistence.metamodel.SetAttribute;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

/**
 * Meta model representation of the Application entity.
 *
 * @author tgianos
 * @see Application
 */
//TODO: See if can generate these classes automatically every time via build
@StaticMetamodel(Application.class)
public class Application_ extends Auditable_ {

    public static volatile SingularAttribute<Application, String> name;
    public static volatile SingularAttribute<Application, ApplicationStatus> status;
    public static volatile SingularAttribute<Application, String> user;
    public static volatile SingularAttribute<Application, String> version;
    public static volatile SingularAttribute<Application, String> envPropFile;
    public static volatile SetAttribute<Application, String> configs;
    public static volatile SetAttribute<Application, String> jars;
    public static volatile SetAttribute<Application, Command> commands;
}
