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

import com.netflix.genie.common.model.Types.JobStatus;
import javax.persistence.metamodel.SingularAttribute;
import javax.persistence.metamodel.StaticMetamodel;

/**
 * Meta model representation of the Application entity.
 *
 * @author tgianos
 * @see Job
 */
@StaticMetamodel(value = Job.class)
public class Job_ extends Auditable_ {

    public static volatile SingularAttribute<Job, String> applicationId;
    public static volatile SingularAttribute<Job, String> applicationName;
    public static volatile SingularAttribute<Job, String> archiveLocation;
    public static volatile SingularAttribute<Job, String> client;
    public static volatile SingularAttribute<Job, String> clientHost;
    public static volatile SingularAttribute<Job, String> clusterCriteriaString;
    public static volatile SingularAttribute<Job, String> commandArgs;
    public static volatile SingularAttribute<Job, String> commandId;
    public static volatile SingularAttribute<Job, String> commandName;
    public static volatile SingularAttribute<Job, String> description;
    public static volatile SingularAttribute<Job, Boolean> disableLogArchival;
    public static volatile SingularAttribute<Job, String> envPropFile;
    public static volatile SingularAttribute<Job, String> executionClusterId;
    public static volatile SingularAttribute<Job, String> executionClusterName;
    public static volatile SingularAttribute<Job, Integer> exitCode;
    public static volatile SingularAttribute<Job, String> fileDependencies;
    public static volatile SingularAttribute<Job, Long> finishTime;
    public static volatile SingularAttribute<Job, Boolean> forwarded;
    public static volatile SingularAttribute<Job, String> group;
    public static volatile SingularAttribute<Job, String> hostName;
    public static volatile SingularAttribute<Job, String> name;
    public static volatile SingularAttribute<Job, String> killURI;
    public static volatile SingularAttribute<Job, String> outputURI;
    public static volatile SingularAttribute<Job, Integer> processHandle;
    public static volatile SingularAttribute<Job, Long> startTime;
    public static volatile SingularAttribute<Job, JobStatus> status;
    public static volatile SingularAttribute<Job, String> statusMsg;
    public static volatile SingularAttribute<Job, String> email;
    public static volatile SingularAttribute<Job, String> user;
}
