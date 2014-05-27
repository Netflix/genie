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

package com.netflix.genie.client.sample;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.netflix.config.ConfigurationManager;
import com.netflix.genie.client.ClusterConfigServiceClient;
import com.netflix.genie.common.model.ClusterConfig;
import com.netflix.genie.common.model.Types.ClusterStatus;

/**
 * A sample client demonstrating usage of the Execution Service Client.
 *
 * @author skrishnan
 *
 */
public final class ClusterConfigServiceSampleClient {

    private ClusterConfigServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code.
     *
     * @param args
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {

        // Initialize Eureka, if it is being used
        // System.out.println("Initializing Eureka");
        // ClusterConfigServiceClient.initEureka("test");

        System.out.println("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genieClient.ribbon.listOfServers",
                "localhost:7001");

        System.out.println("Initializing ClusterConfigServiceClient");
        ClusterConfigServiceClient client = ClusterConfigServiceClient.getInstance();

        String userName = "genietest";
        String name = "MY_TEST_CLUSTER_CONFIG";

//        System.out.println("Creating new config for cluster");
//        HiveConfigElement hiveConfigElement = new HiveConfigElement();
//        hiveConfigElement.setUser(userName);
//        String hiveConfigName = "MY_TEST_HIVE_CONFIG";
//        hiveConfigElement.setName(hiveConfigName);
//        hiveConfigElement.setType(Types.Configuration.TEST.name());
//        hiveConfigElement.setStatus(Types.ConfigStatus.INACTIVE.name());
//        hiveConfigElement.setS3HiveSiteXml("s3://BUCKET/PATH/TO/HIVE-SITE.XML");
//        HiveConfigServiceClient hiveConfigClient = HiveConfigServiceClient.getInstance();
//        hiveConfigElement = hiveConfigClient.createHiveConfig(hiveConfigElement);
//        String hiveConfigId = hiveConfigElement.getId();
//        System.out.println("Hive config created with id: " + hiveConfigId);

        System.out.println("Creating new cluster config");
        ClusterConfig clusterConfig = new ClusterConfig();
        clusterConfig.setUser(userName);
        clusterConfig.setName(name);
//        clusterConfigElement.setTest(Boolean.TRUE);
//        clusterConfigElement.setAdHoc(Boolean.FALSE);
        clusterConfig.setStatus(ClusterStatus.OUT_OF_SERVICE);
//        clusterConfigElement.setS3MapredSiteXml("s3://PATH/TO/MAPRED-SITE.XML");
//        clusterConfigElement.setS3HdfsSiteXml("s3://PATH/TO/HDFS-SITE.XML");
//        clusterConfigElement.setS3CoreSiteXml("s3://PATH/TO/CORE-SITE.XML");
//        clusterConfigElement.setTestHiveConfigId(hiveConfigId);

        clusterConfig = client.createClusterConfig(clusterConfig);
        String id = clusterConfig.getId();
        System.out.println("Cluster config created with id: " + id);

        System.out.println("Getting clusterConfigs using specified filter criteria");
        Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", name);
        params.put("adHoc", "false");
        params.put("test", "true");
        params.put("limit", "3");
        ClusterConfig[] responses = client.getClusterConfigs(params);
        for (ClusterConfig hce : responses) {
            System.out.println("Cluster Configs: {id, status, updateTime} - {"
                    + hce.getId() + ", " + hce.getStatus() + ", "
                    + hce.getUpdateTime() + "}");
        }

        System.out.println("Getting cluster config by id");
        clusterConfig = client.getClusterConfig(id);
        System.out.println("Cluster config status: " + clusterConfig.getStatus());

        System.out.println("Updating existing cluster config");
        clusterConfig.setStatus(ClusterStatus.TERMINATED);
        clusterConfig = client.updateClusterConfig(id, clusterConfig);
        System.out.println("Updated status: " + clusterConfig.getStatus()
                + " at time: " + clusterConfig.getUpdateTime());

        System.out.println("Deleting cluster config using id");
        clusterConfig = client.deleteClusterConfig(id);
        System.out.println("Deleted cluster config with id: " + clusterConfig.getId());

//        System.out.println("Deleting hive config using id");
//        hiveConfigElement = hiveConfigClient.deleteHiveConfig(hiveConfigId);
//        System.out.println("Deleted hive config with id: " + hiveConfigElement.getId());

        System.out.println("Done");
    }
}
