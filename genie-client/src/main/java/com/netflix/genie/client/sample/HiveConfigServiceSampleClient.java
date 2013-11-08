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
import com.netflix.genie.client.HiveConfigServiceClient;
import com.netflix.genie.common.model.HiveConfigElement;
import com.netflix.genie.common.model.Types;

/**
 * A sample client demonstrating usage of the Execution Service Client.
 *
 * @author skrishnan
 *
 */
public final class HiveConfigServiceSampleClient {

    private HiveConfigServiceSampleClient() {
        // never called
    }

    /**
     * Main for running client code.
     */
    public static void main(String[] args) throws Exception {

        // Initialize Eureka, if it is being used
        // System.out.println("Initializing Eureka");
        // HiveConfigServiceClient.initEureka("test");

        System.out.println("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genieClient.ribbon.listOfServers",
                "localhost:7001");

        System.out.println("Initializing HiveConfigServiceClient");
        HiveConfigServiceClient client = HiveConfigServiceClient.getInstance();

        String userName = "genietest";
        String name = "MY_TEST_HIVE_CONFIG";

        System.out.println("Creating new hive config");
        HiveConfigElement hiveConfigElement = new HiveConfigElement();
        hiveConfigElement.setUser(userName);
        hiveConfigElement.setName(name);
        hiveConfigElement.setType(Types.Configuration.TEST.name());
        hiveConfigElement.setStatus(Types.ConfigStatus.INACTIVE.name());
        hiveConfigElement.setS3HiveSiteXml("s3://BUCKET/PATH/TO/HIVE-SITE.XML");
        hiveConfigElement = client.createHiveConfig(hiveConfigElement);
        String id = hiveConfigElement.getId();
        System.out.println("Hive config created with id: " + id);

        System.out.println("Getting hiveConfigs using specified filter criteria");
        Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", name);
        params.put("limit", "3");
        HiveConfigElement[] responses = client.getHiveConfigs(params);
        for (HiveConfigElement hce : responses) {
            System.out.println("Hive Configs: {id, status, updateTime} - {"
                    + hce.getId() + ", " + hce.getStatus() + ", "
                    + hce.getUpdateTime() + "}");
        }

        System.out.println("Getting hive config by id");
        hiveConfigElement = client.getHiveConfig(id);
        System.out.println("Hive config status: " + hiveConfigElement.getStatus());

        System.out.println("Updating existing hive config");
        hiveConfigElement.setStatus(Types.ConfigStatus.DEPRECATED.name());
        hiveConfigElement = client.updateHiveConfig(id, hiveConfigElement);
        System.out.println("Updated status: " + hiveConfigElement.getStatus()
                + " at time: " + hiveConfigElement.getUpdateTime());

        System.out.println("Deleting hive config using id");
        hiveConfigElement = client.deleteHiveConfig(id);
        System.out.println("Deleted hive config with id: " + hiveConfigElement.getId());

        System.out.println("Done");
    }
}
