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
import com.netflix.genie.client.PigConfigServiceClient;
import com.netflix.genie.common.model.PigConfigElement;
import com.netflix.genie.common.model.Types;

/**
 * A sample client demonstrating usage of the Execution Service Client.
 *
 * @author skrishnan
 *
 */
public final class PigConfigServiceSampleClient {

    private PigConfigServiceSampleClient() {
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
        // PigConfigServiceClient.initEureka("test");

        System.out.println("Initializing list of Genie servers");
        ConfigurationManager.getConfigInstance().setProperty("genieClient.ribbon.listOfServers",
                "localhost:7001");

        System.out.println("Initializing PigConfigServiceClient");
        PigConfigServiceClient client = PigConfigServiceClient.getInstance();

        String userName = "genietest";
        String name = "MY_TEST_PIG_CONFIG";

        System.out.println("Creating new pig config");
        PigConfigElement pigConfigElement = new PigConfigElement();
        pigConfigElement.setUser(userName);
        pigConfigElement.setName(name);
        pigConfigElement.setType(Types.Configuration.TEST.name());
        pigConfigElement.setStatus(Types.ConfigStatus.INACTIVE.name());
        pigConfigElement.setS3PigProperties("s3://BUCKET/PATH/TO/PIG.PROPERTIES");
        pigConfigElement = client.createPigConfig(pigConfigElement);
        String id = pigConfigElement.getId();
        System.out.println("Pig config created with id: " + id);

        System.out.println("Getting pigConfigs using specified filter criteria");
        Multimap<String, String> params = ArrayListMultimap.create();
        params.put("name", name);
        params.put("limit", "3");
        PigConfigElement[] responses = client.getPigConfigs(params);
        for (PigConfigElement hce : responses) {
            System.out.println("Pig Configs: {id, status, updateTime} - {"
                    + hce.getId() + ", " + hce.getStatus() + ", "
                    + hce.getUpdateTime() + "}");
        }

        System.out.println("Getting pig config by id");
        pigConfigElement = client.getPigConfig(id);
        System.out.println("Pig config status: " + pigConfigElement.getStatus());

        System.out.println("Updating existing pig config");
        pigConfigElement.setStatus(Types.ConfigStatus.DEPRECATED.name());
        pigConfigElement = client.updatePigConfig(id, pigConfigElement);
        System.out.println("Updated status: " + pigConfigElement.getStatus()
                + " at time: " + pigConfigElement.getUpdateTime());

        System.out.println("Deleting pig config using id");
        pigConfigElement = client.deletePigConfig(id);
        System.out.println("Deleted pig config with id: " + pigConfigElement.getId());

        System.out.println("Done");
    }
}
