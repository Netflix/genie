/*
 *
 *  Copyright 2016 Netflix, Inc.
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

import com.netflix.genie.client.ClusterClient;
import com.netflix.genie.client.impl.GenieClientConfigurationCommonsConfigImpl;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import java.util.HashSet;
import java.util.Set;


/**
 * Sample class that exhibits how to use the JobClient class to run and monitor a job.
 *
 * @author amsharma
 */
@Slf4j
public final class SampleClusterClient {

    /**
     * Default private constructor.
     */
    private SampleClusterClient() {
        // private constructor
    }

    /**
     * Main method that uses the JobClient.
     *
     * @param args The args to the main application.
     * @throws Exception For all other issues.
     */
    public static void main(final String[] args) throws Exception {

        log.debug("Starting Execution.");

        final Configurations configs = new Configurations();
        final Configuration configuration = configs.properties("genie-client.properties");

        final ClusterClient clusterClient =
            new ClusterClient(new GenieClientConfigurationCommonsConfigImpl(configuration));

        // create  new cluster in Genie

        final Set<String> tags = new HashSet<>();
        tags.add("foo");
        tags.add("bar");

        final Set<String> configList = new HashSet<>();
        configList.add("config1");
        configList.add("configs2");

        final Cluster cluster1 = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster1")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .build();


        final Cluster cluster2 = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster2")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .build();


        final Cluster cluster3 = new Cluster.Builder("name", "user", "1.0", ClusterStatus.UP)
            .withId("cluster3")
            .withDescription("client Test")
            .withSetupFile("path to set up file")
            .withTags(tags)
            .withConfigs(configList)
            .build();


        clusterClient.deleteAllClusters();

        log.info("Creating cluster 1");
        clusterClient.createCluster(cluster1);

        log.info("Get cluster 1");
        log.info(clusterClient.getCluster("cluster1").toString());

        log.info("Creating cluster 2");
        clusterClient.createCluster(cluster2);

        log.info("Get cluster 2");
        log.info(clusterClient.getCluster("cluster2").toString());

        log.info("Creating cluster 3");
        clusterClient.createCluster(cluster3);

        log.info("Print all clusters");
        log.info(clusterClient.getClusters().toString());

        log.info("Delete cluster3");
        clusterClient.deleteCluster("cluster3");

        log.info("Print all clusters");
        log.info(clusterClient.getClusters().toString());

        log.info("Get all tags for cluster 1");
        log.info(clusterClient.getTagsForCluster("cluster1").toString());

        log.info("Delete tag foo for cluster1");
        clusterClient.removeTagFromCluster("cluster1", "foo");

        log.info("Get all tags for cluster 1");
        log.info(clusterClient.getTagsForCluster("cluster1").toString());

        log.info("Delete all tags for cluster1");
        clusterClient.removeAllTagsForCluster("cluster1");

        log.info("Get all tags for cluster 1");
        log.info(clusterClient.getTagsForCluster("cluster1").toString());

        log.info("Get all configs for cluster 1");
        log.info(clusterClient.getConfigsForCluster("cluster1").toString());

        log.info("Delete all configs for cluster1");
        clusterClient.removeAllConfigsForCluster("cluster1");

        log.info("Get all configs for cluster 1");
        log.info(clusterClient.getConfigsForCluster("cluster1").toString());

        log.info("Delete all clusters");
        clusterClient.deleteAllClusters();

        clusterClient.getClusters();
    }
}
