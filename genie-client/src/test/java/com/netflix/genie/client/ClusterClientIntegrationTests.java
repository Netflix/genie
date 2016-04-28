package com.netflix.genie.client;

import com.netflix.genie.common.dto.Cluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;


/**
 * Integration Tests for Cluster Client.
 *
 * @author amsharma
 */
public class ClusterClientIntegrationTests extends GenieClientsIntegrationTestsBase {

    private static ClusterClient clusterClient;

    /**
     * Setup for tests.
     *
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        clusterClient = new ClusterClient(getBaseUrl());
    }

    /**
     * Delete all clusters between tests to make them idempotent.
     *
     * @throws Exception If there is any error.
     */
    @After
    public void cleanUp() throws Exception {
        //clusterClient.deleteAllClusters();
    }

    /**
     * Integration test to get all applications from Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanCreateCluster() throws Exception {

        final String id = UUID.randomUUID().toString();
        final Cluster cluster = constructClusterDTO(id);

        final String clusterId = clusterClient.createCluster(cluster);
        Assert.assertEquals(clusterId, id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Cluster cstr = clusterClient.getCluster(id);
    }

    @Test(expected = IOException.class)
    public void testClusterNotExist() throws Exception {
        clusterClient.getCluster("foo");
    }

    @Test
    public void testGetAllClusters() throws Exception {
        final List<Cluster> initialClusterList = clusterClient.getClusters();
        Assert.assertEquals(initialClusterList.size(), 0);

        final Cluster cluster1 = constructClusterDTO(null);
        final Cluster cluster2 = constructClusterDTO(null);

        clusterClient.createCluster(cluster1);
        clusterClient.createCluster(cluster2);

        final List<Cluster> finalClusterList = clusterClient.getClusters();
        Assert.assertEquals(finalClusterList.size(), 2);

        Assert.assertEquals(cluster1.getId(), finalClusterList.get(1).getId());
        Assert.assertEquals(cluster2.getId(), finalClusterList.get(0).getId());
    }
}
