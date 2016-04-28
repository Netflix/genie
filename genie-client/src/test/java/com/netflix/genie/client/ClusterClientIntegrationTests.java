package com.netflix.genie.client;

import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
     * Integration test to get all applications from Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testCanCreateAndGetCluster() throws Exception {

        final String id = UUID.randomUUID().toString();
        final Cluster cluster = constructClusterDTO(id);

        final String clusterId = clusterClient.createCluster(cluster);
        Assert.assertEquals(clusterId, id);

        // Make sure Genie Not Found Exception is not thrown for this call.
        final Cluster cstr = clusterClient.getCluster(id);

        // Make sure the object returned is exactly what was sent to be created
        Assert.assertEquals(cluster.getId(), cstr.getId());
        Assert.assertEquals(cluster.getName(), cstr.getName());
        Assert.assertEquals(cluster.getDescription(), cstr.getDescription());
        Assert.assertEquals(cluster.getConfigs(), cstr.getConfigs());
        Assert.assertEquals(cluster.getSetupFile(), cstr.getSetupFile());
        Assert.assertEquals(cstr.getTags().contains("foo"), true);
        Assert.assertEquals(cstr.getTags().contains("bar"), true);
        Assert.assertEquals(cluster.getStatus(), cstr.getStatus());
    }

    @Test
    public void testGetClustersUsingParams() throws Exception {
        final String cluster1Id = UUID.randomUUID().toString();
        final String cluster2Id = UUID.randomUUID().toString();

        final Set<String> cluster1Tags = new HashSet<>();
        cluster1Tags.add("foo");
        cluster1Tags.add("pi");

        final Set<String> cluster2Tags = new HashSet<>();
        cluster2Tags.add("bar");
        cluster2Tags.add("pi");


        final Cluster cluster1 = new Cluster.Builder("cluster1name", "cluster1user", "1.0", ClusterStatus.UP)
            .withId(cluster1Id)
            .withTags(cluster1Tags)
            .build();

        final Cluster cluster2 =
            new Cluster.Builder("cluster2name", "cluster2user", "2.0", ClusterStatus.OUT_OF_SERVICE)
            .withId(cluster2Id)
            .withTags(cluster2Tags)
            .build();

        clusterClient.createCluster(cluster1);
        clusterClient.createCluster(cluster2);

        // Test get by tags
        List<Cluster> clusterList = clusterClient.getClusters(
            null,
            null,
            Arrays.asList("foo"),
            null,
            null
        );
        Assert.assertEquals(1, clusterList.size());
        Assert.assertEquals(cluster1Id, clusterList.get(0).getId());

        clusterList = clusterClient.getClusters(
            null,
            null,
            Arrays.asList("pi"),
            null,
            null
        );

        Assert.assertEquals(2, clusterList.size());
        Assert.assertEquals(cluster2Id, clusterList.get(0).getId());
        Assert.assertEquals(cluster1Id, clusterList.get(1).getId());

        // Test get by name
        clusterList = clusterClient.getClusters(
            "cluster1name",
            null,
            null,
            null,
            null
        );

        Assert.assertEquals(1, clusterList.size());

        // Test get by status
        clusterList = clusterClient.getClusters(
            null,
            Arrays.asList(ClusterStatus.UP.toString()),
            null,
            null,
            null
        );

        Assert.assertEquals(1, clusterList.size());

        clusterList = clusterClient.getClusters(
            null,
            Arrays.asList(ClusterStatus.UP.toString(), ClusterStatus.OUT_OF_SERVICE.toString()),
            null,
            null,
            null
        );

        Assert.assertEquals(2, clusterList.size());
    }

    /**
     * Test to confirm getting an exception for non existent cluster.
     *
     * @throws Exception If there is a problem.
     */
    @Test(expected = IOException.class)
    public void testClusterNotExist() throws Exception {
        clusterClient.getCluster("foo");
    }

    /**
     * Test get all clusters.
     *
     * @throws Exception If there is problem.
     */
    @Test
    public void testGetAllAndDeleteAllClusters() throws Exception {
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

        clusterClient.deleteAllClusters();
        Assert.assertEquals(clusterClient.getClusters().size(), 0);
    }

    /**
     * Test whether we can delete a cluster in Genie.
     *
     * @throws Exception If there is any problem.
     */
    @Test(expected = IOException.class)
    public void testDeleteCluster() throws Exception {
        final Cluster cluster1 = constructClusterDTO(null);
        clusterClient.createCluster(cluster1);

        final Cluster cluster2 = clusterClient.getCluster(cluster1.getId());
        Assert.assertEquals(cluster2.getId(), cluster1.getId());

        clusterClient.deleteCluster(cluster1.getId());
        clusterClient.getCluster(cluster1.getId());
    }

    /**
     * Test to verify if the update cluster method is working correctly.
     *
     * @throws Exception If there is any problem.
     */
    @Test
    public void testUpdateCluster() throws Exception {
        final Cluster cluster1 = constructClusterDTO(null);
        clusterClient.createCluster(cluster1);

        final Cluster cluster2 = clusterClient.getCluster(cluster1.getId());
        Assert.assertEquals(cluster2.getName(), cluster1.getName());

        final Cluster cluster3 = new
            Cluster.Builder("newname", "newuser", "new version", ClusterStatus.OUT_OF_SERVICE)
            .withId(cluster1.getId())
            .build();

        clusterClient.updateCluster(cluster1.getId(), cluster3);

        final Cluster cluster4 = clusterClient.getCluster(cluster1.getId());

        Assert.assertEquals("newname", cluster4.getName());
        Assert.assertEquals("newuser", cluster4.getUser());
        Assert.assertEquals("new version", cluster4.getVersion());
        Assert.assertEquals(ClusterStatus.OUT_OF_SERVICE, cluster4.getStatus());
        Assert.assertEquals(null, cluster4.getSetupFile());
        Assert.assertEquals(null, cluster4.getDescription());
        Assert.assertEquals(Collections.emptySet(), cluster4.getConfigs());
        Assert.assertEquals(cluster4.getTags().contains("foo"), false);
    }
}
