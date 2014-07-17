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

import com.netflix.genie.common.exceptions.GenieException;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests for ClusterCriteria.
 *
 * @author tgianos
 */
public class TestClusterCriteria {
    /**
     * Make sure the constructors create sets properly.
     *
     * @throws GenieException
     */
    @Test
    public void testConstructors() throws GenieException {
        ClusterCriteria cc = new ClusterCriteria();
        Assert.assertNotNull(cc.getTags());

        final Set<String> tags = new HashSet<String>();
        tags.add("Some Tag");
        cc = new ClusterCriteria(tags);
        Assert.assertEquals(tags, cc.getTags());
    }

    /**
     * Test to make sure constructor throws exception on bad inputs.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testNullTagsConstructor() throws GenieException {
        ClusterCriteria cc = new ClusterCriteria(null);
        cc.getTags();
    }

    /**
     * Test to make sure constructor throws exception on bad inputs.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testEmptyTagsConstructor() throws GenieException {
        final Set<String> tags = new HashSet<String>();
        ClusterCriteria cc = new ClusterCriteria(tags);
        cc.getTags();
    }

    /**
     * Test to make sure setter saves the proper set.
     *
     * @throws GenieException
     */
    @Test
    public void testTagsSetter() throws GenieException {
        final ClusterCriteria cc = new ClusterCriteria();
        final Set<String> tags = new HashSet<String>();
        tags.add("tag1");
        tags.add("tag2");
        tags.add("tag3");
        cc.setTags(tags);
        Assert.assertEquals(tags, cc.getTags());
    }

    /**
     * Test to make sure set throws exception on bad inputs.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testNullTagsSetter() throws GenieException {
        final ClusterCriteria cc = new ClusterCriteria();
        cc.setTags(null);
    }

    /**
     * Test to make sure set throws exception on bad inputs.
     *
     * @throws GenieException
     */
    @Test(expected = GenieException.class)
    public void testEmptyTagsSetter() throws GenieException {
        final Set<String> tags = new HashSet<String>();
        final ClusterCriteria cc = new ClusterCriteria();
        cc.setTags(tags);
    }
}
