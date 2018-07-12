/*
 *
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.genie.web.jpa.entities;

import com.google.common.collect.Lists;
import com.netflix.genie.test.categories.UnitTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.validation.ConstraintViolationException;
import java.util.List;
import java.util.UUID;

/**
 * Unit tests for the AgentConnectionEntity class.
 *
 * @author tgianos
 * @since 3.3.0
 */
@Category(UnitTest.class)
public class AgentConnectionEntityUnitTest extends EntityTestsBase {

    private static final String JOB = UUID.randomUUID().toString();
    private static final String HOST = UUID.randomUUID().toString();

    /**
     * Test constructor.
     */
    @Test
    public void canCreateAgentConnectionEntityWithConstructor() {
        final AgentConnectionEntity entity = new AgentConnectionEntity(JOB, HOST);
        Assert.assertThat(entity.getJobId(), Matchers.is(JOB));
        Assert.assertThat(entity.getServerHostname(), Matchers.is(HOST));
    }

    /**
     * Test setters.
     */
    @Test
    public void canCreateAgentConnectionEntityWithSetters() {
        final AgentConnectionEntity entity = new AgentConnectionEntity();
        entity.setJobId(JOB);
        entity.setServerHostname(HOST);
        Assert.assertThat(entity.getJobId(), Matchers.is(JOB));
        Assert.assertThat(entity.getServerHostname(), Matchers.is(HOST));
    }

    /**
     * Verify validated fields value.
     */
    @Test
    public void cantCreateAgentConnectionEntityDueToSize() {

        final List<Pair<String, String>> invalidParameterPairs = Lists.newArrayList();

        invalidParameterPairs.add(Pair.of(JOB, null));
        invalidParameterPairs.add(Pair.of(null, HOST));
        invalidParameterPairs.add(Pair.of(JOB, " "));
        invalidParameterPairs.add(Pair.of(" ", HOST));
        invalidParameterPairs.add(Pair.of(StringUtils.rightPad(JOB, 256), HOST));
        invalidParameterPairs.add(Pair.of(JOB, StringUtils.rightPad(HOST, 256)));

        for (final Pair<String, String> invalidParameters : invalidParameterPairs) {
            final AgentConnectionEntity entity =
                new AgentConnectionEntity(invalidParameters.getLeft(), invalidParameters.getRight());

            try {
                this.validate(entity);
            } catch (final ConstraintViolationException e) {
                // Expected, move on to the next pair.
                continue;
            }

            Assert.fail("Entity unexpectedly passed validation: " + entity.toString());
        }
    }

    /**
     * Test to make sure equals and hash code only care about the unique file.
     */
    @Test
    public void testEqualsAndHashCode() {
        final AgentConnectionEntity e1 = new AgentConnectionEntity(JOB, HOST);
        final AgentConnectionEntity e2 = new AgentConnectionEntity();
        e2.setJobId(JOB);
        e2.setServerHostname(HOST);

        Assert.assertEquals(e1, e2);
        Assert.assertEquals(e1.hashCode(), e2.hashCode());

        final AgentConnectionEntity e3 = new AgentConnectionEntity(JOB, "FOO");
        final AgentConnectionEntity e4 = new AgentConnectionEntity("BAR", HOST);

        Assert.assertNotEquals(e1, e3);
        Assert.assertNotEquals(e2, e3);
        Assert.assertNotEquals(e1, e4);
        Assert.assertNotEquals(e2, e4);
        Assert.assertNotEquals(e3, e4);

        Assert.assertNotEquals(e1.hashCode(), e3.hashCode());
        Assert.assertNotEquals(e2.hashCode(), e3.hashCode());
        Assert.assertNotEquals(e1.hashCode(), e4.hashCode());
        Assert.assertNotEquals(e2.hashCode(), e4.hashCode());
        Assert.assertNotEquals(e3.hashCode(), e4.hashCode());
    }

    /**
     * Test the toString method.
     */
    @Test
    public void testToString() {
        Assert.assertNotNull(new AgentConnectionEntity().toString());
    }
}
